package shardctrler

import (
	"6.5840/raft"
	"sort"
	"time"
)
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	opChannel   map[int64]chan Config
	lastCmd     map[int64]int64
	killChannel chan struct{}

	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
	ReqId   int64
	MsgId   int64
	Op      string
	Servers map[int][]string
	Shard   int
	GID     int
	GIDs    []int
	CfgNum  int
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{
		ReqId:   nrand(),
		MsgId:   args.MsgId,
		Op:      "Join",
		Servers: args.Servers,
	}
	err, _ := sc.executeCmd(op)
	reply.Err = err
	reply.WrongLeader = err == ErrWrongLeader
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{
		ReqId: nrand(),
		MsgId: args.MsgId,
		Op:    "Leave",
		GIDs:  args.GIDs,
	}
	err, _ := sc.executeCmd(op)
	reply.Err = err
	reply.WrongLeader = err == ErrWrongLeader
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{
		ReqId: nrand(),
		MsgId: args.MsgId,
		Op:    "Move",
		Shard: args.Shard,
		GID:   args.GID,
	}
	err, _ := sc.executeCmd(op)
	reply.Err = err
	reply.WrongLeader = err == ErrWrongLeader
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{
		ReqId:  nrand(),
		MsgId:  args.MsgId,
		Op:     "Query",
		CfgNum: args.Num,
	}
	err, cfg := sc.executeCmd(op)
	reply.Err = err
	reply.Config = cfg
	reply.WrongLeader = err == ErrWrongLeader
}

func (sc *ShardCtrler) executeCmd(op Op) (Err, Config) {
	_, _, isLeader := sc.Raft().Start(op)
	if !isLeader {
		return ErrWrongLeader, Config{}
	}
	sc.mu.Lock()
	ch := make(chan Config, 1)
	sc.opChannel[op.ReqId] = ch
	sc.mu.Unlock()
	select {
	case <-time.After(500 * time.Millisecond):
		return ErrTimeout, Config{}
	case cfg := <-ch:
		return OK, cfg
	}
}

func (sc *ShardCtrler) bal(groups map[int][]string, shards [NShards]int) [NShards]int {
	shardsNew := [NShards]int{}
	if len(groups) == 0 {
		return shardsNew
	}
	if len(groups) == 1 {
		for idx, _ := range shardsNew {
			for gid, _ := range groups {
				shardsNew[idx] = gid
			}
		}
		return shardsNew
	}
	collectGroup := make(map[int][]int)
	var groupSorted []int
	startGid := -1
	for gid, _ := range groups {
		groupSorted = append(groupSorted, gid)
		var sd []int
		collectGroup[gid] = sd
		if gid > startGid {
			startGid = gid
		}
	}
	for index, gid := range shards {
		shardsNew[index] = gid
		if _, ok := collectGroup[gid]; ok {
			collectGroup[gid] = append(collectGroup[gid], index)
		} else {
			shardsNew[index] = startGid
			collectGroup[startGid] = append(collectGroup[startGid], index)
		}
	}
	sort.Ints(groupSorted)
	for {
		minn := 257
		maxx := 0
		maxGid := -1
		minGid := -1
		for _, gid := range groupSorted {
			arr := collectGroup[gid]
			if len(arr) > maxx {
				maxx = len(arr)
				maxGid = gid
			}
			if len(arr) < minn {
				minn = len(arr)
				minGid = gid
			}
		}
		if maxx-minn > 1 {
			maxArr := collectGroup[maxGid]
			exchIdx := maxArr[0]
			maxArr = maxArr[1:]
			collectGroup[maxGid] = maxArr
			shardsNew[exchIdx] = minGid
			collectGroup[minGid] = append(collectGroup[minGid], exchIdx)
		} else {
			break
		}
	}
	return shardsNew
}

func (sc *ShardCtrler) applyLoop() {
	for {
		select {
		case <-sc.killChannel:
			return
		case msg := <-sc.applyCh:
			if msg.CommandValid {
				op := msg.Command.(Op)
				sc.mu.Lock()
				cfgNum := 0
				if "Query" == op.Op {
					if op.CfgNum == -1 || op.CfgNum >= len(sc.configs) {
						cfgNum = len(sc.configs) - 1
					} else {
						cfgNum = op.CfgNum
					}
				} else if lastMsg, ok := sc.lastCmd[op.ReqId]; !ok || lastMsg < op.MsgId {
					switch op.Op {
					case "Join":
						sc.executeJoin(op.Servers)
					case "Leave":
						sc.executeLeave(op.GIDs)
					case "Move":
						sc.executeMove(op.Shard, op.GID)
					default:
						panic("command unrecognizable")
					}
					sc.lastCmd[op.ReqId] = op.MsgId
				}
				if ch, ok := sc.opChannel[op.ReqId]; ok {
					ch <- sc.configs[cfgNum]
				}
				sc.mu.Unlock()
			}
		}
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	sc.killChannel <- struct{}{}
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.opChannel = make(map[int64]chan Config)
	sc.lastCmd = make(map[int64]int64)
	sc.killChannel = make(chan struct{})

	go sc.applyLoop()

	return sc
}
