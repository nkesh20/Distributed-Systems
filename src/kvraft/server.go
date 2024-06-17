package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"encoding/gob"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string
	ClientId  int64
	MsgId     int
	Key       string
	Value     string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	database            map[string]string
	acknowledgementData map[int64]int
	res                 map[int]chan Op
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	opEntry := Op{
		ClientId:  args.ClientId,
		MsgId:     args.MsgId,
		Key:       args.Key,
		Operation: "Get",
	}

	success := kv.appendToLog(opEntry)
	if !success {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	val, present := kv.database[args.Key]
	kv.mu.Unlock()

	if !present {
		reply.Err = ErrNoKey
		return
	}

	reply.Err = OK
	reply.Value = val

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		ClientId:  args.ClientId,
		MsgId:     args.MsgId,
		Key:       args.Key,
		Value:     args.Value,
		Operation: args.Op,
	}

	success := kv.appendToLog(op)
	if !success {
		reply.Err = ErrWrongLeader
	} else {
		reply.Err = OK
	}

}

func (kv *KVServer) appendToLog(opEntry Op) bool {
	index, _, isLeader := kv.rf.Start(opEntry)

	if !isLeader {
		return false
	}

	kv.mu.Lock()
	channel, success := kv.res[index]
	if !success {
		channel = make(chan Op, 1)
		kv.res[index] = channel
	}
	kv.mu.Unlock()

	select {
	case op := <-channel:
		return opEntry == op
	case <-time.After(time.Millisecond * ReplyTimeout):
		return false
	}
}

func (kv *KVServer) isUpdated(clientId int64, msgId int) bool {
	oldMsgId, success := kv.acknowledgementData[clientId]

	if success {
		return oldMsgId < msgId
	}
	return true
}

func (kv *KVServer) dbUpdate(args Op) {
	switch args.Operation {
	case "Put":
		kv.database[args.Key] = args.Value
	case "Append":
		kv.database[args.Key] += args.Value
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) readSnapshot(data []byte) {
	if data == nil || len(data) == 0 {
		return
	}

	buffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buffer)

	var database map[string]string
	var acknowledgementData map[int64]int

	if decoder.Decode(&database) != nil || decoder.Decode(&acknowledgementData) != nil {
		DPrintf("error")
	} else {
		kv.database = database
		kv.acknowledgementData = acknowledgementData
	}
}

func (kv *KVServer) applyLoop() {
	for !kv.killed() {
		msg := <-kv.applyCh

		if !msg.CommandValid {
			buffer := bytes.NewBuffer(msg.Snapshot)
			decoder := gob.NewDecoder(buffer)
			kv.mu.Lock()
			kv.database = make(map[string]string)
			kv.acknowledgementData = make(map[int64]int)
			decoder.Decode(&kv.database)
			decoder.Decode(&kv.acknowledgementData)
			kv.mu.Unlock()
		} else {
			op := msg.Command.(Op)
			index := msg.CommandIndex
			kv.mu.Lock()
			if kv.isUpdated(op.ClientId, op.MsgId) {
				kv.dbUpdate(op)
				kv.acknowledgementData[op.ClientId] = op.MsgId
			}

			if kv.maxraftstate != -1 && kv.rf.GetPersister().RaftStateSize() > kv.maxraftstate {
				buffer := new(bytes.Buffer)
				encoder := gob.NewEncoder(buffer)
				encoder.Encode(kv.database)
				encoder.Encode(kv.acknowledgementData)
				data := buffer.Bytes()
				kv.rf.Snapshot(index, data)
			}

			channel, success := kv.res[index]
			if success {
				select {
				case <-kv.res[index]:
				default:
				}
				channel <- op
			}
			kv.mu.Unlock()
		}
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg, 100)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.acknowledgementData = make(map[int64]int)
	kv.res = make(map[int]chan Op)
	kv.database = make(map[string]string)
	kv.readSnapshot(kv.rf.GetPersister().ReadSnapshot())
	go kv.applyLoop()

	return kv
}
