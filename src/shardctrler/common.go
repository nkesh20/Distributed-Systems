package shardctrler

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func (config *Config) Copy() Config {

	cp := Config{
		Num:    config.Num,
		Shards: config.Shards,
		Groups: make(map[int][]string),
	}
	for k, v := range config.Groups {
		cp.Groups[k] = append([]string{}, v...)
	}
	return cp
}

const (
	OK             = "OK"
	ErrTimeout     = "ErrTimeout"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
	MsgId   int64
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs  []int
	MsgId int64
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard int
	GID   int
	MsgId int64
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num   int // desired config number
	MsgId int64
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

func (sc *ShardCtrler) lastCfg() Config {
	return sc.configs[len(sc.configs)-1]
}

func (config *Config) deepCpy() (map[int][]string, [NShards]int) {
	cfgGroups := make(map[int][]string)
	for k, v := range config.Groups {
		cfgGroups[k] = v
	}
	cfgShards := [NShards]int{}
	for i, gid := range config.Shards {
		cfgShards[i] = gid
	}
	return cfgGroups, cfgShards
}

func (sc *ShardCtrler) executeJoin(Servers map[int][]string) {
	lastCfg := sc.lastCfg()
	gr, sh := lastCfg.deepCpy()
	var newGIDs []int
	for gid, serv := range Servers {
		if _, ok := gr[gid]; !ok {
			newGIDs = append(newGIDs, gid)
		}
		nServers := make([]string, len(serv))
		copy(nServers, serv)
		gr[gid] = nServers
	}
	sc.configs = append(sc.configs, Config{
		Num:    len(sc.configs),
		Shards: sc.bal(gr, sh),
		Groups: gr,
	})
}

func (sc *ShardCtrler) executeLeave(GIDs []int) {
	lastCfg := sc.lastCfg()
	gr, sh := lastCfg.deepCpy()
	for _, GID := range GIDs {
		delete(gr, GID)
	}
	sc.configs = append(sc.configs, Config{
		Num:    len(sc.configs),
		Shards: sc.bal(gr, sh),
		Groups: gr,
	})
}

func (sc *ShardCtrler) executeMove(Shard int, GID int) {
	lastCfg := sc.lastCfg()
	gr, sh := lastCfg.deepCpy()
	sh[Shard] = GID
	sc.configs = append(sc.configs, Config{
		Num:    len(sc.configs),
		Shards: sh,
		Groups: gr,
	})
}
