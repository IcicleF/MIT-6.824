package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK = iota
	ErrNoKey
	ErrWrongGroup
	ErrWrongLeader

	ErrNotMigrating
	ErrRejected
)

type Err int

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	CliId int64
	SeqId int64
}
type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key   string
	CliId int64
	SeqId int64
}
type GetReply struct {
	Err   Err
	Value string
}

type MigrateArgs struct {
	Gid         int // My GID
	MigratingTo int // My future configuration number
	ShardId     int // The shard ID I am asking
}
type MigrateReply struct {
	Err      Err               // OK, or ErrWrongLeader, or ErrNotMigrating
	Shard    map[string]string // The corresponding shard
	Executed map[int64]int64   // Full executed map for deduplication
}

type CheckConfigArgs struct {
	Gid int
}
type CheckConfigReply struct {
	Err Err // OK, or ErrWrongLeader
	Num int
}
