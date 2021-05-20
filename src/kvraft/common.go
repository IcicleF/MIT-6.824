package kvraft

const (
	OK = iota
	ErrNoKey
	ErrWrongLeader
	ErrLostLeadership
	ErrUnknown
)

type Err int
type RPCReply interface {
	GetErr() Err
	GetLeader() int // [!!!!] Hole: Raft ID might be different from KVServer ID
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	Id    int64
}

type PutAppendReply struct {
	Err           Err
	CorrectLeader int
}

func (par PutAppendReply) GetErr() Err {
	return par.Err
}
func (par PutAppendReply) GetLeader() int {
	return par.CorrectLeader
}

type GetArgs struct {
	Key string
	Id  int64
}

type GetReply struct {
	Err           Err
	Value         string
	CorrectLeader int
}

func (gr GetReply) GetErr() Err {
	return gr.Err
}
func (gr GetReply) GetLeader() int {
	return gr.CorrectLeader
}
