package shardctrler

import (
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

// ===========================================================================================
// BEGIN OF SELF-DEFINED DEBUGGING LIBRARY

// Debugging
const Debug = true

const (
	Log = iota
	Info
	Important
	Warning
	Error
	None

	Exp4A  = 0x1
	Exp4B  = 0x2
	Exp4AB = 0x3
)

const ShownLogLevel = Info
const ShownPhase = 0
const CancelColoring = true

func DPrintln(phase int, typ int, format string, a ...interface{}) {
	if typ == Error || (Debug && ((phase & ShownPhase) != 0) && typ >= ShownLogLevel) {
		var prefix string
		var color int = 0

		switch typ {
		case Log:
			prefix = "[LOG]    "
		case Info:
			prefix = "[INFO]   "
		case Important:
			prefix = "[INFO !] "
			color = 1
		case Warning:
			prefix = "[WARN]   "
			color = 33
		case Error:
			prefix = "[ERR]    "
			color = 31
		}
		params := make([]interface{}, 0)
		if CancelColoring {
			params = append(params, prefix)
			params = append(params, a...)
			fmt.Printf("  %v"+format+"\n", params...)
		} else {
			params = append(params, color, prefix)
			params = append(params, a...)
			fmt.Printf("\x1b[0;%dm  %v"+format+"\x1b[0m\n", params...)
		}
	}
	if typ == Error {
		if CancelColoring {
			fmt.Printf("*** Exit because of unexpected situation. ***\n\n")
		} else {
			fmt.Printf("\x1b[0;31m*** Exit because of unexpected situation. ***\x1b[0m\n\n")
		}
		os.Exit(-1)
	}
}

// func DPrintf(format string, a ...interface{}) (n int, err error) {
// 	if Debug {
// 		log.Printf(format, a...)
// 	}
// 	return
// }

// END OF SELF-DEFINED DEBUGGING LIBRARY
// ===========================================================================================

// ===========================================================================================
// START OF CONFIGURATION METHODS

// Take a config and balance its load
func (cf *Config) Balance() {
	DPrintln(Exp4A, Info, "Start balance on %+v.", *cf)

	// If no replica group
	if len(cf.Groups) == 0 {
		for i := 0; i < NShards; i++ {
			cf.Shards[i] = 0
		}
		return
	}

	keys := make([]int, 0)
	for k := range cf.Groups {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	invalidShards := make([]int, 0)
	counter := make(map[int]int)
	for i := 0; i < NShards; i++ {
		pos := sort.SearchInts(keys, cf.Shards[i])
		if pos == len(keys) || keys[pos] != cf.Shards[i] {
			invalidShards = append(invalidShards, i)
		} else {
			counter[cf.Shards[i]]++
		}
	}

	// First, deal with invalid positions
	for _, i := range invalidShards {
		min, mink := NShards+1, -1
		for _, k := range keys {
			if counter[k] < min {
				min = counter[k]
				mink = k
			}
		}
		cf.Shards[i] = mink
		counter[mink]++
	}

	average := NShards / len(keys)

	// Then, balance overloaded groups
	for {
		min, mink := NShards+1, -1
		max, maxk := -1, -1
		for _, k := range keys {
			if counter[k] < min {
				min = counter[k]
				mink = k
			}
			if counter[k] > max {
				max = counter[k]
				maxk = k
			}
		}
		if min == average && max <= average+1 {
			break
		}

		// Migrate once
		for i := 0; i < NShards; i++ {
			if cf.Shards[i] == maxk {
				cf.Shards[i] = mink
				counter[maxk]--
				counter[mink]++
				break
			}
		}
	}

	DPrintln(Exp4A, Info, "End balance at %+v.", *cf)
}

func (cf *Config) duplicate() Config {
	cf2 := Config{Groups: make(map[int][]string)}
	for i := 0; i < 10; i++ {
		cf2.Shards[i] = cf.Shards[i]
	}
	for k, v := range cf.Groups {
		cf2.Groups[k] = make([]string, len(v))
		copy(cf2.Groups[k], v)
	}
	return cf2
}

// END OF CONFIGURATION METHODS
// ===========================================================================================

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	// persister *raft.Persister

	// Your data here.
	killed        int64
	receivedIndex int             // Applied index
	executed      map[int64]int64 // Deduplication
	configs       []Config        // indexed by config num
}

type Op struct {
	CliId int64
	SeqId int64
	Type  int
	Args  interface{}
}

type Response struct {
	Err    Err
	Result Config
}

const (
	JoinOp = iota
	LeaveOp
	MoveOp
	QueryOp
)

func (sc *ShardCtrler) perform(op Op) Response {
	reply := Response{Err: OK}
	DPrintln(Exp4A, Log, "ShardCtrler %d received RPC of op %+v.", sc.me, op)

	// Try to start an agreement, and reject RPC if not leader
	index, term, isLeader := sc.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		DPrintln(Exp4A, Log, "ShardCtrler %d rejected op because it is not leader.", sc.me)
		return reply
	}

	DPrintln(Exp4A, Log, "ShardCtrler %d informed Raft of op[%d] = %+v.", sc.me, index, op)
	for !sc.Killed() {
		// Sleep first
		time.Sleep(time.Millisecond * 1)

		// Check then
		sc.mu.Lock()
		receivedIndex := sc.receivedIndex
		curSeq, ok := sc.executed[op.CliId]
		sc.mu.Unlock()

		rfTerm, stillLeader := sc.rf.GetState()
		if rfTerm != term || !stillLeader {
			// Raft shifts to a new state
			reply.Err = ErrWrongLeader
			return reply
		}

		if receivedIndex < index {
			continue
		}
		if !ok || curSeq < op.SeqId {
			// Index found, however RPC not executed
			DPrintln(Exp4A, Warning, "ShardCtrler %d found op id (%d,%d) has been replaced.", sc.me, op.CliId, op.SeqId)
			reply.Err = ErrWrongLeader
			return reply
		}
		// DPrintln(Exp4A, Log, "ShardCtrler %d confirmed op id (%d,%d) in its RPC.", sc.me, op.CliId, op.SeqId)

		if op.Type == QueryOp {
			queryArgs, ok := op.Args.(QueryArgs)
			if !ok {
				DPrintln(Exp4A, Error,
					"ShardCtrler %d failed to convert Op.Args = %+v to type QueryArgs!", sc.me, op.Args)
			}
			num := queryArgs.Num

			sc.mu.Lock()
			if num < 0 || num >= len(sc.configs) {
				num = len(sc.configs) - 1
			}
			reply.Result = sc.configs[num]
			DPrintln(Exp4A, Info, "ShardCtrler %d: config[%d] = %+v.", sc.me, queryArgs.Num, reply.Result)
			sc.mu.Unlock()
		}
		break
	}
	return reply
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	op := Op{CliId: args.CliId, SeqId: args.SeqId, Type: JoinOp, Args: *args}
	res := sc.perform(op)
	reply.Err = res.Err
	reply.WrongLeader = res.Err == ErrWrongLeader
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	op := Op{CliId: args.CliId, SeqId: args.SeqId, Type: LeaveOp, Args: *args}
	res := sc.perform(op)
	reply.Err = res.Err
	reply.WrongLeader = res.Err == ErrWrongLeader
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	op := Op{CliId: args.CliId, SeqId: args.SeqId, Type: MoveOp, Args: *args}
	res := sc.perform(op)
	reply.Err = res.Err
	reply.WrongLeader = res.Err == ErrWrongLeader
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	op := Op{CliId: args.CliId, SeqId: args.SeqId, Type: QueryOp, Args: *args}
	res := sc.perform(op)
	reply.Err = res.Err
	reply.WrongLeader = res.Err == ErrWrongLeader
	if reply.Err == OK {
		reply.Config = res.Result
	}
}

func (sc *ShardCtrler) poller() {
	for m := range sc.applyCh {
		if sc.Killed() {
			break
		}

		if m.SnapshotValid {
			DPrintln(Exp4AB, Error, "Snapshot is not implemented in ShardCtrler!")
		} else if m.CommandValid {
			op, ok := m.Command.(Op)
			if !ok {
				DPrintln(Exp4AB, Error, "ShardCtrler %d detects a command that is not of type Op!", sc.me)
			}
			DPrintln(Exp4A, Info, "ShardCtrler %d received confirmation of op[%d] = Type %d %+v.",
				sc.me, m.CommandIndex, op.Type, op.Args)

			// Record & execute
			func() {
				sc.mu.Lock()
				defer sc.mu.Unlock()

				if m.CommandIndex != sc.receivedIndex+1 {
					DPrintln(Exp4AB, Error, "ShardCtrler %d received index %d out of order (prev %d)!",
						sc.me, m.CommandIndex, sc.receivedIndex)
				}
				sc.receivedIndex = m.CommandIndex

				curSeq, ok := sc.executed[op.CliId]
				if !ok {
					sc.executed[op.CliId] = 0
					curSeq = 0
				}
				if curSeq < op.SeqId {
					// If not duplicated request
					sc.executed[op.CliId] = op.SeqId
					latest := sc.configs[len(sc.configs)-1].duplicate()
					latest.Num = len(sc.configs)

					switch op.Type {
					case JoinOp:
						joinOp := op.Args.(JoinArgs)
						for gid, group := range joinOp.Servers {
							latest.Groups[gid] = group
						}
						latest.Balance()
					case LeaveOp:
						leaveOp := op.Args.(LeaveArgs)
						for _, gid := range leaveOp.GIDs {
							delete(latest.Groups, gid)
						}
						latest.Balance()
					case MoveOp:
						moveOp := op.Args.(MoveArgs)
						latest.Shards[moveOp.Shard] = moveOp.GID
					default:
						DPrintln(Exp4A, Log, "ShardCtrler %d detected op[%d] to be a query.", sc.me, m.CommandIndex)
					}

					// Put new configuration into ShardCtrler
					if op.Type != QueryOp {
						sc.configs = append(sc.configs, latest)
						DPrintln(Exp4A, Info, "ShardCtrler %d now has %d configuration(s) (latest %+v).",
							sc.me, latest.Num, latest)
					}
				}
			}()
		}
	}
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt64(&sc.killed, 1)
	sc.rf.Kill()
}

func (sc *ShardCtrler) Killed() bool {
	killed := atomic.LoadInt64(&sc.killed)
	return killed != 0
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me
	// sc.persister = persister

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})

	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.receivedIndex = 0
	sc.executed = make(map[int64]int64)

	// Your code here.
	atomic.StoreInt64(&sc.killed, 0)
	go sc.poller()

	return sc
}
