package kvraft

import (
	"bytes"
	"fmt"
	"os"
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

	Exp3A1 = 0x1
	Exp3A2 = 0x2
	Exp3A  = 0x3
	Exp3B  = 0x4
	Exp3AB = 0x7
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

const (
	Undef = iota
	PutOp
	AppendOp
	GetOp
)

func str2op(op string) int {
	if op == "Put" {
		return PutOp
	}
	return AppendOp
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  int
	CliId int64
	SeqId int64
	Key   string
	Value string
}

type Response GetReply

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

	// Your definitions here.
	store         map[string]string // The real KV store
	receivedIndex int               // Applied index
	executed      map[int64]int64   // Deduplication
}

func (kv *KVServer) perform(op Op) Response {
	reply := Response{Err: OK}
	reply.CorrectLeader = kv.me
	DPrintln(Exp3A1, Log, "KV %d received RPC of op %+v.", kv.me, op)

	// Try to start an agreement, and reject RPC if not leader
	index, term, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.CorrectLeader = kv.rf.GetLeaderMaybe()
		DPrintln(Exp3A1, Log, "KV %d rejected op because it is not leader.", kv.me)
		return reply
	}

	DPrintln(Exp3A1, Log, "KV %d informed Raft of op[%d] = %+v.", kv.me, index, op)
	for !kv.killed() {
		// Sleep first
		time.Sleep(time.Millisecond * 1)

		// Check then
		kv.mu.Lock()
		receivedIndex := kv.receivedIndex
		curSeq, ok := kv.executed[op.CliId]
		kv.mu.Unlock()

		rfTerm, stillLeader := kv.rf.GetState()
		if rfTerm != term || !stillLeader {
			// Raft shifts to a new state
			reply.Err = ErrLostLeadership
			return reply
		}

		if receivedIndex < index {
			continue
		}
		if !ok || curSeq < op.SeqId {
			// Index found, however RPC not executed
			DPrintln(Exp3A1, Warning, "KV %d found op id (%d,%d) is not confirmed.", kv.me, op.CliId, op.SeqId)
			reply.Err = ErrLostLeadership
			return reply
		}
		// DPrintln(Exp3A1, Log, "KV %d confirmed op id (%d,%d) in its RPC.", kv.me, op.CliId, op.SeqId)

		kv.mu.Lock()
		if op.Type == GetOp {
			val, ok := kv.store[op.Key]
			if ok {
				reply.Value = val
			} else {
				reply.Value = ""
			}
		}
		kv.mu.Unlock()

		break
	}
	return reply
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{Type: GetOp, Key: args.Key, CliId: args.CliId, SeqId: args.SeqId}
	res := kv.perform(op)
	reply.Err = res.Err
	reply.Value = res.Value
	reply.CorrectLeader = res.CorrectLeader
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{Type: str2op(args.Op), Key: args.Key, Value: args.Value, CliId: args.CliId, SeqId: args.SeqId}
	res := kv.perform(op)
	reply.Err = res.Err
	reply.CorrectLeader = res.CorrectLeader
	DPrintln(Exp3AB, Log, "KV %d: PutAppend %+v -> %+v", kv.me, args, reply)
}

func (kv *KVServer) poller() {
	for m := range kv.applyCh {
		if kv.killed() {
			break
		}

		if m.SnapshotValid {
			kv.mu.Lock()
			if kv.rf.CondInstallSnapshot(m.SnapshotTerm, m.SnapshotIndex, m.Snapshot) {
				DPrintln(Exp3A1, Info, "KV %d installing snapshot till index %d.", kv.me, m.SnapshotIndex)
				buf := bytes.NewBuffer(m.Snapshot)
				dec := labgob.NewDecoder(buf)

				if dec.Decode(&kv.store) != nil || dec.Decode(&kv.executed) != nil {
					DPrintln(Exp3AB, Error, "KV %d cannot decode snapshot!", kv.me)
				}
				kv.receivedIndex = m.SnapshotIndex
			}
			kv.mu.Unlock()
		} else if m.CommandValid {
			op, ok := m.Command.(Op)
			if !ok {
				DPrintln(Exp3AB, Error, "KV %d detects a command that is not of type Op!", kv.me)
			}

			DPrintln(Exp3A, Info, "KV %d received confirmation of op[%d] = %+v.", kv.me, m.CommandIndex, op)

			// Record & execute
			func() {
				kv.mu.Lock()
				defer kv.mu.Unlock()

				if m.CommandIndex != kv.receivedIndex+1 {
					DPrintln(Exp3AB, Error, "KV %d received index %d out of order (prev %d)!",
						kv.me, m.CommandIndex, kv.receivedIndex)
				}
				kv.receivedIndex = m.CommandIndex

				curSeq, ok := kv.executed[op.CliId]
				if !ok {
					kv.executed[op.CliId] = 0
					curSeq = 0
				}
				if curSeq < op.SeqId {
					// If not duplicate
					kv.executed[op.CliId] = op.SeqId
					if op.Type == PutOp {
						kv.store[op.Key] = op.Value
					} else if op.Type == AppendOp {
						kv.store[op.Key] = kv.store[op.Key] + op.Value
					}
				}

				// Check if there is need to snapshot
				stateSize := kv.persister.RaftStateSize()
				if kv.maxraftstate > 0 && stateSize >= kv.maxraftstate/10*8 {
					DPrintln(Exp3B, Info,
						"KV %d issues a snapshot to index %d = {store = %+v, executed = %+v} (state size %d).",
						kv.me, kv.receivedIndex, kv.store, kv.executed, stateSize)
					index := kv.receivedIndex

					// Snapshot = [store, executed]
					buf := new(bytes.Buffer)
					enc := labgob.NewEncoder(buf)
					enc.Encode(kv.store)
					enc.Encode(kv.executed)
					snapshot := buf.Bytes()

					kv.rf.Snapshot(index, snapshot)
				}
			}()
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
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
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister

	// You may need initialization code here.
	kv.store = make(map[string]string)
	kv.receivedIndex = 0
	kv.executed = make(map[int64]int64)
	// kv.performedCount = 0
	// kv.receivedCount = 0

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.poller()
	// go kv.snapshotter(persister)

	return kv
}
