package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd

	lastLeader int
	cliId      int64
	seqId      int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers

	// You'll have to add code here.
	ck.lastLeader = -1
	ck.cliId = nrand()
	ck.seqId = 0

	return ck
}

func (ck *Clerk) dispatch(closure func(int) RPCReply) RPCReply {
	var res RPCReply
	if ck.lastLeader >= 0 {
		res = closure(ck.lastLeader)
		if res != nil && res.GetErr() == OK {
			return res
		}
	}

	// Start finding leader
	leader := 0
	for {
		// Dispatch request to current "leader"
		DPrintln(Exp3A1, Log, "Clerk: examining server %d...", leader)
		reply := closure(leader)
		if reply != nil {
			// If the server replied correctly
			if err := reply.GetErr(); err == OK {
				// Correct!
				DPrintln(Exp3A1, Info, "Clerk: dispatched op to leader %d!", leader)
				ck.lastLeader = leader
				return reply
			} else {
				DPrintln(Exp3A1, Log, "Clerk: server %d report it is not leader...", leader)
			}
		} else {
			DPrintln(Exp3A1, Warning, "Clerk: server %d did not return.", leader)
		}

		// Move to next leader, and wait for a short period of time
		leader = (leader + 1) % len(ck.servers)
		time.Sleep(time.Millisecond * 1)
	}
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	DPrintln(Exp3A1, Log, "Clerk: Get(%s)", key)
	id := atomic.AddInt64(&ck.seqId, 1)
	closure := func(server int) RPCReply {
		args := GetArgs{Key: key, CliId: ck.cliId, SeqId: id}
		reply := GetReply{}

		ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
		if !ok {
			return nil
		}
		return reply
	}
	reply := ck.dispatch(closure)

	// No retry logic here
	if reply.GetErr() != OK {
		DPrintln(Exp3A1, Error, "Clerk: dispatch Get(%s) failed, received error %d!", key, reply.GetErr())
	}
	gr, ok := reply.(GetReply)
	if !ok {
		DPrintln(Exp3AB, Error, "Clerk: cannot cast dispatch result to GetReply")
	}
	return gr.Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	DPrintln(Exp3A1, Log, "Clerk: %s(%s, %s)", op, key, value)
	id := atomic.AddInt64(&ck.seqId, 1)
	closure := func(server int) RPCReply {
		args := PutAppendArgs{Key: key, Value: value, Op: op, CliId: ck.cliId, SeqId: id}
		reply := PutAppendReply{}

		ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			return nil
		}
		return reply
	}
	reply := ck.dispatch(closure)

	// No retry logic here
	if reply.GetErr() != OK {
		DPrintln(
			Exp3A1, Error, "Clerk: dispatch %s(%s, %s) failed, received error %d!",
			op, key, value, reply.GetErr())
	}
	_, ok := reply.(PutAppendReply)
	if !ok {
		DPrintln(Exp3AB, Error, "Clerk: cannot cast dispatch result to PutAppendReply")
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
