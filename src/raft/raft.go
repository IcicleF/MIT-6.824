package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"crypto/rand"
	"math/big"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const TimeoutMin = 500
const TimeoutMax = 700

// randomizer
func rnd(min int, max int) int {
	x, _ := rand.Int(rand.Reader, big.NewInt(int64(max-min)))
	return int(x.Int64()) + min
}

// majority calculator
func majority(num int) int {
	return num/2 + 1
}

// minimizer
func min(x int64, y int64) int64 {
	if x < y {
		return x
	}
	return y
}

const (
	RPCOk = iota
	RPCRejected
	RPCLost
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int64
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int64               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int64        // last term server has seen
	votedFor    int64        // candidate ID that received vote in current term
	logs        []LogEntry   // log entry array
	logsLock    sync.RWMutex // provide atomic access to log array

	commitIndex int64 // index of highest log entry known to be commited
	lastApplied int64 // index of highest log entry applied to state machine

	indexesLock sync.RWMutex
	nextIndex   []int64 // if I am leader, for each server, index of the next log entry to send
	matchIndex  []int64 // if I am leader, for each server, index of highest log entry known to be replicated

	// Other states to be maintained
	LeaderId      int64 // current leader
	role          int64 // my role
	lastRPC       int64 // arrival time of last valid RPC
	receivedVotes int64 // my votes in an election

	applyCh   chan ApplyMsg
	applyLock sync.Mutex
	applyCond sync.Cond
}

const (
	RoleLeader = iota
	RoleFollower
	RoleCandidate
)

func (rf *Raft) initFollowerIndexes() {
	rf.logsLock.RLock()
	defer rf.logsLock.RUnlock()

	rf.indexesLock.Lock()
	defer rf.indexesLock.Unlock()

	nextInd := len(rf.logs)

	rf.nextIndex = make([]int64, 0)
	rf.matchIndex = make([]int64, 0)
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex = append(rf.nextIndex, int64(nextInd))
		rf.matchIndex = append(rf.matchIndex, -1)
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	term = int(atomic.LoadInt64(&rf.currentTerm))
	isleader = atomic.LoadInt64(&rf.role) == RoleLeader

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int64
	CandidateId  int
	LastLogIndex int64
	LastLogTerm  int64
}

//
// RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Ok   bool
	Term int64
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	DPrintln(Exp2A, Info, "Raft %d received RequestVote from %d with term %d.",
		rf.me, args.CandidateId, args.Term)

	// 1. Reply false if term < currentTerm
	reply.Term = atomic.LoadInt64(&rf.currentTerm)
	if args.Term < reply.Term {
		DPrintln(Exp2A, Warning, "Raft %d rejected the RequestVote because of smaller term.", rf.me)
		reply.Ok = false
		return
	}

	// Transit to new term and follower
	reply.Term = args.Term
	if atomic.LoadInt64(&rf.currentTerm) != args.Term {
		atomic.StoreInt64(&rf.role, RoleFollower)
		atomic.StoreInt64(&rf.currentTerm, int64(args.Term))
		atomic.StoreInt64(&rf.votedFor, -1)
	}

	// 2. If votedFor is null or candidateId, ...
	vote := atomic.LoadInt64(&rf.votedFor)
	reply.Ok = (vote == -1 || vote == int64(args.CandidateId))
	if !reply.Ok {
		return
	}

	// ... and candidate's log is at least as up-to-date as receiver's log, ...
	rf.logsLock.RLock()
	localLastLogIndex := len(rf.logs) - 1
	var localLastLogTerm int64 = -1
	if localLastLogIndex >= 0 {
		localLastLogTerm = rf.logs[localLastLogIndex].Term
	}
	rf.logsLock.RUnlock()

	if localLastLogTerm > args.LastLogTerm || (localLastLogTerm == args.LastLogTerm && localLastLogIndex > int(args.LastLogIndex)) {
		reply.Ok = false
		return
	}

	// ... grant vote.
	atomic.StoreInt64(&rf.votedFor, int64(args.CandidateId))
	reply.Ok = true

	// Valid request from candidate, update last RPC
	atomic.StoreInt64(&rf.lastRPC, time.Now().UnixNano())

	DPrintln(Exp2A, Info, "Raft %d decide to vote for %d in term %d.", rf.me, args.CandidateId, reply.Term)
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntries
type AppendEntriesArgs struct {
	Term         int64
	LeaderId     int
	PrevLogIndex int64
	PrevLogTerm  int64
	Entries      []LogEntry
	LeaderCommit int64
}

type AppendEntriesReply struct {
	Ok   bool
	Term int64
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Term = atomic.LoadInt64(&rf.currentTerm)

	// 1. Reply false if term < currentTerm
	if args.Term < reply.Term {
		DPrintln(Exp2A, Warning, "Raft %d rejected AppendEntries from %d because of smaller term",
			rf.me, args.LeaderId)
		reply.Ok = false
		return
	}

	// Convert to follower
	if atomic.LoadInt64(&rf.role) == RoleLeader {
		DPrintln(Exp2A, Important, "Raft %d reverts from leader to follower", rf.me)
	}
	atomic.StoreInt64(&rf.role, RoleFollower)
	atomic.StoreInt64(&rf.currentTerm, int64(args.Term))
	reply.Term = args.Term

	// Valid request from leader, update related variables
	atomic.StoreInt64(&rf.LeaderId, int64(args.LeaderId))
	atomic.StoreInt64(&rf.lastRPC, time.Now().UnixNano())

	// 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	phase2Flag := true

	rf.logsLock.RLock()
	logLen := len(rf.logs)
	if logLen <= int(args.PrevLogIndex) {
		phase2Flag = false
	} else if args.PrevLogIndex >= 0 {
		phase2Flag = rf.logs[args.PrevLogIndex].Term == args.PrevLogTerm
	}
	rf.logsLock.RUnlock()

	if !phase2Flag {
		if len(args.Entries) != 0 {
			DPrintln(Exp2B, Warning,
				"Raft %d rejected AppendEntries from %d because of prevLog unmatch: remote len %d, prev (%d %d).",
				rf.me, args.LeaderId, len(args.Entries), args.PrevLogIndex, args.PrevLogTerm)
		}
		reply.Ok = false
		return
	}

	// true if follower contained entry matching prevLogIndex and prevLogTerm
	reply.Ok = true

	// 3. If an existing entry conflicts with a new one, delete the existing entry and all that follows it
	// 4. Append any new entries not already in the log
	rf.logsLock.Lock()
	rf.logs = append(rf.logs[:args.PrevLogIndex+1], args.Entries...)
	lastEntry := len(rf.logs) - 1
	rf.logsLock.Unlock()

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > atomic.LoadInt64(&rf.commitIndex) {
		atomic.StoreInt64(&rf.commitIndex, min(args.LeaderCommit, int64(lastEntry)))
		rf.applyCond.Signal()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) logApplier() {
	rf.applyLock.Lock()
	defer rf.applyLock.Unlock()

	for !rf.killed() {
		for atomic.LoadInt64(&rf.commitIndex) <= rf.lastApplied {
			rf.applyCond.Wait()
		}

		// Start applying the log
		rf.lastApplied++
		applyIndex := rf.lastApplied

		rf.logsLock.RLock()
		command := rf.logs[applyIndex].Command
		rf.logsLock.RUnlock()

		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      command,
			CommandIndex: int(applyIndex) + 1,
		}
		rf.applyCh <- applyMsg

		DPrintln(Exp2B, Important, "Raft %d successfully applied log %d = %v.", rf.me, applyIndex, applyMsg)
	}
}

func (rf *Raft) logReplicator(server int) {
	for !rf.killed() {
		if atomic.LoadInt64(&rf.role) == RoleLeader {
			rf.logsLock.RLock()
			lastLogIndex := len(rf.logs) - 1
			rf.logsLock.RUnlock()

			rf.indexesLock.RLock()
			nextIndex := rf.nextIndex[server]
			rf.indexesLock.RUnlock()

			for atomic.LoadInt64(&rf.role) == RoleLeader && lastLogIndex >= int(nextIndex) {
				DPrintln(Exp2B, Info, "Raft %d starts replicating log for %d (last %d >= next %d)",
					rf.me, server, lastLogIndex, nextIndex)

				ok := rf.doSendAppendEntries(server, nextIndex)
				if ok == RPCOk {
					DPrintln(Exp2B, Info, "Raft %d: follower %d successfully replicated log to %d",
						rf.me, server, lastLogIndex)
					rf.indexesLock.Lock()
					atomic.StoreInt64(&rf.nextIndex[server], int64(lastLogIndex+1))
					atomic.StoreInt64(&rf.matchIndex[server], int64(lastLogIndex))
					rf.indexesLock.Unlock()
					break
				} else if ok == RPCRejected {
					DPrintln(Exp2B, Warning, "Raft %d: follower %d rejected log replication (%d to %d)",
						rf.me, server, nextIndex, lastLogIndex)
					nextIndex = atomic.AddInt64(&rf.nextIndex[server], -1)
				}
				time.Sleep(time.Millisecond * 10)
			}
		}
		time.Sleep(time.Millisecond * 10)
	}
}

func (rf *Raft) logCommitter() {
	for !rf.killed() {
		if atomic.LoadInt64(&rf.role) == RoleLeader {
			matchIndex := make([]int, len(rf.peers))

			// Copy matchIndex
			rf.logsLock.RLock()
			rf.indexesLock.RLock()
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					matchIndex[i] = len(rf.logs) - 1
					continue
				}
				matchIndex[i] = int(rf.matchIndex[i])
			}
			rf.indexesLock.RUnlock()

			// Find the matchIndex majority
			sort.Ints(matchIndex)
			majorityMatched := matchIndex[len(rf.peers)-majority(len(rf.peers))]

			// N > commitIndex ...
			ok := majorityMatched > int(atomic.LoadInt64(&rf.commitIndex))

			// ... and log[N].term == currentTerm
			ok = ok && rf.logs[majorityMatched].Term == atomic.LoadInt64(&rf.currentTerm)
			rf.logsLock.RUnlock()

			// Set commitIndex = N
			if ok {
				DPrintln(Exp2B, Important, "Raft %d decided logs to %d is commited.", rf.me, majorityMatched)
				atomic.StoreInt64(&rf.commitIndex, int64(majorityMatched))
				rf.applyCond.Signal()
			}
		}
		time.Sleep(time.Millisecond * 10)
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	isLeader := atomic.LoadInt64(&rf.role) == RoleLeader

	// DPrintln(Exp2B, Info, "Raft %d receives an agreement request: %v.", rf.me, command)

	// Your code here (2B).
	if !isLeader {
		// If not leader, returns false
		return -1, -1, isLeader
	}

	// Starts agreement
	term := atomic.LoadInt64(&rf.currentTerm)
	index := rf.localLogAppend(term, command)

	DPrintln(Exp2B, Important, "Raft %d starts agreement on term %d, index %d.", rf.me, term, index)

	return index + 1, int(term), isLeader
}

func (rf *Raft) localLogAppend(term int64, command interface{}) int {
	rf.logsLock.Lock()
	defer rf.logsLock.Unlock()

	l := len(rf.logs)
	rf.logs = append(rf.logs, LogEntry{Term: term, Command: command})
	return l
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt64(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt64(&rf.dead)
	return z == 1
}

const (
	BroadcastRequestVote = iota
	BroadcastHeartbeat
)

func (rf *Raft) broadcast(kind int) {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		switch kind {
		case BroadcastRequestVote:
			go rf.doSendRequestVote(i)
		case BroadcastHeartbeat:
			go rf.doSendAppendEntries(i, -1)
		}
	}
}

func (rf *Raft) doSendRequestVote(server int) {
	// prepare a RequestVote RPC
	args := RequestVoteArgs{}
	reply := RequestVoteReply{}

	args.Term = atomic.LoadInt64(&rf.currentTerm)
	args.CandidateId = rf.me

	rf.logsLock.RLock()
	args.LastLogIndex = int64(len(rf.logs) - 1)
	if args.LastLogIndex < 0 {
		args.LastLogTerm = -1
	} else {
		args.LastLogTerm = rf.logs[args.LastLogIndex].Term
	}
	rf.logsLock.RUnlock()

	ok := rf.sendRequestVote(server, &args, &reply)
	if !ok {
		return
	}

	// Need to verify that I am still a candidate
	if atomic.LoadInt64(&rf.role) != RoleCandidate {
		return
	}

	if reply.Ok && reply.Term == atomic.LoadInt64(&rf.currentTerm) {
		atomic.AddInt64(&rf.receivedVotes, 1)
	}

	// If RPC response contains term > currentTerm, set currentTerm = term, convert to follower
	if reply.Term > atomic.LoadInt64(&rf.currentTerm) {
		atomic.StoreInt64(&rf.role, RoleFollower)
		atomic.StoreInt64(&rf.currentTerm, int64(reply.Term))
	}
}

func (rf *Raft) doSendAppendEntries(server int, head int64) int {
	// prepare a heartbeat
	args := AppendEntriesArgs{}
	reply := AppendEntriesReply{}

	rf.logsLock.RLock()
	args.Term = atomic.LoadInt64(&rf.currentTerm)
	args.LeaderId = rf.me
	if head < 0 {
		head = int64(len(rf.logs))
	}

	args.PrevLogIndex = head - 1
	if head == 0 {
		args.PrevLogTerm = -1
	} else {
		args.PrevLogTerm = rf.logs[head-1].Term
	}
	args.Entries = make([]LogEntry, len(rf.logs)-int(head))
	copy(args.Entries, rf.logs[head:])
	args.LeaderCommit = atomic.LoadInt64(&rf.commitIndex)
	rf.logsLock.RUnlock()

	ok := rf.sendAppendEntries(server, &args, &reply)
	if !ok {
		return RPCLost
	}

	res := RPCOk
	if !reply.Ok {
		// DPrintln(Exp2A, Warning,
		// 	"Raft %d: heartbeat (term: %d) rejected by peer %d (term: %d)",
		// 	rf.me, atomic.LoadInt64(&rf.currentTerm), server, reply.Term)
		res = RPCRejected
	}

	// If RPC response contains term > currentTerm, set currentTerm = term, convert to follower
	if reply.Term > atomic.LoadInt64(&rf.currentTerm) {
		DPrintln(Exp2B, Warning, "Raft %d fast-forwards to term %d and convert to follower.", rf.me, reply.Term)
		atomic.StoreInt64(&rf.role, RoleFollower)
		atomic.StoreInt64(&rf.currentTerm, int64(reply.Term))

		// Reset RPC ticker
		atomic.StoreInt64(&rf.lastRPC, time.Now().UnixNano())
	}
	return res
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	last := atomic.LoadInt64(&rf.lastRPC)
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(time.Millisecond * time.Duration(rnd(TimeoutMin, TimeoutMax)))

		// waken up
		cur := atomic.LoadInt64(&rf.lastRPC)
		if cur == last {
			// no RPC arrived, begin election
			if atomic.LoadInt64(&rf.role) != RoleFollower {
				// Only followers can transit to candidate
				last = cur
				continue
			}

			// repeat election until I am not a candidate
			for rf.killed() == false {
				DPrintln(Exp2A, Important, "Raft %d starts election at term %d!",
					rf.me, atomic.LoadInt64(&rf.currentTerm))

				atomic.AddInt64(&rf.currentTerm, 1)
				atomic.StoreInt64(&rf.role, RoleCandidate)
				atomic.StoreInt64(&rf.votedFor, int64(rf.me))
				atomic.StoreInt64(&rf.receivedVotes, 1)
				rf.broadcast(BroadcastRequestVote)

				timeout := (time.Millisecond * time.Duration(rnd(TimeoutMin, TimeoutMax))).Nanoseconds()
				startTime := time.Now().UnixNano()
				// as long as I am still a candidate
				for atomic.LoadInt64(&rf.role) == RoleCandidate && !rf.killed() {
					curTime := time.Now().UnixNano()
					if curTime-startTime > timeout {
						// (c) a period of time goes by no winner
						//  -> abort and go to next term
						break
					}

					if atomic.LoadInt64(&rf.receivedVotes) >= int64(majority(len(rf.peers))) {
						// (a) I win the election
						DPrintln(Exp2A, Important, "Raft %d thinks it has won!", rf.me)
						atomic.StoreInt64(&rf.role, RoleLeader)
						atomic.StoreInt64(&rf.LeaderId, int64(rf.me))
						rf.initFollowerIndexes()
						rf.broadcast(BroadcastHeartbeat)
					}
				}
				// (a) I win the election
				// (b) another server establishes itself as leader
				if atomic.LoadInt64(&rf.role) != RoleCandidate {
					break
				}
			}
		}
		last = cur
	}
}

// The heart is responsible to send out heartbeats when I am leader.
func (rf *Raft) heart() {
	for rf.killed() == false {
		if atomic.LoadInt64(&rf.role) == RoleLeader {
			rf.broadcast(BroadcastHeartbeat)
		}
		time.Sleep(time.Millisecond * 100)
	}
}

///
/// the service or tester wants to create a Raft server.
/// @param peers      the ports of all the Raft servers (including this one) are in peers[].
/// @param me         this server's port is peers[me].
/// 			 	  all the servers' peers[] arrays have the same order.
/// @param persister  a place for this server to save its persistent state, and also initially
///                   holds the most recent saved state, if any.
/// @param applyCh    a channel on which the tester or service expects Raft to send ApplyMsg messages.
///
/// Make() must return quickly, so it should start goroutines
/// for any long-running work.
///
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.applyCh = applyCh

	rf.logs = make([]LogEntry, 0)

	atomic.StoreInt64(&rf.role, RoleFollower)
	rf.lastRPC = time.Now().UnixNano()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// beat my heart
	go rf.heart()

	// start follower log replicator
	for i := 0; i < len(peers); i++ {
		if i == me {
			continue
		}
		go rf.logReplicator(i)
	}

	// start commiter
	go rf.logCommitter()

	// start log applier
	rf.applyLock = sync.Mutex{}
	rf.applyCond = sync.Cond{L: &rf.applyLock}
	go rf.logApplier()

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
