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
	"bytes"
	"crypto/rand"
	"fmt"
	"math/big"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

const TimeoutMin = 300
const TimeoutMax = 600
const TimeoutHeartbeat = 100

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
	RPCOk        = iota // Remote accepts the RPC
	RPCRejected         // Remote rejects the RPC
	RPCLost             // RPC Call function returns false
	RPCRetracted        // This node regards the RPC as rejected for some reason
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

// Logger (must be used inside a lock)
// ======================================================================

type LogEntry struct {
	Term    int64
	Command interface{}
}

// An atomic timestamp
type Timestamp struct {
	mut sync.Mutex
	t   time.Time
}

func (at *Timestamp) Set() {
	at.mut.Lock()
	defer at.mut.Unlock()
	at.t = time.Now()
}

func (at *Timestamp) Get() time.Time {
	at.mut.Lock()
	defer at.mut.Unlock()
	return at.t
}

func (at *Timestamp) Since() time.Duration {
	at.mut.Lock()
	defer at.mut.Unlock()
	return time.Since(at.t)
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.RWMutex        // provide atomic access to persistent info
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int64               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int64      // last term server has seen
	votedFor    int        // candidate ID that received vote in current term
	logs        []LogEntry // log entry array

	commitIndex int64 // index of highest log entry known to be commited
	lastApplied int64 // index of highest log entry applied to state machine

	indexesLock sync.RWMutex
	nextIndex   []int64 // if I am leader, for each server, index of the next log entry to send
	matchIndex  []int64 // if I am leader, for each server, index of highest log entry known to be replicated

	// Other states to be maintained
	LeaderId      int64     // current leader
	role          int64     // my role
	lastRPC       Timestamp // last RPC received
	receivedVotes int64     // my votes in an election

	applyCh   chan ApplyMsg // apply messages
	applyLock sync.Mutex    // apply lock
	applyCond sync.Cond     // apply condvar
}

const (
	RoleLeader = iota
	RoleFollower
	RoleCandidate
)

func (rf *Raft) initFollowerIndexes() {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

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

func (rf *Raft) fastForwardToTerm(term int64, needLock bool, needRefreshRPC bool) {
	if needLock {
		rf.mu.Lock()
		defer rf.mu.Unlock()
	}
	if term <= rf.currentTerm {
		return
	}

	rf.currentTerm = term
	atomic.StoreInt64(&rf.role, RoleFollower)
	rf.votedFor = -1
	// DPrintln(Exp2C, Warning, "Raft %d setting votedFor = -1", rf.me)
	rf.persist()

	if needRefreshRPC {
		rf.lastRPC.Set()
	}
}

func (rf *Raft) GetTermAndRole() (int64, int64) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	return rf.currentTerm, atomic.LoadInt64(&rf.role)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	term, role := rf.GetTermAndRole()
	isleader := role == RoleLeader

	return int(term), isleader
}

func (rf *Raft) PrintState() {
	var role int64
	var currentTerm int64
	var logLen int
	var lastLogTerm int64

	rf.mu.RLock()
	currentTerm = rf.currentTerm
	role = rf.role
	logLen = len(rf.logs)
	lastLogTerm = -1
	if logLen > 0 {
		lastLogTerm = rf.logs[logLen-1].Term
	}
	rf.mu.RUnlock()

	roleStr := ""
	switch role {
	case RoleCandidate:
		roleStr = "CANDIDATE"
	case RoleFollower:
		roleStr = "FOLLOWER "
	case RoleLeader:
		roleStr = "LEADER   "
	}

	fmt.Printf("Raft %d: %v  term = %d  last log entry (term %d, index %d)\n",
		rf.me, roleStr, currentTerm, lastLogTerm, logLen-1)
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).

	buf := new(bytes.Buffer)
	enc := labgob.NewEncoder(buf)
	enc.Encode(rf.currentTerm)
	enc.Encode(rf.votedFor)
	enc.Encode(rf.logs)

	data := buf.Bytes()
	rf.persister.SaveRaftState(data)

	DPrintln(Exp2C, Log, "Raft %d encoded state {term = %d, vote = %d, log = %v}.",
		rf.me, rf.currentTerm, rf.votedFor, rf.logs)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.currentTerm = 0
		rf.votedFor = -1
		// DPrintln(Exp2C, Warning, "Raft %d setting votedFor = -1", rf.me)
		rf.logs = make([]LogEntry, 0)
		return
	}

	DPrintln(Exp2C, Log, "Raft %d begins decoding data from persistent memory.", rf.me)

	// Your code here (2C).
	buf := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(buf)

	if dec.Decode(&rf.currentTerm) != nil || dec.Decode(&rf.votedFor) != nil || dec.Decode(&rf.logs) != nil {
		DPrintln(Exp2C, Error, "Raft %d cannot decode persistent data!")
	}

	DPrintln(Exp2C, Info, "Raft %d successfully decoded {term = %d, vote = %d, log = %v}.",
		rf.me, rf.currentTerm, rf.votedFor, rf.logs)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintln(Exp2A, Info, "Raft %d received RequestVote from %d with term %d.", rf.me, args.CandidateId, args.Term)

	var localLastLogTerm int64 = -1
	localLastLogIndex := len(rf.logs) - 1
	if localLastLogIndex >= 0 {
		localLastLogTerm = rf.logs[localLastLogIndex].Term
	}

	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		DPrintln(Exp2A, Warning, "Raft %d rejected the RequestVote from %d because of smaller term.",
			rf.me, args.CandidateId)
		reply.Ok = false
		reply.Term = rf.currentTerm
		return
	}

	// Transit to new term and follower
	if args.Term > rf.currentTerm {
		rf.fastForwardToTerm(args.Term, false, false)
	}
	reply.Term = rf.currentTerm

	// 2. If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, ...
	if localLastLogTerm > args.LastLogTerm || (localLastLogTerm == args.LastLogTerm && localLastLogIndex > int(args.LastLogIndex)) {
		reply.Ok = false
		return
	}

	// ... grant vote.
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		reply.Ok = true

		// votedFor changed
		rf.persist()

		// Valid request from candidate, update last RPC
		rf.lastRPC.Set()

		DPrintln(Exp2A, Info, "Raft %d decided to vote for %d in term %d.", rf.me, args.CandidateId, reply.Term)
	} else {
		reply.Ok = false
	}
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
	// DPrintln(Exp2, Info, "Raft %d sending RequestVote to %d", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntries
type AppendEntriesArgs struct {
	Term         int64
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int64
	Entries      []LogEntry
	LeaderCommit int64
}

type AppendEntriesReply struct {
	Ok       bool
	Term     int64
	MyLogLen int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintln(Exp2AB, Log, "Raft %d received AppendEntries from %d.", rf.me, args.LeaderId)

	var prevLogTerm int64 = -1

	role := atomic.LoadInt64(&rf.role)
	logLen := len(rf.logs)
	if args.PrevLogIndex >= 0 && args.PrevLogIndex < logLen {
		prevLogTerm = rf.logs[args.PrevLogIndex].Term
	}

	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		DPrintln(Exp2AB, Warning, "Raft %d rejected AppendEntries from %d because of smaller term.",
			rf.me, args.LeaderId)
		reply.Ok = false
		reply.Term = rf.currentTerm
		return
	}

	// Convert to follower
	if role == RoleLeader {
		DPrintln(Exp2AB, Important, "Raft %d reverts from leader to follower by AppendEntries from %d.",
			rf.me, args.LeaderId)
	}
	atomic.StoreInt64(&rf.role, RoleFollower)
	if args.Term > reply.Term {
		rf.fastForwardToTerm(args.Term, false, true)
		role = RoleFollower
	}
	reply.Term = rf.currentTerm

	// Valid request from leader, update related variables
	atomic.StoreInt64(&rf.LeaderId, int64(args.LeaderId))
	rf.lastRPC.Set()

	// 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	phase2Flag := true

	if logLen <= int(args.PrevLogIndex) {
		phase2Flag = false
	} else if args.PrevLogIndex >= 0 {
		phase2Flag = prevLogTerm == args.PrevLogTerm
	}

	reply.MyLogLen = logLen
	if !phase2Flag {
		if len(args.Entries) != 0 {
			DPrintln(Exp2B, Warning,
				"Raft %d rejected AppendEntries from %d because of prevLog unmatch (len %d, prevInd %d, prevTerm %d).",
				rf.me, args.LeaderId, len(args.Entries), args.PrevLogIndex, args.PrevLogTerm)
		}
		reply.Ok = false
		return
	}

	// true if follower contained entry matching prevLogIndex and prevLogTerm
	reply.Ok = true

	// 3. If an existing entry conflicts with a new one, delete the existing entry and all that follows it
	// 4. Append any new entries not already in the log
	lastEntry := len(rf.logs) - 1
	if len(args.Entries) > 0 {
		DPrintln(Exp2C, Info, "Raft %d: before AE from %d (prev %d), log = %v.",
			rf.me, args.LeaderId, args.PrevLogIndex, rf.logs)

		firstUnmatch := -1
		for i := 0; i < len(args.Entries); i++ {
			index := args.PrevLogIndex + 1 + i
			if index > lastEntry || rf.logs[index].Term != args.Entries[i].Term {
				firstUnmatch = i
				break
			}
		}

		if firstUnmatch >= 0 {
			rf.logs = append(rf.logs[:args.PrevLogIndex+1+firstUnmatch], args.Entries[firstUnmatch:]...)
			DPrintln(Exp2C, Info, "Raft %d: after AE from %d, log = %v.", rf.me, args.LeaderId, rf.logs)
			lastEntry = len(rf.logs) - 1
			rf.persist() // logs changed
		} else {
			DPrintln(Exp2C, Info, "Raft %d: after AE from %d, unchanged.", rf.me, args.LeaderId)
		}
	}

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > atomic.LoadInt64(&rf.commitIndex) {
		atomic.StoreInt64(&rf.commitIndex, min(args.LeaderCommit, int64(lastEntry)))
		rf.applyCond.Signal()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// DPrintln(Exp2, Info, "Raft %d sending AppendEntries to %d", rf.me, server)
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

		rf.mu.RLock()
		entry := rf.logs[applyIndex]
		rf.mu.RUnlock()

		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: int(applyIndex) + 1,
		}
		rf.applyCh <- applyMsg

		DPrintln(Exp2B, Important, "Raft %d successfully applied log %v.", rf.me, applyMsg)
		// rf.tracer.Append(fmt.Sprintf("Applied log[%d] = %+v.", applyIndex, entry))
	}
}

func (rf *Raft) logReplicator(server int) {
	const continuousRejectThreshold = 3

	for !rf.killed() {
		time.Sleep(time.Millisecond * 10)
		if atomic.LoadInt64(&rf.role) == RoleLeader {
			unprepared := false
			var nextIndex int64 = -1

			rf.mu.RLock()
			rf.indexesLock.RLock()
			lastLogIndex := len(rf.logs) - 1
			if len(rf.nextIndex) == len(rf.peers) {
				nextIndex = rf.nextIndex[server]
			} else {
				unprepared = true
			}
			rf.indexesLock.RUnlock()
			rf.mu.RUnlock()

			if lastLogIndex < 0 || unprepared {
				// Only replicate if there are logs
				continue
			}

			if lastLogIndex >= 0 && lastLogIndex < int(nextIndex) {
				// No need to append log
				continue
			}

			continuousRejects := 0
			for !rf.killed() {
				rf.mu.RLock()
				term := rf.currentTerm
				role := atomic.LoadInt64(&rf.role)
				rf.mu.RUnlock()

				if role != RoleLeader {
					break
				}

				ok, logLen, replUntil := rf.doSendAppendEntries(server, term, int(nextIndex))
				if ok == RPCRetracted {
					break
				}

				if ok == RPCOk {
					if replUntil >= 0 {
						DPrintln(Exp2AB, Info, "Raft %d: follower %d successfully replicated log to %d.",
							rf.me, server, replUntil)
					}
					rf.indexesLock.Lock()
					rf.nextIndex[server] = int64(replUntil + 1)
					rf.matchIndex[server] = int64(replUntil)
					rf.indexesLock.Unlock()
					break
				} else if ok == RPCRejected {
					DPrintln(Exp2AB, Warning, "Raft %d: follower %d rejected log replication (%d to %d).",
						rf.me, server, nextIndex, lastLogIndex)

					if atomic.LoadInt64(&rf.role) != RoleLeader {
						break
					}
					continuousRejects += 1

					rf.indexesLock.Lock()
					if continuousRejects >= continuousRejectThreshold {
						nextIndex = 0
					} else if int(nextIndex) > logLen {
						// nextIndex fast-backwarding
						nextIndex = int64(logLen)
					} else {
						nextIndex--
					}
					rf.nextIndex[server] = nextIndex
					rf.indexesLock.Unlock()

					if nextIndex < 0 {
						DPrintln(Exp2B, Error,
							"Raft %d tries to replicate logs to %d before the first entry (nextIndex = %d)!",
							rf.me, server, nextIndex)
					}
					continue
				}
				DPrintln(Exp2ABC, Warning, "Raft %d losts its AppendEntries to %d!", rf.me, server)
				time.Sleep(time.Millisecond * 10)
			}
		}
	}
}

func (rf *Raft) logCommitter() {
	for !rf.killed() {
		if atomic.LoadInt64(&rf.role) == RoleLeader {
			matchIndex := make([]int, len(rf.peers))

			// Copy matchIndex
			rf.mu.RLock()
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
			if majorityMatched >= len(rf.logs) {
				majorityMatched = len(rf.logs) - 1
			}

			// N > commitIndex ...1
			ok := majorityMatched > int(atomic.LoadInt64(&rf.commitIndex))

			// ... and log[N].term == currentTerm
			ok = ok && (rf.logs[majorityMatched].Term == rf.currentTerm)
			rf.mu.RUnlock()

			// Set commitIndex = N
			if ok {
				DPrintln(Exp2B, Important, "Raft %d decided logs to %d is commited.", rf.me, majorityMatched)
				// rf.tracer.Append(fmt.Sprintf("Committed log to %d.", majorityMatched))
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader := atomic.LoadInt64(&rf.role) == RoleLeader

	// DPrintln(Exp2B, Info, "Raft %d receives an agreement request: %v.", rf.me, command)

	// Your code here (2B).
	if !isLeader {
		// If not leader, returns false
		return -1, -1, isLeader
	}

	// Starts agreement
	term := rf.currentTerm
	index := len(rf.logs)
	rf.logs = append(rf.logs, LogEntry{Term: term, Command: command})
	rf.persist()

	DPrintln(Exp2B, Log, "Raft %d starts agreement on term %d, index %d.", rf.me, term, index)

	return index + 1, int(term), isLeader
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

func (rf *Raft) broadcast(kind int, term int64) {
	if kind == BroadcastHeartbeat && atomic.LoadInt64(&rf.role) != RoleLeader {
		return
	}
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		switch kind {
		case BroadcastRequestVote:
			go rf.doSendRequestVote(i, term)
		case BroadcastHeartbeat:
			go rf.doSendAppendEntries(i, term, -1)
		}
	}
}

func (rf *Raft) doSendRequestVote(server int, term int64) int {
	// prepare a RequestVote RPC
	args := RequestVoteArgs{}
	reply := RequestVoteReply{}

	args.Term = term
	args.CandidateId = rf.me

	rf.mu.RLock()
	args.LastLogIndex = int64(len(rf.logs) - 1)
	if args.LastLogIndex < 0 {
		args.LastLogTerm = -1
	} else {
		args.LastLogTerm = rf.logs[args.LastLogIndex].Term
	}
	rf.mu.RUnlock()

	ok := rf.sendRequestVote(server, &args, &reply)
	if !ok {
		return RPCLost
	}

	// If RPC response contains term > currentTerm, set currentTerm = term, convert to follower
	if reply.Term > args.Term {
		rf.fastForwardToTerm(reply.Term, true, true)
		return RPCRetracted
	}

	// Need to verify that I am still a candidate
	if atomic.LoadInt64(&rf.role) != RoleCandidate {
		return RPCRetracted
	}

	if reply.Ok && reply.Term == args.Term {
		atomic.AddInt64(&rf.receivedVotes, 1)
		return RPCOk
	}

	// The RequestVote is rejected because of log unmatch
	return RPCRejected
}

// Returns (RPCStatus, RemoteLogLen, LocalReplicatedLogEnd)
func (rf *Raft) doSendAppendEntries(server int, term int64, head int) (int, int, int) {
	DPrintln(Exp2A, Log, "Raft %d: doSendAppendEntries to %d", rf.me, server)

	args := AppendEntriesArgs{}
	reply := AppendEntriesReply{}

	rf.mu.RLock()
	args.Term = term
	args.LeaderId = rf.me
	logLen := len(rf.logs)
	if head < 0 {
		head = logLen
	}

	args.PrevLogIndex = head - 1
	if head == 0 {
		args.PrevLogTerm = -1
	} else {
		args.PrevLogTerm = rf.logs[head-1].Term
	}
	args.Entries = make([]LogEntry, logLen-int(head))
	copy(args.Entries, rf.logs[head:logLen])
	rf.mu.RUnlock()

	args.LeaderCommit = atomic.LoadInt64(&rf.commitIndex)

	ok := rf.sendAppendEntries(server, &args, &reply)
	if !ok {
		return RPCLost, 0, -1
	}

	res := RPCOk
	// If RPC response contains term > currentTerm, set currentTerm = term, convert to follower
	if reply.Term > args.Term {
		DPrintln(Exp2B, Warning, "Raft %d fast-forwards to term %d and convert to follower.", rf.me, reply.Term)
		rf.fastForwardToTerm(reply.Term, true, true)
		res = RPCRetracted
	} else if !reply.Ok {
		// DPrintln(Exp2A, Warning,
		// 	"Raft %d: heartbeat (term: %d) rejected by peer %d (term: %d)",
		// 	rf.me, atomic.LoadInt64(&rf.currentTerm), server, reply.Term)
		res = RPCRejected
		if head == 0 && atomic.LoadInt64(&rf.role) == RoleLeader {
			DPrintln(Exp2C, Warning, "Raft %d send whole log to %d but is rejected!", rf.me, server)
		}
	}
	return res, reply.MyLogLen, (logLen - 1)
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		last := rf.lastRPC.Get()
		time.Sleep(time.Millisecond * time.Duration(rnd(TimeoutMin, TimeoutMax)))

		// waken up
		cur := rf.lastRPC.Get()
		if cur == last {
			// no RPC arrived, begin election
			if atomic.LoadInt64(&rf.role) != RoleFollower {
				// Only followers can transit to candidate
				continue
			}

			// repeat election until I am not a candidate
			for !rf.killed() {
				rf.mu.Lock()
				rf.currentTerm++
				term := rf.currentTerm
				atomic.StoreInt64(&rf.role, RoleCandidate)
				rf.votedFor = rf.me
				rf.persist()
				rf.mu.Unlock()

				DPrintln(Exp2A, Important, "Raft %d starts election at term %d!", rf.me, term)

				timeout := time.Millisecond * time.Duration(rnd(TimeoutMin, TimeoutMax))

				atomic.StoreInt64(&rf.receivedVotes, 1)
				rf.broadcast(BroadcastRequestVote, term)

				startTime := time.Now()
				// as long as I am still a candidate
				for !rf.killed() && atomic.LoadInt64(&rf.role) == RoleCandidate {
					if time.Since(startTime) > timeout {
						// (c) a period of time goes by no winner
						//  -> abort and go to next term
						DPrintln(Exp2B|Exp2C, Warning, "Raft %d withdraws election in term %d for timeout.",
							rf.me, term)
						break
					}

					if atomic.LoadInt64(&rf.receivedVotes) >= int64(majority(len(rf.peers))) {
						// (a) I win the election
						DPrintln(Exp2A, Important, "Raft %d thinks it has won!", rf.me)
						// rf.tracer.Append(fmt.Sprintf("Win election in term %d.", term))
						rf.initFollowerIndexes()
						atomic.StoreInt64(&rf.LeaderId, int64(rf.me))
						atomic.StoreInt64(&rf.role, RoleLeader)
						rf.broadcast(BroadcastHeartbeat, term)
					}
					time.Sleep(time.Millisecond * 10)
				}
				// (a) I win the election; or
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
	for !rf.killed() {
		term, role := rf.GetTermAndRole()
		if role == RoleLeader {
			rf.broadcast(BroadcastHeartbeat, term)
		}
		time.Sleep(time.Millisecond * TimeoutHeartbeat)
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
	// Initialize persistent state
	rf.readPersist(persister.ReadRaftState())

	// Initialize volatile state
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.applyCh = applyCh

	atomic.StoreInt64(&rf.role, RoleFollower)
	rf.lastRPC.Set()

	// initialize from state persisted before a crash
	// rf.readPersist(persister.ReadRaftState())

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
