package shardkv

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
	"6.824/shardctrler"
)

func maxi64(x int64, y int64) int64 {
	if x < y {
		return y
	}
	return x
}

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

	Exp4B = 0x1
)

const ShownLogLevel = Info
const ShownPhase = Exp4B
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

// END OF SELF-DEFINED DEBUGGING LIBRARY
// ===========================================================================================

// ===========================================================================================
// BEGIN OF DESCRIPTION
//
// 协议：
// 1. Configuration 更新
//   (1) 不允许跳步，必须按 configuration 序号依次更新；
//   (2) 全局完成一次更新前，禁止下一次更新；
//   (3) 状态：NORMAL -> MIGRATING -> WAITING -> NORMAL -> ...
// 2. NORMAL：正常工作，并且没有发现新的 configuration；通常全局处于 NORMAL 并使用同一个 configuration；
// 3. MIGRATING：Raft leader 发现 MigrationStart 被 commit，向下一个 configuration 迁移；
// 4. WAITING：当前 Raft group 已经完成迁移，按照下一个 configuration 提供服务，但全局没有都完成。

// 实现细节：
// 1. 仅在 NORMAL(n) 收到 MigrationStart(n+1) 时才转移到 MIGRATING，否则 no-op；
// 2. 更新时快照当前 KV store，并且仅继续对【与本次更新无关（既不拿来也不被拿走）】的 shard 提供服务；
// 3. 主动拉取 shard，拉取时双方必须均处于 MIGRATING 状态，否则拒绝，发方重试；
// 4. 拉取回来并 MultiPut 成功后，就可以立即开始对这个 shard 提供服务；
// 5. issued 在进入新状态后清空（可以防止发起两轮请求）；
// 6. 处理 join：test 里面不会动态新建和删除 Raft group。因此如果下一个 configuration 里面没有我的事，我可以直接转移过去。

// 程序逻辑：
// 1. configUpdater 检测到新 configuration，存在 kv.future 里面并提交 MigrationStart；
// 2. poller -> executeServerOp 检测到 MigrationStart commit
//   (1) 如果离开 configuration（前一个与我有关，下一个与我无关）：保存 oldstore，立即迁移至 NORMAL(n+1)；
//   (2) 如果前后都在 configuration 外，不覆盖写 oldstore，并且立即迁移至 NORMAL(n+1)；
//   (3) 否则，迁移至 MIGRATING；
// 3. shardPuller 检测到 MIGRATING 并确认 Raft 状态，如果是 leader，开始拉取 shard；
// 4. shardPuller::pullShard 通过 RPC 取得 shard 并提交给 Raft；
// 5. poller -> executeServerOp 检测到 Migrate commit，整合进 KV store，并在接受完时将 kv.future 变为当前 config，迁移到 WAITING；
// 6. configUpdater 检测到全局更新完成并迁移到 NORMAL。

// END OF DESCRIPTION
// ===========================================================================================

const (
	GetOp = iota
	PutOp
	AppendOp

	MigrationStartOp = iota
	MultiPut
)
const NShards = shardctrler.NShards

func str2op(op string) int {
	if op == "Put" {
		return PutOp
	}
	return AppendOp
}

type ClientOp struct {
	CliId int64
	SeqId int64
	Type  int
	Key   string
	Value string
}

type ServerOp struct {
	MigrateTo int               // # of config to migrate to
	ShardId   int               // -1 indicates MigrationStart
	Shard     map[string]string // nil indicates MigrationStart
	Executed  map[int64]int64   // nil indicates MigrationStart
}

// State only meaningful to Raft leaders
const (
	NORMAL    = iota // Operating normally under current configuration
	MIGRATING        // Leader sees a committed MigrationStart, and is migrating to a newer configuration
	WAITING          // Leader has finished migration, but it is waiting for global view change completion
)

type ShardKV struct {
	mu       sync.Mutex
	me       int
	rf       *raft.Raft
	applyCh  chan raft.ApplyMsg
	make_end func(string) *labrpc.ClientEnd
	gid      int
	ctrlers  []*labrpc.ClientEnd

	maxraftstate int             // snapshot if log grows this big
	persister    *raft.Persister // Raft persister

	// Your definitions here.
	// My state: NORMAL -> MIGRATING -> WAITING -> NORMAL -> ...
	state int64

	mck    *shardctrler.Clerk
	config shardctrler.Config
	future shardctrler.Config

	killed        int64             // Whether I am killed
	receivedIndex int               // Raft log index I have received
	executed      map[int64]int64   // Deduplication
	store         map[string]string // Real KV store
	oldstore      map[string]string // Snapshotted KV store
	oldstoreNum   int               // Configuration number of oldstore
	servable      [NShards]bool     // Shards that I can serve now
	issued        int64             // Whether MigrationStart or ShardPull are issued (VOLATILE)
}

func copyStore(src map[string]string) map[string]string {
	res := map[string]string{}
	for k, v := range src {
		res[k] = v
	}
	return res
}

func copyExecuted(src map[int64]int64) map[int64]int64 {
	res := map[int64]int64{}
	for k, v := range src {
		res[k] = v
	}
	return res
}

func (kv *ShardKV) getServable(config shardctrler.Config) [NShards]bool {
	res := [NShards]bool{}
	for i := 0; i < NShards; i++ {
		res[i] = (config.Shards[i] == kv.gid)
	}
	return res
}

func (kv *ShardKV) isActive(config shardctrler.Config) bool {
	for i := 0; i < NShards; i++ {
		if config.Shards[i] == kv.gid {
			return true
		}
	}
	return false
}

func (kv *ShardKV) performClientOp(op ClientOp) (int, string) {
	// Check whether I am responsible for this key
	kv.mu.Lock()
	shard := key2shard(op.Key)
	gid := kv.config.Shards[shard]
	kv.mu.Unlock()

	if gid != kv.gid {
		// Reject if not responsible
		return ErrWrongGroup, ""
	}

	// Check whether I am leader
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		DPrintln(Exp4B, Log, "KV (g-%d, %d, config ?) rejected op because it is not leader.", kv.gid, kv.me)
		return ErrWrongLeader, ""
	}

	DPrintln(Exp4B, Info, "KV (g-%d, %d, config ?) informed Raft of op[%d] = %+v.", kv.gid, kv.me, index, op)
	reply := ""
	for !kv.Killed() {
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
			return ErrWrongLeader, ""
		}

		if receivedIndex < index {
			continue
		}
		if !ok || curSeq < op.SeqId {
			// Index found, however RPC not executed
			DPrintln(Exp4B, Warning, "KV (g-%d, %d, config ?) found op id (%d,%d) is not confirmed.",
				kv.gid, kv.me, op.CliId, op.SeqId)
			return ErrWrongLeader, ""
		}

		err := OK
		func() {
			kv.mu.Lock()
			defer kv.mu.Unlock()

			// Now, the current op has been committed by Raft. However, it can be rejected by configuration changes.
			if !kv.servable[key2shard(op.Key)] {
				DPrintln(Exp4B, Warning, "KV (g-%d, %d, config ?) found op %+v is committed, but cannot serve it.",
					kv.gid, kv.me, op)
				err = ErrWrongGroup
			}

			if op.Type == GetOp {
				val, ok := kv.store[op.Key]
				if ok {
					reply = val
				}
			}
		}()
		return err, reply
	}

	// Must be killed
	return ErrRejected, ""
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := ClientOp{Type: GetOp, Key: args.Key, CliId: args.CliId, SeqId: args.SeqId}
	reply.Err, reply.Value = kv.performClientOp(op)
	DPrintln(Exp4B, Log, "KV (g-%d, %d, config ?): Get %+v -> %+v.", kv.gid, kv.me, args, reply)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := ClientOp{Type: str2op(args.Op), Key: args.Key, Value: args.Value, CliId: args.CliId, SeqId: args.SeqId}
	reply.Err, _ = kv.performClientOp(op)
	DPrintln(Exp4B, Log, "KV (g-%d, %d, config ?): PutAppend %+v -> %+v.", kv.gid, kv.me, args, reply)
}

func (kv *ShardKV) Migrate(args *MigrateArgs, reply *MigrateReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// If I am not leader, reject
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// If I haven't progressed to (or over) MIGRATING(args.MigrateTo), reject
	if kv.config.Num+1 < args.MigratingTo ||
		(kv.config.Num+1 == args.MigratingTo && atomic.LoadInt64(&kv.state) == NORMAL) {
		DPrintln(Exp4B, Warning, "KV (g-%d, %d, config %d), rejected Migrate (to %d) because not yet proceeded.",
			kv.gid, kv.me, kv.config.Num, args.MigratingTo)
		reply.Err = ErrNotMigrating
		return
	}

	// If configuration number does not match, reject
	if kv.oldstoreNum+1 != args.MigratingTo {
		DPrintln(Exp4B, Warning, "KV (g-%d, %d, config %d) rejected Migrate (to %d) because oldstoreNum (%d) not match!",
			kv.gid, kv.me, kv.config.Num, args.MigratingTo, kv.oldstoreNum)
		reply.Err = ErrRejected
		return
	}

	// Migrate
	reply.Err = OK
	reply.Executed = copyExecuted(kv.executed)
	reply.Shard = map[string]string{}
	for k, v := range kv.store {
		if key2shard(k) == args.ShardId {
			reply.Shard[k] = v
		}
	}
}

func (kv *ShardKV) CheckConfig(args *CheckConfigArgs, reply *CheckConfigReply) {
	// NOTICE: Pay attention to dead locks.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, isLeader := kv.rf.GetState()
	if isLeader {
		reply.Err = OK
		reply.Num = kv.config.Num
	} else {
		reply.Err = ErrWrongLeader
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt64(&kv.killed, 1)
	kv.rf.Kill()
}

func (kv *ShardKV) Killed() bool {
	killed := atomic.LoadInt64(&kv.killed)
	return killed != 0
}

func (kv *ShardKV) executeClientOp(m raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if m.CommandIndex != kv.receivedIndex+1 {
		DPrintln(Exp4B, Error, "KV (g-%d, %d, config %d) received index %d out of order (prev %d)!",
			kv.gid, kv.me, kv.config.Num, m.CommandIndex, kv.receivedIndex)
	}
	kv.receivedIndex = m.CommandIndex

	op, _ := m.Command.(ClientOp)
	curSeq := kv.executed[op.CliId]

	if curSeq < op.SeqId {
		// If not duplicate
		kv.executed[op.CliId] = op.SeqId

		// Reject operation if not executable
		if !kv.servable[key2shard(op.Key)] {
			return
		}

		if op.Type == PutOp {
			kv.store[op.Key] = op.Value
		} else if op.Type == AppendOp {
			kv.store[op.Key] = kv.store[op.Key] + op.Value
		}
	}
}

func (kv *ShardKV) executeServerOp(m raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if m.CommandIndex != kv.receivedIndex+1 {
		DPrintln(Exp4B, Error, "KV (g-%d, %d, config %d) received index %d out of order (prev %d)!",
			kv.gid, kv.me, kv.config.Num, m.CommandIndex, kv.receivedIndex)
	}
	kv.receivedIndex = m.CommandIndex

	op, _ := m.Command.(ServerOp)
	if op.ShardId == -1 {
		// This is a MigrationStart
		// Check if I am in a correct state to perform this operation (restart causes log replay to occur in wrong time)
		if kv.config.Num+1 != op.MigrateTo || atomic.LoadInt64(&kv.state) != NORMAL {
			DPrintln(Exp4B, Warning, "KV (g-%d, %d, config %d) trying to migrate to config %d, skipped!",
				kv.gid, kv.me, kv.config.Num, op.MigrateTo)
			return
		}

		// Pull configuration
		kv.future = kv.mck.Query(op.MigrateTo)

		// If the next configuration has nothing to do with me, migrate directly to NORMAL(n+1)
		if !kv.isActive(kv.future) {
			// If I am active in the old configuration, I should save the current KV store for others
			if kv.isActive(kv.config) {
				kv.oldstore = copyStore(kv.store)
				kv.oldstoreNum = kv.config.Num
			}

			// Directly migrate to a clean state
			DPrintln(Exp4B, Important, "KV (g-%d, %d, config %d): NORMAL -> next NORMAL.", kv.gid, kv.me, kv.config.Num)

			kv.store = map[string]string{}
			kv.config = kv.future
			atomic.StoreInt64(&kv.state, NORMAL)
			atomic.StoreInt64(&kv.issued, 0)
			return
		}

		// If I gets the shard from nowhere, migrate directly to NORMAL(n+1)
		if kv.future.Num == 1 {
			// Directly migrate to a clean state
			DPrintln(Exp4B, Important, "KV (g-%d, %d, config %d): initial NORMAL -> next NORMAL.",
				kv.gid, kv.me, kv.config.Num)
			kv.config = kv.future
			kv.servable = kv.getServable(kv.config)
			atomic.StoreInt64(&kv.state, NORMAL)
			atomic.StoreInt64(&kv.issued, 0)
			return
		}

		// Otherwise, snapshot current KV store, and enable service only to those prepared shards
		kv.oldstore = copyStore(kv.store)
		kv.oldstoreNum = kv.config.Num
		for i := 0; i < NShards; i++ {
			kv.servable[i] = (kv.future.Shards[i] == kv.gid) && (kv.config.Shards[i] == kv.gid || kv.config.Shards[i] == 0)
		}

		// If I don't need new shards, go to WAITING directly, otherwise go to MIGRATING
		if kv.servable == kv.getServable(kv.future) {
			DPrintln(Exp4B, Important, "KV (%d, %d, config %d -> %d): NORMAL -> WAITING.",
				kv.gid, kv.me, kv.config.Num, kv.future.Num)
			kv.config = kv.future
			atomic.StoreInt64(&kv.state, WAITING)
		} else {
			DPrintln(Exp4B, Important, "KV (g-%d, %d, config %d): NORMAL -> MIGRATING.", kv.gid, kv.me, kv.config.Num)
			atomic.StoreInt64(&kv.state, MIGRATING)
		}
		atomic.StoreInt64(&kv.issued, 0)
	} else {
		// This is a MultiPut
		// Double-check correctness and skip duplicated requests
		if kv.config.Num+1 != op.MigrateTo || atomic.LoadInt64(&kv.state) != MIGRATING {
			DPrintln(Exp4B, Warning, "KV (g-%d, %d, config %d) does a MultiPut, but not in MIGRATING, skipped!",
				kv.gid, kv.me, kv.config.Num)
			return
		}
		if kv.servable[op.ShardId] {
			DPrintln(Exp4B, Warning, "KV (g-%d, %d, config %d) skips duplicate MultiPut to shard %d.",
				kv.gid, kv.me, kv.config.Num, op.ShardId)
			return
		}

		// Put the shard into KV store
		for k, v := range op.Shard {
			kv.store[k] = v
		}

		// Merge deduplication information
		for k, v := range op.Executed {
			kv.executed[k] = maxi64(kv.executed[k], v)
		}

		// Enable service to the shard
		kv.servable[op.ShardId] = true

		// If all shards are prepared, move to WAITING and new configuration
		if kv.servable == kv.getServable(kv.future) {
			DPrintln(Exp4B, Important, "KV (%d, %d, config %d -> %d): MIGRATING -> WAITING.",
				kv.gid, kv.me, kv.config.Num, kv.future.Num)
			kv.config = kv.future
			atomic.StoreInt64(&kv.state, WAITING)
			atomic.StoreInt64(&kv.issued, 0)
		}
	}
}

func (kv *ShardKV) poller() {
	for m := range kv.applyCh {
		if kv.Killed() {
			break
		}

		if m.SnapshotValid {
			kv.mu.Lock()
			if kv.rf.CondInstallSnapshot(m.SnapshotTerm, m.SnapshotIndex, m.Snapshot) {
				DPrintln(Exp4B, Info, "KV (g-%d, %d, config %d) installing snapshot till index %d.",
					kv.gid, kv.me, kv.config.Num, m.SnapshotIndex)
				buf := bytes.NewBuffer(m.Snapshot)
				dec := labgob.NewDecoder(buf)

				var state int64
				dec.Decode(state)
				dec.Decode(&kv.config)
				dec.Decode(&kv.future)
				dec.Decode(&kv.oldstore)
				dec.Decode(&kv.oldstoreNum)
				dec.Decode(&kv.store)
				dec.Decode(&kv.executed)
				dec.Decode(&kv.servable)
				atomic.StoreInt64(&kv.state, state)

				kv.receivedIndex = m.SnapshotIndex
			}
			kv.mu.Unlock()
		} else if m.CommandValid {
			switch m.Command.(type) {
			case ClientOp:
				DPrintln(Exp4B, Log, "KV (g-%d, %d, config ?) received confirmation of client op[%d] = %+v.",
					kv.gid, kv.me, m.CommandIndex, m.Command)
				kv.executeClientOp(m)

			case ServerOp:
				DPrintln(Exp4B, Log, "KV (g-%d, %d, config ?) received confirmation of server op[%d] = %+v.",
					kv.gid, kv.me, m.CommandIndex, m.Command)
				kv.executeServerOp(m)

			default:
				DPrintln(Exp4B, Error, "KV (g-%d, %d, config ?) detected op %+v is neither client nor server op!",
					kv.gid, kv.me, m.Command)
			}

			// Check if there is need to snapshot
			stateSize := kv.persister.RaftStateSize()
			if kv.maxraftstate > 0 && stateSize >= kv.maxraftstate/10*8 {
				DPrintln(Exp4B, Info,
					"KV (g-%d, %d, config ?) snapshots to index %d = {store = %+v, executed = %+v} (state size %d).",
					kv.gid, kv.me, kv.receivedIndex, kv.store, kv.executed, stateSize)
				index := kv.receivedIndex

				buf := new(bytes.Buffer)
				enc := labgob.NewEncoder(buf)

				enc.Encode(atomic.LoadInt64(&kv.state))
				enc.Encode(kv.config)
				enc.Encode(kv.future)
				enc.Encode(kv.oldstore)
				enc.Encode(kv.oldstoreNum)
				enc.Encode(kv.store)
				enc.Encode(kv.executed)
				enc.Encode(kv.servable)

				snapshot := buf.Bytes()
				kv.rf.Snapshot(index, snapshot)
			}
		}
	}
}

// This coroutine is responsible for two tasks:
// 1. Detect configuration update at leader when NORMAL, and issues MigrationStart to Raft
// 2. Detect global update completion when WAITING, and moves to NORMAL
func (kv *ShardKV) configUpdater() {
	for !kv.Killed() {
		// Sleep first
		time.Sleep(time.Millisecond * 100)

		// Check then
		kv.mu.Lock()
		state := atomic.LoadInt64(&kv.state)
		current := kv.config
		kv.mu.Unlock()

		// If I am in the WAITING state, then I should poll other Raft groups to go NORMAL
		if state == WAITING {
			accepted := true

			// Now, future already becomes current config
			for _, group := range current.Groups {
				l := len(group)
				num := -1

				// For each group, poll until a leader successfully responds me
				for num == -1 {
					for i := 0; i < l; i++ {
						serverName := group[i]
						srv := kv.make_end(serverName)

						args := CheckConfigArgs{Gid: kv.gid}
						reply := CheckConfigReply{}
						ok := srv.Call("ShardKV.CheckConfig", &args, &reply)
						if ok && reply.Err == OK {
							num = reply.Num
							break
						}
					}
				}
				if num < current.Num {
					accepted = false
					break
				}
			}

			// If all Raft groups have successfully migrated to WAITING(current.Num), then go NORMAL
			if accepted {
				// Clean-up omitted to prevent potential problems...
				// kv.mu.Lock()
				// kv.oldstore = nil
				// kv.oldstoreNum = 0
				// kv.mu.Unlock()
				DPrintln(Exp4B, Important, "KV (g-%d, %d, config %d): WAITING -> NORMAL.", kv.gid, kv.me, current.Num)

				atomic.StoreInt64(&kv.state, NORMAL)
				atomic.StoreInt64(&kv.issued, 0)
				continue
			}
		}

		// If there is a new configuration, and I am in the normal state, then try to start migration
		newer := kv.mck.Query(current.Num + 1)
		if newer.Num > current.Num && state == NORMAL {
			// If already issued, do nothing
			if atomic.LoadInt64(&kv.issued) == 1 {
				continue
			}

			// Issue a log entry to Raft
			migrationStart := ServerOp{MigrateTo: newer.Num, ShardId: -1, Shard: nil, Executed: nil}
			_, _, isLeader := kv.rf.Start(migrationStart)
			if isLeader {
				DPrintln(Exp4B, Important,
					"KV (g-%d, %d, config %d) detected a newer config %d and issued a MigrationStart.",
					kv.gid, kv.me, current.Num, newer.Num)
				atomic.StoreInt64(&kv.issued, 1)
			}
		}
	}
}

// This coroutine is responsible for starting sub-goroutines when the KV is in MIGRATING state.
func (kv *ShardKV) shardPuller() {
	pullShard := func(future int, id int, group []string) {
		var clients []*labrpc.ClientEnd
		for _, srv := range group {
			clients = append(clients, kv.make_end(srv))
		}

		var shard map[string]string
		var executed map[int64]int64

	outfor:
		for {
			for i := 0; i < len(clients); i++ {
				args := MigrateArgs{Gid: kv.gid, MigratingTo: future, ShardId: id}
				reply := MigrateReply{}
				ok := clients[i].Call("ShardKV.Migrate", &args, &reply)
				// DPrintln(Exp4B, Info, "%+v", reply)
				if ok && reply.Err == OK {
					shard = reply.Shard
					executed = reply.Executed
					break outfor
				}
			}
			time.Sleep(time.Millisecond * 10)
		}

		kv.mu.Lock()
		defer kv.mu.Unlock()

		// Issue shard update to Raft
		op := ServerOp{MigrateTo: future, ShardId: id, Shard: shard, Executed: executed}
		_, _, isLeader := kv.rf.Start(op)
		if !isLeader {
			DPrintln(Exp4B, Warning, "KV (g-%d, %d, config %d) issued shardPuller but is not leader any more!",
				kv.gid, kv.me, future-1)
		}
	}

	for !kv.Killed() {
		time.Sleep(time.Millisecond * 100)

		// Only leader can pull shards
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			continue
		}

		// Only pull when KV is in MIGRATING state
		kv.mu.Lock()
		state := atomic.LoadInt64(&kv.state)
		config := kv.config
		future := kv.future
		kv.mu.Unlock()

		if state != MIGRATING {
			continue
		}

		// Set issued flag and start pulling shards that are located at OTHER GROUPS
		if !atomic.CompareAndSwapInt64(&kv.issued, 0, 1) {
			continue
		}
		DPrintln(Exp4B, Important, "KV (g-%d, %d, config %d) starts pulling shards.", kv.gid, kv.me, config.Num)

		for i := 0; i < NShards; i++ {
			if config.Shards[i] != 0 && config.Shards[i] != kv.gid && future.Shards[i] == kv.gid {
				group, ok := config.Groups[config.Shards[i]]
				if !ok {
					DPrintln(Exp4B, Error, "KV (g-%d, %d, config %d) cannot find group %d in config %+v.",
						kv.gid, kv.me, config.Num, config.Shards[i], config)
				}
				go pullShard(future.Num, i, group)
			}
		}
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(ClientOp{})
	labgob.Register(ServerOp{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister

	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.state = NORMAL
	kv.store = make(map[string]string)
	kv.receivedIndex = 0
	kv.executed = make(map[int64]int64)

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.config.Num = 0
	kv.config.Groups = map[int][]string{}
	kv.servable = kv.getServable(kv.config)
	kv.issued = 0

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.poller()
	go kv.configUpdater()
	go kv.shardPuller()

	return kv
}
