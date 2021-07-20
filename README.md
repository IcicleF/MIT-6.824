# MIT 6.824

## Lab 2A

Nothing to say.

## Lab 2B

* Persistent state（`currentTerm`、`votedFor` 和 `logs`）整体必须是原子的
* `RequestVote` 和 `AppendEntries` 两个 RPC 的处理必须是原子的（整体要加锁）
* 收到 `RequestVote` 导致 term 更新，但因为对方 log 陈旧而不给投票时，不应该刷新选举计时器
* `RequestVote` RPC 返回后，必须检查 RPC 的 term 和当前 term 是否一致
* `AppendEntries` RPC 仅在**出现不一致的日志**的情况下才会发生 log 裁剪，这是为了防止 leader 的 `AppendEntries` 乱序到达，导致原本已被成功 replicate 的 log 发生回退

## Lab 3

Too easy, omit.

## Lab 4

https://icyf.me/2021/06/26/mit-6-824-lab4b/
