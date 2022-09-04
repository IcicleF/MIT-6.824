# A Go implementation of MIT 6.824 (2021)

**DO NOT PLAGIARIZE FROM THIS REPOSITORY IF YOU ARE CURRENTLY TAKING THIS CLASS, OR YOU WILL BE IN SERIOUS VIOLATION OF THE ACADEMIC INTEGRITY CODE.**

**THERE ARE NOT ANY CORRECTNESS GUARANTEES.**

## Lab 2A

Nothing special.

## Lab 2B

Some meaningful hints from my point of view:

* If you do not want to dig into the Raft protocol, you must make everything atomic, including the persistent state (`currentTerm`, `votedFor`, and `logs`) and the RPC handlers.
  * The easiest way to do this is to acquire a global mutex.
  * If you are confident enough, use a reader-writer lock instead of a mutex.
* A `RequestVote` RPC can update my `currentTerm` but I can still reject the vote because the requester's log is too old. In such a scenario, I should not restart my vote timer.
* RPC responses can arrive at later terms than the corresponding requests. Drop the requests if the terms do not match.
* `AppendEntries` only truncates the log when there are **inconsistent** log entries. Do not truncate the log if I have newer log entriess than the RPC but without inconsistent ones. Otherwise, reordered `AppendEntries` RPCs can revert successfully appended log entries.

## Lab 3

Nothing special.

## Lab 4

Generally speaking, this lab requires some kind of epoch update protocol. I defined three states:

* `NORMAL(n)`: The whole cluster is operating on configuration `n`.
* `MIGRATING(n)`: I, a Raft leader, am migrating configuration `n` to `n+1`.
* `WAITING(n)`: I, a Rafe leader, have migrated to configuration `n`, but other Raft leaders might not.

This protocol simplifies the design since migration is performed config-by-config. One cannot jump from configuration `0` directly to configuration `123`.
Detailed protocol [here](https://icyf.me/2021/06/26/mit-6-824-lab4b/) in Chinese.
