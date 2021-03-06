package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

const (
	MsgOK = iota
	MsgError

	MsgMapTask = iota
	MsgReduceTask
	MsgWaitTask
	MsgNoTask

	MsgMapFinished = iota
	MsgReduceFinished
)

// Add your RPC definitions here.
type GeneralCarrier struct {
	Kind     int
	Id       int
	NMaps    int
	NReduces int
	Msg      string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
