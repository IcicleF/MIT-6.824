package raft

import (
	"fmt"
	"os"
)

// Debugging
const Debug = true

const (
	Log = iota
	Info
	Warning
	Error
	Important
	None

	Exp2A = 0x1
	Exp2B = 0x2
	Exp2C = 0x4
	Exp2D = 0x8

	Exp2AB  = 0x3
	Exp2ABC = 0x7
	Exp2    = 0xF
)

const ShownLogLevel = Info
const ShownPhase = Exp2A

func DPrintln(phase int, typ int, format string, a ...interface{}) (n int, err error) {
	if typ == Error || ((Debug && ((phase & ShownPhase) != 0)) && typ >= ShownLogLevel) {
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
		params = append(params, color, prefix)
		params = append(params, a...)
		fmt.Printf("\x1b[0;%dm  %v"+format+"\x1b[0m\n", params...)
	}
	if typ == Error {
		fmt.Printf("\x1b[0;31m*** Exit because of unexpected situation. ***\x1b[0m\n\n")
		os.Exit(-1)
	}
	return
}
