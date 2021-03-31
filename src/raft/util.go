package raft

import (
	"fmt"
	"os"
)

// Debugging
const Debug = true

const (
	Raw = iota
	Info
	Important
	Warning
	Error

	Exp2A  = 0x1
	Exp2B  = 0x2
	Exp2C  = 0x4
	Exp2D  = 0x8
	Exp2AB = 0x3
	Exp2   = 0xF
)

const ShownPhase = 0

func DPrintln(phase int, typ int, format string, a ...interface{}) (n int, err error) {
	if Debug && ((phase & ShownPhase) != 0) {
		var prefix string
		var color int = 0

		switch typ {
		case Raw:
			prefix = ""
		case Info:
			prefix = "[INFO]   "
		case Important:
			prefix = "[INFO !] "
			color = 1
		case Warning:
			prefix = "[WARN]   "
			color = 33
		case Error:
			prefix = "[ERR]  "
			color = 31
		}
		params := make([]interface{}, 0)
		params = append(params, color, prefix)
		params = append(params, a...)
		fmt.Printf("\x1b[0;%dm  %v"+format+"\x1b[0m\n", params...)
	}
	if typ == Error {
		os.Exit(-1)
	}
	return
}
