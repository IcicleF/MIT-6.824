package raft

import "fmt"

// Debugging
const Debug = true

const (
	Raw = iota
	Info
	Important
	Warning
	Error
)

func DPrintln(typ int, format string, a ...interface{}) (n int, err error) {
	if Debug {
		var prefix string
		var color int = 0

		switch typ {
		case Raw:
			prefix = ""
		case Info:
			prefix = "[INFO] "
		case Important:
			prefix = "[INFO !] "
			color = 1
		case Warning:
			prefix = "[WARN] "
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
	return
}
