package raft

import "log"

// Debugging
const Debug = 4

func DPrintf(debug int, format string, a ...interface{}) (n int, err error) {
	if debug > Debug {
		log.Printf(format, a...)
	}
	return
}
