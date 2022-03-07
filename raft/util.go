package raft

import (
	"fmt"
	"time"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		t := fmt.Sprintf("%s%s", time.Now().Format(time.RFC3339Nano), " ")
		fmt.Printf(t+format+"\n", a...)
	}
	return
}
