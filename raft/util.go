package raft

import (
	"fmt"
	"math/rand"
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

func GetRandomDuration(base time.Duration, id int) time.Duration {
	// rf.me作随机种子，保证每个节点种子不一样，足够随机
	random := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
	randomVal := random.Intn(100)

	return time.Duration(randomVal)*time.Millisecond + base
}
