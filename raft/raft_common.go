package raft

import (
	"crypto/md5"
	"fmt"
	"math/rand"
	"strings"
	"time"
)

// getRandomDuration 在base基础上随机增加若干时间
func getRandomDuration(base time.Duration, id int) time.Duration {
	// rf.me作随机种子，保证每个节点种子不一样，足够随机
	random := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
	randomVal := random.Intn(100)

	return time.Duration(randomVal)*time.Millisecond + base
}

func (rf *Raft) DPrintf(format string, a ...interface{}) {
	p := fmt.Sprintf("%p ", rf)
	DPrintf(p+format, a...)
}

func logsToString(logs []*raftLog) string {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
			fmt.Println("val:", logs)
		}
	}()
	sb := strings.Builder{}
	for _, log := range logs {
		l := fmt.Sprintf("[%d-%x]", log.Term, getMd5(log.Entry))
		sb.WriteString(l)
	}
	return sb.String()
}

func getMd5(val interface{}) string {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
			fmt.Println("val:", val)
		}
	}()

	str := fmt.Sprintf("%s", val)
	md5Val := md5.Sum([]byte(str))
	return fmt.Sprintf("%x", md5Val)[:8]
}
