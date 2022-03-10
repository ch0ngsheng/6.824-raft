package raft

import (
	"crypto/md5"
	"fmt"
	"testing"
)

func TestGetRandom(t *testing.T) {
	base := heartbeatInterval
	for i := 0; i < 10; i++ {
		for id := 0; id < 5; id++ {
			fmt.Printf("%s-", getRandomDuration(base, id))
		}
		fmt.Println()
	}
}

func TestMd5(t *testing.T) {
	md5Val := md5.Sum([]byte("0"))
	fmt.Printf("%x\n", md5Val)

	str1 := getMd5(737152563984022213)
	fmt.Printf("%s\n%x\n", str1, str1)
}
