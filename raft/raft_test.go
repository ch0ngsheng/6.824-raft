package raft

import (
	"crypto/md5"
	"fmt"
	"log"
	"testing"
)

func TestGetRandom(t *testing.T) {
	base := heartbeatInterval
	for i := 0; i < 10; i++ {
		for id := 0; id < 3; id++ {
			fmt.Printf("%s-", getRandomDuration(base, id))
		}
		fmt.Println()
	}
}

func TestGolang(t *testing.T) {
	s := []int{1, 2}
	fmt.Println(s[2:])
}

func TestMd5(t *testing.T) {
	md5Val := md5.Sum([]byte("0"))
	fmt.Printf("%x\n", md5Val)

	str1 := getMd5(5542)
	fmt.Printf("%s\n%x\n", str1, str1)

	log.Printf("tst")
}
