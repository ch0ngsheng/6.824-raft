package raft

import (
	"time"
)

const (
	raftFollower uint32 = iota
	raftLeader
	raftCandidate

	raftFollowerRole  = "follower"
	raftLeaderRole    = "leader"
	raftCandidateRole = "candidate"
)

var roleMap = map[uint32]string{}

type heartbeatHandler func(raft *Raft)

var heartbeatHandlerMap = map[uint32]heartbeatHandler{}

func init() {
	roleMap[raftFollower] = raftFollowerRole
	roleMap[raftCandidate] = raftCandidateRole
	roleMap[raftLeader] = raftLeaderRole

	heartbeatHandlerMap[raftFollower] = followerHandler
	heartbeatHandlerMap[raftCandidate] = candidateHandler
	heartbeatHandlerMap[raftLeader] = leaderHandler
}

// candidateHandler candidate的心跳定时器到期
func candidateHandler(rf *Raft) {
	rfc := rf.getInfoLocked()
	rf.DPrintf("HB: <%d-%s>: (ignore)", rfc.me, rfc.getRole())
	// 正在选举，忽略心跳到期事件
}

// followerHandler follower的心跳定时器到期
func followerHandler(rf *Raft) {
	rfc := rf.getInfoLocked()
	rf.DPrintf("HB: <%d-%s>: (to rise election)", rfc.me, rfc.getRole())
	// 转为candidate，发起新的选举

	resultChan := make(chan int, 0)
	go requestVote(rf, resultChan)

	ticker := time.NewTicker(time.Millisecond * 100)
	var result int
	loop := true
	for loop {
		select {
		case <-ticker.C:
			rfc = rf.getInfoLocked()
			if rf.killed() {
				rf.DPrintf("HB: <%d-%s>: found killed when waiting vote result %p", rfc.me, rfc.getRole(), rf)
				return
			}
		case result = <-resultChan:
			loop = false
		}
	}
	ticker.Stop()
	// rf.DPrintf("HB: <%d-%s>: vote result: %d.", rf.me, rf.getRole(), result)

	rfc = rf.getInfoLocked()
	switch result {
	case voteResultKilled:
		return
	case voteResultStopped, voteResultBeFollower, voteResultLose:
		rf.DPrintf("HB: <%d-%s>: vote end, be follower because of %d", rfc.me, rfc.getRole(), result)
		rf.switchRoleLocked(raftFollower)
	case voteResultTimeout:
		// 重新选举
		rf.DPrintf("HB: <%d-%s>: vote timeout, try again", rfc.me, rfc.getRole())
		rf.switchRoleLocked(raftFollower)
		followerHandler(rf)
	case voteResultWin:
		rf.DPrintf("HB: <%d-%s>: vote win", rfc.me, rfc.getRole())
		rf.switchRoleLocked(raftLeader)
		// 变成leader，立即发送心跳
		go appendEntry(rf)
	default:
		panic("HB: unknown vote result")
	}
}

// leaderHandler leader的心跳定时器到期
func leaderHandler(rf *Raft) {
	rfc := rf.getInfoLocked()
	rf.DPrintf("HB: <%d-%s>: (to append entry)", rfc.me, rfc.getRole())
	go appendEntry(rf)

}
