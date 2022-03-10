package raft

import (
	"fmt"
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.resetTimer(applierInterval)
	rf.DPrintf("HB: <%d-%s>: (ignore)", rf.me, rf.getRole())
}

// followerHandler follower的心跳定时器到期
func followerHandler(rf *Raft) {
	rf.mu.Lock()

	// 再次检查，是不是刚刚收到了来自leader的心跳
	now := time.Now().Unix()
	if rf.timerResetTime.Add(applierInterval/2).Unix() > now {
		rf.DPrintf("Ignore this timeout, because just receive HB from leader")
		rf.resetTimer(applierInterval)
		rf.mu.Unlock()
		return
	}

	rf.requestVoteTimes += 1
	rf.switchRole(raftCandidate)
	rf.updateTermAndPersist(rf.term + 1)
	rf.updateVotedForAndPersist(int32(rf.me))
	rf.DPrintf("HB: <%d-%s>: (to rise election) at term %d", rf.me, rf.getRole(), rf.term)
	rf.mu.Unlock()

	resultChan := make(chan int, 0)
	go requestVote(rf, resultChan)

	ticker := time.NewTicker(time.Millisecond * 60)
	var result int
	loop := true
	for loop {
		select {
		case <-ticker.C: // todo 如果到期后，没有取走，后面还会取到前面错过的事件吗？
			rf.mu.Lock()
			if rf.killed() {
				rf.DPrintf("HB: <%d-%s>: found killed when waiting vote result %p", rf.me, rf.getRole(), rf)
				rf.mu.Unlock()
				return
			}
			if rf.role == raftFollower {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
		case result = <-resultChan:
			loop = false
		}
	}
	ticker.Stop()

	rf.mu.Lock()
	switch result {
	case voteResultKilled, voteResultNotCandidate, voteResultOutdated:
		rf.mu.Unlock()
		return
	case voteResultFoundHigherTerm, voteResultLose:
		rf.DPrintf("HB: <%d-%s>: vote end, be follower because of %d", rf.me, rf.getRole(), result)
		rf.switchRole(raftFollower)
		rf.resetTimer(applierInterval)
		rf.mu.Unlock()
		return
	case voteResultTimeout:
		// 等待超时下次重新选举
		rf.DPrintf("HB: <%d-%s>: vote timeout at number%d, try again", rf.me, rf.getRole(), rf.requestVoteTimes)
		rf.switchRole(raftFollower)
		rf.mu.Unlock()
		//mark 这里不要马上开始下一轮，而是等待当前心跳自动超时（在选举开始时和选举期间，心跳到期后都会自动重置）
		//followerHandler(rf)
		return
	case voteResultWin:
		rf.DPrintf("HB: <%d-%s>: vote win", rf.me, rf.getRole())
		rf.switchRole(raftLeader)
		// 变成leader，立即发送心跳
		rf.resetTimer(applierInterval)
		rf.mu.Unlock()

		go appendEntry(rf)
	default:
		rf.mu.Unlock()
		panic(fmt.Sprintf("HB: unknown vote result %v", result))
	}
}

// leaderHandler leader的心跳定时器到期
func leaderHandler(rf *Raft) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.DPrintf("HB: <%d-%s>: (to append entry)", rf.me, rf.getRole())

	// mark leader的心跳周期要比选举超时时间election timeout小。
	rf.resetTimer(applierInterval)

	go appendEntry(rf)
}
