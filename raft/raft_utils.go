package raft

import (
	"sync/atomic"
	"time"
)

// getLastLogNumber 获取最后一条日志条目，返回索引号，任期号
func (rf *Raft) getLastLogNumber() (index uint64, term uint64) {
	lens := uint64(len(rf.logs))
	return lens - 1, rf.logs[lens-1].Term
}

func (rf *Raft) getRole() string {
	role := atomic.LoadUint32(&rf.role)
	return roleMap[role]
}

func (rf *Raft) switchRole(role uint32) {
	rf.role = role

	if role == raftLeader {
		rf.nextIndex = make([]uint64, len(rf.peers))
		rf.matchIndex = make([]uint64, len(rf.peers))
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = uint64(len(rf.logs)) // 初始值为leader最后一个日志条目的索引值+1
			rf.matchIndex[i] = 0
		}
		rf.matchIndex[rf.me] = uint64(len(rf.logs) - 1)

		// 重置logTermsFirst, logTermsLast
		rf.logTermsFirst = make(map[uint64]uint64)
		rf.logTermsLast = make(map[uint64]uint64)
		for i := 0; i < len(rf.logs); i++ {
			log := rf.logs[i]
			if _, ok := rf.logTermsFirst[log.Term]; !ok {
				rf.logTermsFirst[log.Term] = uint64(i)
			}
			rf.logTermsLast[log.Term] = uint64(i)
		}

		rf.persist()
	}
}

func (rf *Raft) updateMatchIndex(who int, matchIndex uint64) {
	rf.matchIndex[who] = matchIndex
	rf.nextIndex[who] = matchIndex + 1
}

func (rf *Raft) updateCommitIndex() {
	/*
		Fig.8的要求，只通过当前term的日志判断是否更新commitIndex
			If there exists an N such that
				N > commitIndex,
				a majority of matchIndex[i] ≥ N,
				and log[N].term == currentTerm:
			set commitIndex = N

		实际实现时做了扩展：
			如果全部节点都满足 matchIndex[i] >= N，set commitIndex = N
			此时不要求log[N].Term == currentTerm
	*/
	for i := rf.commitIndex + 1; i < uint64(len(rf.logs)); i++ {
		/*if rf.logs[i].Term != rf.term {
			continue
		}*/

		var cnt int
		for j := 0; j < len(rf.peers); j++ {
			if rf.matchIndex[j] >= i {
				cnt += 1
			}
		}
		if rf.logs[i].Term == rf.term && cnt > len(rf.peers)/2 {
			rf.commitIndex = i
		} else if rf.logs[i].Term != rf.term && cnt == len(rf.peers) {
			rf.commitIndex = i
			rf.DPrintf("ALL MATCH, update commitIndex from earlier term %d", rf.logs[i].Term)
		}
	}
}

func (rf *Raft) updateTermAndPersist(term uint64) {
	rf.term = term
	rf.votedFor = -1

	rf.persist()
}
func (rf *Raft) updateVotedForAndPersist(id int32) {
	rf.votedFor = id
	rf.persist()
}

func (rf *Raft) updateNextIndex(who int, xIndex uint64, xTerm uint64, xLen uint64) {
	if xLen != 0 {
		rf.nextIndex[who] = xLen
		return
	}
	if idx, ok := rf.logTermsLast[xTerm]; ok {
		rf.nextIndex[who] = idx + 1
		return
	}
	rf.nextIndex[who] = xIndex
}

func (rf *Raft) rstElectionTimer() {
	du := getRandomDuration(ElectionTimeout, rf.me)
	if !rf.timer.Stop() {
		select {
		case <-rf.timer.C:
		default:

		}
	}
	rf.timer.Reset(du)
	rf.timerResetTime = time.Now()

	rf.DPrintf("<RST AE Timer><%d-%s> val: %v", rf.me, rf.getRole(), du)
}

func (rf *Raft) rstLeaderTimer() {
	du := HeartbeatInterval
	if !rf.timer.Stop() {
		select {
		case <-rf.timer.C:
		default:

		}
	}
	rf.timer.Reset(du)
	rf.timerResetTime = time.Now()

	rf.DPrintf("<RST AE Timer><%d-%s> val: %v", rf.me, rf.getRole(), du)
}
