package raft

import (
	"time"
)

// getLastLogNumber 获取最后一条日志条目，返回索引号，任期号
func (rf *Raft) getLastLogNumber() (index uint64, term TermType) {
	lens := uint64(len(rf.logs))
	return lens - 1, rf.logs[lens-1].Term
}

func (rf *Raft) switchToCandidateAndPersist() {
	rf.requestVoteTimes += 1
	rf.votedFor = int32(rf.me)
	rf.term += 1
	rf.role = Candidate
	rf.persist()
	rf.voteCounter.Reset()
}

func (rf *Raft) updateMatchIndex(who int, matchIndex uint64) {
	rf.matchIndex[who] = matchIndex
	rf.nextAppendEntryIndex[who] = matchIndex + 1
}

func (rf *Raft) updateCommitIndex() {
	/*
		论文Fig.8的要求，只通过当前term的日志判断是否更新commitIndex
			If there exists an N such that
				N > commitIndex,
				a majority of matchIndex[i] ≥ N,
				and log[N].term == currentTerm:
			set commitIndex = N

		论文中还提到，刚切换为Leader时写入当前term下的一条no-op日志，解决没有客户端请求进来时，无法推进commitIndex的问题
	*/
	for i := rf.commitIndex + 1; i < uint64(len(rf.logs)); i++ {
		if rf.logs[i].Term != rf.term {
			continue
		}

		var cnt int
		for j := 0; j < len(rf.peers); j++ {
			if rf.matchIndex[j] >= i {
				cnt += 1
			}
		}
		if rf.logs[i].Term == rf.term && cnt > len(rf.peers)/2 {
			rf.commitIndex = i
		}
	}
}

func (rf *Raft) switchToLeaderAndPersist() {
	rf.role = Leader
	rf.votedFor = -1

	// 发送一次心跳No-OP
	log := &Log{
		Term:    rf.term,
		Command: -1,
		NoOp:    true,
	}
	rf.WriteLog(log)
	go appendEntry(rf)

	// 初始化
	rf.nextAppendEntryIndex = make([]uint64, len(rf.peers))
	rf.matchIndex = make([]uint64, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextAppendEntryIndex[i] = uint64(len(rf.logs)) // 初始值为leader最后一个日志条目的索引值+1
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = uint64(len(rf.logs) - 1)

	// 重置logTermsFirst, LastIndexPerTerm
	rf.FirstIndexPerTerm = make(map[TermType]uint64)
	rf.LastIndexPerTerm = make(map[TermType]uint64)

	// 重置 totalNoOP
	rf.totalNoOPLogs = 0

	for i := 0; i < len(rf.logs); i++ {
		log := rf.logs[i]
		if _, ok := rf.FirstIndexPerTerm[log.Term]; !ok {
			rf.FirstIndexPerTerm[log.Term] = uint64(i)
		}
		rf.LastIndexPerTerm[log.Term] = uint64(i)

		if log.NoOp {
			rf.totalNoOPLogs += 1
		}
	}

	rf.persist()
}

func (rf *Raft) switchToFollowerAndPersist(term TermType) {
	rf.role = Follower
	rf.term = term
	rf.votedFor = -1
	rf.persist()
}

func (rf *Raft) updateVotedForAndPersist(id int32) {
	rf.votedFor = id
	rf.persist()
}

func (rf *Raft) updateNextIndex(who int, xIndex uint64, xTerm TermType, xLen uint64) {
	if xLen != 0 {
		rf.nextAppendEntryIndex[who] = xLen
		return
	}
	if idx, ok := rf.LastIndexPerTerm[xTerm]; ok {
		rf.nextAppendEntryIndex[who] = idx + 1
		return
	}
	rf.nextAppendEntryIndex[who] = xIndex
}

// resetElectionTimer 重置选举超时时间
func (rf *Raft) resetElectionTimer() {
	du := GetRandomDuration(ElectionTimeout, rf.me)
	rf.resetTimer(du)
}

// resetLeaderTimer 重置Leader发送心跳时间
func (rf *Raft) resetLeaderTimer() {
	du := HeartbeatInterval
	rf.resetTimer(du)
}

func (rf *Raft) resetTimer(du time.Duration) {
	if !rf.electionTimer.Stop() { // mark 此时，raft_job.go 中正在 <-electionTimer.C 阻塞
		select {
		case <-rf.electionTimer.C:
		default:

		}
	}
	rf.electionTimer.Reset(du)
	rf.timerResetTime = time.Now()

	DPrintf("<RST AE Timer><%d-%s> val: %v, %s", rf.me, rf.role, du, rf.timerResetTime.Format(time.RFC3339Nano))
}
