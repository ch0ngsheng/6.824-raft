package raft

import "sync/atomic"

// getInfoLocked 获取rf的拷贝
func (rf *Raft) getInfoLocked() (raft *Raft) {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	return rf
}

// getLastLogNumberLocked 获取最后一条日志条目，返回索引号，任期号
func (rf *Raft) getLastLogNumberLocked() (index uint64, term uint64) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.getLastLogNumber()
}

// getLastLogNumber 获取最后一条日志条目，返回索引号，任期号
func (rf *Raft) getLastLogNumber() (index uint64, term uint64) {
	lens := uint64(len(rf.logs))
	return lens - 1, rf.logs[lens-1].Term
}

func (rf *Raft) getRole() string {
	role := atomic.LoadUint32(&rf.role)
	return roleMap[role]
}

func (rf *Raft) switchRoleLocked(role uint32) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.switchRole(role)
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
	}
}

func (rf *Raft) updateMatchIndexLocked(who int, matchIndex uint64) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.matchIndex[who] = matchIndex
	rf.nextIndex[who] = matchIndex + 1
}

func (rf *Raft) updateCommitIndexLocked() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	/*
		If there exists an N such that
			N > commitIndex,
			a majority of matchIndex[i] ≥ N,
			and log[N].term == currentTerm:
		set commitIndex = N
	*/
	for i := rf.commitIndex + 1; i < uint64(len(rf.logs)); i++ {
		if rf.logs[i].Term != rf.term {
			continue
		}

		var cnt int
		for j := 0; j < len(rf.peers); j++ {
			if rf.matchIndex[j] >= i {
				cnt += 1
				if cnt > len(rf.peers)/2 {
					rf.commitIndex = i
				}
			}
		}
	}
}

func (rf *Raft) updateTermLocked(term uint64) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.updateTerm(term)
}
func (rf *Raft) updateTerm(term uint64) {
	rf.term = term
	rf.votedFor = -1

	rf.persist()
}
func (rf *Raft) updateVotedFor(id int32) {
	rf.votedFor = id
	rf.persist()
}

func (rf *Raft) updateNextIndexLocked(who int, xIndex uint64, xTerm uint64, xLen uint64) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

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
