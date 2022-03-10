package raft

import (
	"fmt"
)

type AppendEntryArgs struct {
	Term              uint64
	LeaderID          int
	PreLogIndex       uint64
	PreLogTerm        uint64
	Entries           []*raftLog
	LeaderCommitIndex uint64
}

type AppendEntryReply struct {
	Term    uint64
	Success bool
	// 冲突回退参数
	XIndex uint64
	XTerm  uint64
	XLen   uint64
}

type appendEntryInfo struct {
	preLogIndex uint64
	preLogTerm  uint64
	entries     []*raftLog
}

func (rf *Raft) buildAppendEntryInfoByID(who int) *appendEntryInfo {
	preLogIndex := rf.nextIndex[who] - 1
	defer func() {
		// used to debug
		if err := recover(); err != nil {
			fmt.Println(err, who, rf.me, rf.role, preLogIndex, len(rf.logs))
			panic(err)
		}
	}()

	preLogTerm := rf.logs[preLogIndex].Term
	entries := make([]*raftLog, 0)
	entries = append(entries, rf.logs[preLogIndex+1:]...)

	info := &appendEntryInfo{
		preLogIndex: preLogIndex,
		preLogTerm:  preLogTerm,
		entries:     entries,
	}
	return info
}

func (rf *Raft) buildAppendEntryInfo() map[int]*appendEntryInfo {
	m := make(map[int]*appendEntryInfo)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		m[i] = rf.buildAppendEntryInfoByID(i)
	}
	return m
}

func (rf *Raft) buildAppendEntryInfoLocked() map[int]*appendEntryInfo {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.buildAppendEntryInfo()
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.DPrintf("Receive AE>>>   me: %d, from L: %d, term: %d, preLogIdx:%d, preLogTerm:%d, leaderCommitIdx:%d, logs: %s",
		rf.me, args.LeaderID, args.Term, args.PreLogIndex, args.PreLogTerm, args.LeaderCommitIndex, logsToString(args.Entries),
	)
	defer func() {
		rf.DPrintf(">>>Receive AE   me: %d, from L: %d, resp: %v, commitIndex %d, final logs len %d, logs: %v",
			rf.me, args.LeaderID, reply, rf.commitIndex, len(rf.logs), logsToString(rf.logs),
		)
	}()

	if args.Term < rf.term {
		reply.Success = false
		reply.Term = rf.term
		return
	}

	rf.resetTimer(applierInterval)

	if rf.role != raftFollower {
		rf.switchRole(raftFollower)
	}

	if args.Term > rf.term {
		rf.switchRole(raftFollower)
		rf.updateTermAndPersist(args.Term)
	}

	if args.PreLogIndex == 0 {
		// 从第1个日志开始发送的
	} else if uint64(len(rf.logs)-1) < args.PreLogIndex {
		// 自己的日志短
		reply.Success = false
		reply.Term = rf.term
		reply.XTerm = 0
		reply.XIndex = 0
		reply.XLen = uint64(len(rf.logs))

		return
	} else if rf.logs[args.PreLogIndex].Term != args.PreLogTerm {
		// preLogIndex处的日志term不匹配
		reply.Success = false
		reply.Term = rf.term
		reply.XTerm = rf.logs[args.PreLogIndex].Term
		reply.XIndex = rf.logTermsFirst[reply.XTerm]
		reply.XLen = 0

		return
	}

	reply.Term = rf.term
	reply.Success = true

	// 追加日志
	newLogs := rf.logs[:args.PreLogIndex+1]
	newLogs = append(newLogs, args.Entries...)
	rf.logs = newLogs

	// 更新logTermsFirst, logTermsLast
	for i := args.PreLogIndex + 1; i < uint64(len(rf.logs)); i++ {
		log := rf.logs[i]
		if _, ok := rf.logTermsFirst[log.Term]; !ok {
			rf.logTermsFirst[log.Term] = i
		}
		rf.logTermsLast[log.Term] = i
	}

	rf.persist()

	// 如果leader的commitIndex更小，则不修改。否则可能导致本节点日志重复应用
	// commitIndex在leader刚当选时被重置
	if args.LeaderCommitIndex > rf.commitIndex {
		lastIndex := uint64(len(rf.logs) - 1)

		// commitIndex = min(leaderCommitIndex, len(logs)-1)
		rf.commitIndex = lastIndex
		if args.LeaderCommitIndex < lastIndex {
			rf.commitIndex = args.LeaderCommitIndex
		}
	}
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}
