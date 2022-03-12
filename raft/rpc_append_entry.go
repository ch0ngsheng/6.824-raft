package raft

// AppendEntryArgs AppendEntry RPC 的请求参数
type AppendEntryArgs struct {
	Term              uint64
	LeaderID          int
	PreLogIndex       uint64
	PreLogTerm        uint64
	Entries           []*raftLog
	LeaderCommitIndex uint64
}

// AppendEntryReply AppendEntry RPC 的响应参数
type AppendEntryReply struct {
	Term    uint64
	Success bool
	// 冲突回退参数
	XIndex uint64
	XTerm  uint64
	XLen   uint64
}
type appEntryUtil struct {
}

var ae = appEntryUtil{}

// AppendEntry 处理Append Entry RPC
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

	if ok := ae.checkTerm(rf, args, reply); !ok {
		return
	}

	rf.rstElectionTimer()
	ae.updateTermAndRole(rf, args.Term)

	if can := ae.canAppend(rf, args, reply); !can {
		return
	}

	ae.appendLog(rf, args, reply)
}

func (appEntryUtil) checkTerm(rf *Raft, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	if args.Term < rf.term {
		reply.Success = false
		reply.Term = rf.term
		return false
	}
	return true
}

func (appEntryUtil) updateTermAndRole(rf *Raft, term uint64) {
	rf.switchRole(raftFollower)

	if term > rf.term {
		rf.updateTermAndPersist(term)
	}
}

func (appEntryUtil) canAppend(rf *Raft, args *AppendEntryArgs, reply *AppendEntryReply) (can bool) {
	if args.PreLogIndex == 0 {
		// 从第1个日志开始发送的
		return true
	}
	if uint64(len(rf.logs)-1) < args.PreLogIndex {
		// 自己的日志短
		reply.Success = false
		reply.Term = rf.term
		reply.XTerm = 0
		reply.XIndex = 0
		reply.XLen = uint64(len(rf.logs))

		return false
	}
	if rf.logs[args.PreLogIndex].Term != args.PreLogTerm {
		// preLogIndex处的日志term不匹配
		reply.Success = false
		reply.Term = rf.term
		reply.XTerm = rf.logs[args.PreLogIndex].Term
		reply.XIndex = rf.logTermsFirst[reply.XTerm]
		reply.XLen = 0

		return false
	}
	return true
}

func (appEntryUtil) appendLog(rf *Raft, args *AppendEntryArgs, reply *AppendEntryReply) {
	reply.Term = rf.term
	reply.Success = true

	// 追加日志
	newLogs := make([]*raftLog, len(args.Entries))
	copy(newLogs, args.Entries)
	rf.logs = append(rf.logs[:args.PreLogIndex+1], newLogs...)

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
