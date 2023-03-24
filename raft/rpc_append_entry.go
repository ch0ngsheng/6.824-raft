package raft

// AppendEntryArgs AppendEntry RPC 的请求参数
type AppendEntryArgs struct {
	Term              TermType
	LeaderID          int
	PreLogIndex       uint64
	PreLogTerm        TermType
	Entries           []*Log
	LeaderCommitIndex uint64
}

// AppendEntryReply AppendEntry RPC 的响应参数
type AppendEntryReply struct {
	Term    TermType
	Success bool
	// 冲突回退参数
	XIndex uint64
	XTerm  TermType
	XLen   uint64
}
type AppendEntryUtil struct {
}

var appendEntryUtil = AppendEntryUtil{}

// AppendEntry 处理AppendEntry RPC
func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("Receive AE>>>   me: %d, from L: %d, term: %d, preLogIdx:%d, preLogTerm:%d, leaderCommitIdx:%d, logs: %s",
		rf.me, args.LeaderID, args.Term, args.PreLogIndex, args.PreLogTerm, args.LeaderCommitIndex, Logs(args.Entries),
	)
	defer func() {
		DPrintf(">>>Receive AE   me: %d, from L: %d, resp: %v, commitIndex %d, final logs len %d, logs: %v",
			rf.me, args.LeaderID, reply, rf.commitIndex, len(rf.logs), Logs(rf.logs),
		)
	}()

	if ok := appendEntryUtil.checkTerm(rf, args, reply); !ok {
		return
	}

	// 身份验证通过后才能重置定时器
	rf.resetElectionTimer()
	rf.switchToFollowerAndPersist(args.Term)

	if can := appendEntryUtil.canAppend(rf, args, reply); !can {
		return
	}

	appendEntryUtil.appendLog(rf, args, reply)
}

func (AppendEntryUtil) checkTerm(rf *Raft, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	if args.Term < rf.term {
		reply.Success = false
		reply.Term = rf.term
		return false
	}
	return true
}

func (AppendEntryUtil) canAppend(rf *Raft, args *AppendEntryArgs, reply *AppendEntryReply) (can bool) {
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
		reply.XIndex = rf.FirstIndexPerTerm[reply.XTerm]
		reply.XLen = 0

		return false
	}
	return true
}

func (AppendEntryUtil) appendLog(rf *Raft, args *AppendEntryArgs, reply *AppendEntryReply) {
	reply.Term = rf.term
	reply.Success = true

	// 追加日志
	newLogs := make([]*Log, len(args.Entries))
	copy(newLogs, args.Entries)
	rf.logs = append(rf.logs[:args.PreLogIndex+1], newLogs...)

	rf.persist()

	// 如果leader的commitIndex更小，则不修改。否则可能导致本节点日志重复应用
	// commitIndex由leader主动更新
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
