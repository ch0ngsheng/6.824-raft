package raft

import (
	"sync/atomic"
	"time"
)

const (
	heartbeatInterval  = time.Millisecond * 100
	requestVoteTimeout = time.Millisecond * 200
	applierInterval    = time.Millisecond * 100
)

const (
	// 节点被kill
	voteResultKilled = iota + 1
	// 选举响应过期，收到响应时自己的任期变高了
	voteResultOutdated
	// 选举期间发现自己不再是candidate
	voteResultNotCandidate
	// 选举超时
	voteResultTimeout
	// 选举获胜
	voteResultWin
	// 选举失败
	voteResultLose
	// 发现更高term，变为follower
	voteResultFoundHigherTerm
)

// heatbeat 监控心跳事件
func heatbeat(rf *Raft) {
	rf.mu.Lock()

	timer := rf.timer
	rf.resetTimer(applierInterval)

	rf.mu.Unlock()

	for {
		select {
		case <-timer.C:
			// 角色可能发生了变化
			rf.mu.Lock()

			if rf.killed() {
				rf.DPrintf("<%d-%s>: find killed when heartbeat %p", rf.me, rf.getRole(), rf)
				rf.mu.Unlock()
				return
			}

			handler := heartbeatHandlerMap[rf.role]
			rf.mu.Unlock()

			go handler(rf)
		}
	}
}

type requestVoteResult struct {
	args *RequestVoteArgs
	*RequestVoteReply
	ok      bool
	fromWho int
}

// requestVote 发起投票，follower在心跳超时时执行
func requestVote(rf *Raft, returnChan chan int) {
	rf.mu.Lock()

	lastIndex, lastTerm := rf.getLastLogNumber()
	request := &RequestVoteArgs{
		Term:         rf.term,
		CandidateID:  rf.me,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}
	rf.DPrintf("HB: <%d-%s>: rise vote at new term %d, number%d", rf.me, rf.getRole(), rf.term, rf.requestVoteTimes)

	resultChan := make(chan requestVoteResult, len(rf.peers)-1)

	du := getRandomDuration(requestVoteTimeout, rf.me)
	requestVoteTimer := time.NewTimer(du)
	defer requestVoteTimer.Stop() // mark 防止.C接收错乱 todo .C没有接收会怎样？事件会一直保留吗

	rf.resetTimer(applierInterval)
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(server, request, reply)
			result := requestVoteResult{
				args:             request,
				RequestVoteReply: reply,
				ok:               ok,
				fromWho:          server,
			}
			resultChan <- result
		}(i)
	}

	votedNum := 1 // 赞同票，包含自己一票
	denyNum := 0  // 反对票
	votedNodes := []int{rf.me}
	denyNodes := make([]int, 0)

	ticker := time.NewTicker(time.Millisecond * 50)
	defer ticker.Stop()

	// mark 从发出RPC，到接收到RPC响应，raft的状态可能发生了变化
	for {
		if rf.killed() {
			returnChan <- voteResultKilled
			return
		}

		select {
		case <-requestVoteTimer.C:
			// 选举超时
			returnChan <- voteResultTimeout
			return
		case <-ticker.C:
			rf.mu.Lock()
			if rf.role != raftCandidate {
				rf.mu.Unlock()
				returnChan <- voteResultNotCandidate
				return
			}
			rf.mu.Unlock()
		case res := <-resultChan:
			rf.mu.Lock()
			if rf.role != raftCandidate {
				rf.mu.Unlock()
				returnChan <- voteResultNotCandidate
				return
			}

			if res.args.Term < rf.term {
				// 来自之前发出的消息结果，网络是不可信的
				rf.DPrintf("HB: <%d-%s>: term %d, receive old RV resp from %d with old term %d, ignore",
					rf.me, rf.getRole(), rf.term, res.fromWho, res.args.Term)
				rf.mu.Unlock()
				//continue
				// 这个协程已经是老的了，直接退出
				returnChan <- voteResultOutdated
				return
			}

			if !res.ok {
				// 节点响应超时
				denyNum += 1
				denyNodes = append(denyNodes, res.fromWho)
				rf.DPrintf("HB: <%d-%s>: term: %d, vote from %d, result: %s",
					rf.me, rf.getRole(), rf.term, res.fromWho, "node not ok")
			} else {
				if res.Term > atomic.LoadUint64(&rf.term) {
					rf.DPrintf("HB: <%d-%s>: term: %d, vote quit because higher term %d",
						rf.me, rf.getRole(), rf.term, res.Term)
					rf.updateTermAndPersist(res.Term) // mark 收到高term响应
					rf.mu.Unlock()
					returnChan <- voteResultFoundHigherTerm
					return
				}
				if res.Voted {
					votedNum += 1
					votedNodes = append(votedNodes, res.fromWho)
					rf.DPrintf("HB: <%d-%s>: term: %d, vote resp from %d %s",
						rf.me, rf.getRole(), rf.term, res.fromWho, "voted")
				} else {
					denyNum += 1
					denyNodes = append(denyNodes, res.fromWho)
					rf.DPrintf("HB: <%d-%s>: term: %d, vote resp from %d %s",
						rf.me, rf.getRole(), rf.term, res.fromWho, "deny")
				}
			}

			if votedNum > len(rf.peers)/2 {
				// 当选
				rf.DPrintf("HB: <%d-%s>: term: %d, voted: %v, deny: %v result: %s",
					rf.me, rf.getRole(), rf.term, votedNodes, denyNodes, "win!")
				rf.mu.Unlock()
				returnChan <- voteResultWin
				return
			}
			if denyNum > len(rf.peers)/2 {
				// 未当选
				rf.DPrintf("HB: <%d-%s>: term: %d, voted: %v, deny: %v result: %s",
					rf.me, rf.getRole(), rf.term, votedNodes, denyNodes, "lose!")
				rf.mu.Unlock()
				returnChan <- voteResultLose
				return
			}
			rf.mu.Unlock()
		}
	}
}

// appendEntry 追加日志条目
func appendEntry(rf *Raft) {
	rf.mu.Lock()

	if !(rf.role == raftLeader) {
		rf.mu.Unlock()
		return
	}

	oldTerm := rf.term
	rf.appendEntryTimes += 1

	rf.DPrintf("[HB AE Begin]>>> term: %d, leader: %d/ae-%dth, commitIndex: %d, leader len(logs): %d", rf.term, rf.me, rf.appendEntryTimes, rf.commitIndex, len(rf.logs))

	// mark leaderCommitIndex 和 日志追加信息要在一次锁中获取，防止中间有更新
	infoMap := rf.buildAppendEntryInfo()
	peerLen, me, commitIndex := len(rf.peers), rf.me, rf.commitIndex
	rf.mu.Unlock()

	resultChan := make(chan *appendEntryResult, peerLen-1)
	finishedMap := make(map[int]bool)

	for i := 0; i < peerLen; i++ {
		if i == me {
			continue
		}

		go func(no int, term uint64) {
			args := &AppendEntryArgs{
				Term:              term,
				LeaderID:          me,
				PreLogIndex:       infoMap[no].preLogIndex,
				PreLogTerm:        infoMap[no].preLogTerm,
				Entries:           infoMap[no].entries,
				LeaderCommitIndex: commitIndex,
			}
			appendEntryToNodeLocked(rf, no, args, resultChan)
		}(i, oldTerm)
	}

	ticker := time.NewTicker(time.Millisecond * 50)
	defer ticker.Stop()

	for {
		if len(finishedMap) == peerLen-1 {
			// 全部节点已处理完成
			break
		}

		var result *appendEntryResult
		select {
		case <-ticker.C:
			if rf.killed() {
				return
			}
			rf.mu.Lock()
			if rf.role != raftLeader {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			continue
		case result = <-resultChan:
			break // 退出select clause
		}
		if rf.killed() {
			return
		}

		// mark 收到响应时，节点状态可能发生了变化

		rf.mu.Lock()
		if rf.role != raftLeader {
			// mark 已不是leader，停止发送。防止因自己的日志回滚导致后面计算preLogIndex过大触发越界。
			rf.DPrintf("[HB AE RESP] term: %d, me: %d/ae-%dth, NOT leader anymore", rf.term, rf.me, rf.appendEntryTimes)
			rf.mu.Unlock()
			return
		}

		if rf.term > result.args.Term {
			// 收到了自己之前任期时发出的RPC响应，放弃处理这次响应。这次响应延迟太多了
			// finishedMap[result.fromWho] = true
			rf.DPrintf("[HB AE RESP] term: %d, leader: %d, receive old resp with term %d, IGNORE", rf.term, rf.me, result.args.Term)
			rf.mu.Unlock()
			//continue
			// 交给新的goroutine处理
			return
		}

		// 失败，网络不可达，本次忽略这个节点
		if !result.ok {
			finishedMap[result.fromWho] = true
			rf.DPrintf("[HB AE RESP] term: %d, leader: %d/ae-%dth, node %d network down", oldTerm, rf.me, rf.appendEntryTimes, result.fromWho)
			rf.mu.Unlock()
			continue
		}

		// 成功，更新对应节点的nextIndex[i]和matchIndex[i]
		if result.reply.Success {

			matchIndex := result.args.PreLogIndex + uint64(len(result.args.Entries))
			rf.updateMatchIndex(result.fromWho, matchIndex)
			finishedMap[result.fromWho] = true

			// mark 注意，更新commitIndex不应该等到所有节点都响应之后才做
			// 不然会导致集群一直不一致，因为节点故障后一直不会响应。
			// 根据matchIndex[i]更新自己的commitIndex
			rf.updateCommitIndex()
			rf.DPrintf("[HB AE RESP] leader: %d/ae-%dth, node %d reply success, now commitIndex is %d/%d，matchIndex: %v", rf.me, rf.appendEntryTimes, result.fromWho, rf.commitIndex, len(rf.logs), rf.matchIndex)
			rf.mu.Unlock()
			continue
		}

		// 以下处理，返回的Success值为false的情况
		if rf.term < result.reply.Term {
			// 对端任期更高，转为follower
			rf.DPrintf("[HB AE RESP] term: %d, me: %d, receive higher term %d from %d, be follower", rf.term, rf.me, result.reply.Term, result.fromWho)
			rf.switchRole(raftFollower)
			rf.updateTermAndPersist(result.reply.Term)
			rf.mu.Unlock()
			return
		} else {
			// 当前任期与对端一致，但是没有成功，说明对端日志冲突，重试
			rf.updateNextIndex(result.fromWho, result.reply.XIndex, result.reply.XTerm, result.reply.XLen)
			rf.DPrintf("[HB AE RESP CONFLICT]me %d, from %d XIdx %d XTerm %d XLen %d, len(logs) %d newNextIndex %d",
				rf.me, result.fromWho, result.reply.XIndex, result.reply.XTerm, result.reply.XLen, len(rf.logs), rf.nextIndex[result.fromWho])
			entryInfo := rf.buildAppendEntryInfoByID(result.fromWho)
			cmtIdxNow := rf.commitIndex
			termNow := rf.term
			rf.mu.Unlock()

			go func(cmtIdx, term uint64) {
				args := &AppendEntryArgs{
					Term:              term,
					LeaderID:          me,
					PreLogIndex:       entryInfo.preLogIndex,
					PreLogTerm:        entryInfo.preLogTerm,
					Entries:           entryInfo.entries,
					LeaderCommitIndex: cmtIdx,
				}
				appendEntryToNodeLocked(rf, result.fromWho, args, resultChan)
			}(cmtIdxNow, termNow)
		}
	}
}

type appendEntryResult struct {
	args    *AppendEntryArgs
	reply   *AppendEntryReply
	fromWho int
	ok      bool
}

func appendEntryToNodeLocked(rf *Raft, who int, args *AppendEntryArgs, resultChan chan *appendEntryResult) {
	reply := &AppendEntryReply{}
	// have a delay
	ok := rf.sendAppendEntry(who, args, reply)

	result := &appendEntryResult{
		fromWho: who,
		args:    args,
		reply:   reply,
		ok:      ok,
	}
	resultChan <- result
}

// applier 应用已提交的日志条目
func applier(rf *Raft) {
	rf.mu.Lock()
	rf.DPrintf("<%d-%s>: applier start", rf.me, rf.getRole())
	rf.mu.Unlock()

	ticker := time.NewTicker(applierInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rf.mu.Lock()

			if rf.killed() {
				rf.DPrintf("<%d-%s>: applier exit because node is killed %p", rf.me, rf.getRole(), rf)
				rf.mu.Unlock()
				return
			}

			lastApplied, commitIndex := rf.lastApplied, rf.commitIndex
			rf.DPrintf("APPLY <%d-%s>: apply status: lastApplied %d, commitIndex %d", rf.me, rf.getRole(), lastApplied, commitIndex)
			logs := make([]*raftLog, commitIndex-lastApplied)
			copy(logs, rf.logs[lastApplied+1:commitIndex+1])

			if len(logs) == 0 {
				rf.mu.Unlock()
				continue
			}

			rf.DPrintf("<%d-%s>: applying %d logs", rf.me, rf.getRole(), len(logs))

			for i := 0; i < len(logs); i++ {
				idx := int(lastApplied+1) + i
				msg := ApplyMsg{
					CommandValid: true,
					Command:      logs[i].Entry,
					CommandIndex: idx,
				}
				rf.applyMsgChan <- msg
				rf.lastApplied = uint64(idx)
			}
			rf.mu.Unlock()
		}
	}
}
