package raft

import (
	"sync/atomic"
	"time"
)

const (
	heartbeatInterval       = time.Millisecond * 100
	heartbeatIntervalFloat  = 20
	applierInterval         = time.Millisecond * 100
	requestVoteTimeout      = heartbeatInterval * 2
	requestVoteTimeoutFloat = 20
)

const (
	// 节点被kill
	voteResultKilled = iota + 1
	// 选举被停止
	voteResultStopped
	// 选举超时
	voteResultTimeout
	// 选举获胜
	voteResultWin
	// 选举失败
	voteResultLose
	// 发现更高term，变为follower
	voteResultBeFollower
)

// heatbeat 监控心跳事件
func heatbeat(rf *Raft) {

	timer := rf.appendEntryTimer
	du := rf.appendEntryTimerDuration
	timer.Reset(du)
	rf.DPrintf("<RST AE Timer><%d-%s> val: %v", rf.me, rf.getRole(), du)
	rf.DPrintf("<%d-%s>: heartbeat monitor start", rf.me, rf.getRole())

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
			// todo 如果是leader，把心跳间隔调小一点

			du = rf.appendEntryTimerDuration
			timer.Reset(du)
			rf.DPrintf("<RST AE Timer><%d-%s> val: %v", rf.me, rf.getRole(), du)
			role := rf.role
			rf.mu.Unlock()

			go heartbeatHandlerMap[role](rf)
		}
	}
}

type requestVoteResult struct {
	*RequestVoteReply
	ok      bool
	fromWho int
}

// requestVote 发起投票，follower在心跳超时时执行
func requestVote(rf *Raft, returnChan chan int) {
	rf.mu.Lock()
	rf.role = raftCandidate
	rf.updateTerm(rf.term + 1)
	rf.updateVotedFor(int32(rf.me))
	rf.votingStopChan = make(chan struct{}, 1)

	lastIndex, lastTerm := rf.getLastLogNumber()
	request := &RequestVoteArgs{
		Term:         rf.term,
		CandidateID:  rf.me,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}
	rf.DPrintf("HB: <%d-%s>: rise vote at new term %d", rf.me, rf.getRole(), rf.term)

	resultChan := make(chan requestVoteResult, len(rf.peers)-1)

	du := getRandomDuration(requestVoteTimeout, rf.me)
	rf.requestVoteTimer.Reset(du)
	rf.DPrintf("<RST RV Timer><%d-%s> val: %v", rf.me, rf.getRole(), du)
	defer rf.requestVoteTimer.Stop()
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(server, request, reply)
			result := requestVoteResult{
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

	// mark 从发出RPC，到接收到RPC响应，raft的状态可能发生了变化
	for {
		rfc := rf.getInfoLocked()

		if rf.killed() {
			rf.DPrintf("<%d-%s>: found killed when request vote %p", rfc.me, rfc.getRole(), rf)
			returnChan <- voteResultKilled
			return
		}

		select {
		case <-rf.requestVoteTimer.C:
			// 选举超时
			returnChan <- voteResultTimeout
			return
		case <-rf.votingStopChan:
			// 选举被中止，可能由于收到了来自leader的有效心跳
			returnChan <- voteResultStopped
			return
		case res := <-resultChan:
			rfc = rf.getInfoLocked()
			if rfc.role == raftFollower {
				return
			}

			if !res.ok {
				// 节点响应超时
				denyNum += 1
				rf.DPrintf("HB: <%d-%s>: term: %d, vote from %d, result: %s",
					rf.me, rfc.getRole(), rfc.term, res.fromWho, "node not ok")

				continue
			}
			if res.Term > atomic.LoadUint64(&rf.term) {
				// 发现更高term，变为follower
				rf.DPrintf("HB: <%d-%s>: term: %d, vote quit because higher term %d",
					rfc.me, rfc.getRole(), rfc.term, res.Term)

				returnChan <- voteResultBeFollower
				return
			}
			if res.Voted {
				votedNum += 1
				votedNodes = append(votedNodes, res.fromWho)
				rf.DPrintf("HB: <%d-%s>: term: %d, vote resp from %d %s",
					rfc.me, rfc.getRole(), rfc.term, res.fromWho, "voted")
			} else {
				denyNum += 1
				denyNodes = append(denyNodes, res.fromWho)
				rf.DPrintf("HB: <%d-%s>: term: %d, vote resp from %d %s",
					rfc.me, rfc.getRole(), rfc.term, res.fromWho, "deny")
			}
			if votedNum > len(rf.peers)/2 {
				// 当选
				rf.DPrintf("HB: <%d-%s>: term: %d, voted: %v, deny: %v result: %s",
					rfc.me, rfc.getRole(), rfc.term, votedNodes, denyNodes, "win!")

				returnChan <- voteResultWin
				return
			}
			if denyNum > len(rf.peers)/2 {
				// 未当选
				rf.DPrintf("HB: <%d-%s>: term: %d, voted: %v, deny: %v result: %s",
					rfc.me, rfc.getRole(), rfc.term, votedNodes, denyNodes, "lose!")

				returnChan <- voteResultLose
				return
			}
		}
	}
}

// appendEntry 追加日志条目
func appendEntry(rf *Raft) {
	rfc := rf.getInfoLocked()
	originTerm, originCommitIndex, role := rfc.term, rfc.commitIndex, rfc.role
	if !(role == raftLeader) {
		return
	}

	atomic.StoreUint32(&rf.appendEntryTimes, atomic.LoadUint32(&rf.appendEntryTimes)+1)

	rf.appendEntryStopChan = make(chan struct{}, 1)

	rf.DPrintf("[HB AE Begin]>>> term: %d, leader: %d/ae-%dth, commitIndex: %d", originTerm, rfc.me, rfc.appendEntryTimes, originCommitIndex)

	infoMap := rf.buildAppendEntryInfoLocked()
	resultChan := make(chan *appendEntryResult, len(rf.peers)-1)
	finishedMap := make(map[int]bool)

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go appendEntryToNode(rf, i, infoMap[i], resultChan)
	}

	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()

	for {
		if len(finishedMap) == len(rf.peers)-1 {
			// 全部节点已处理完成
			break
		}

		var result *appendEntryResult
		select {
		case <-ticker.C:
			if rf.killed() {
				return
			}
			continue
		case result = <-resultChan:
			break // 退出select clause
		case <-rf.appendEntryStopChan:
			// 停止发送
			return
		}
		if rf.killed() {
			return
		}

		// mark 收到响应时，节点状态可能发生了变化

		if atomic.LoadUint32(&rf.role) != raftLeader {
			// mark 已不是leader，停止发送。防止因自己的日志回滚导致计算preLogIndex过大触发越界。
			return
		}

		rfc = rf.getInfoLocked()
		// 失败，网络不可达，本次忽略这个节点
		if !result.ok {
			finishedMap[result.fromWho] = true
			rf.DPrintf("[HB AE] term: %d, leader: %d/ae-%dth, node %d network down", originTerm, rfc.me, rfc.appendEntryTimes, result.fromWho)
			continue
		}

		// 成功，更新对应节点的nextIndex[i]和matchIndex[i]
		if result.reply.Success {
			matchIndex := result.args.PreLogIndex + uint64(len(result.args.Entries))
			rf.updateMatchIndexLocked(result.fromWho, matchIndex)
			finishedMap[result.fromWho] = true

			// mark 注意，更新commitIndex不应该等到所有节点都响应之后才做
			// 不然会导致集群一直不一致，因为节点故障后一直不会响应。
			// 根据matchIndex[i]更新自己的commitIndex
			rf.updateCommitIndexLocked()
			rf.DPrintf("HB AE: leader: %d/ae-%dth, node %d reply success, now commitIndex is %d/%d", rfc.me, rfc.appendEntryTimes, result.fromWho, rfc.commitIndex, len(rfc.logs))

			continue
		}

		// 以下处理，返回的Success值为false的情况
		rfc = rf.getInfoLocked()
		curTerm := rfc.term // 重新获取当前任期
		if curTerm > result.args.Term {
			// 收到了自己之前任期时发出的RPC响应，放弃处理这次响应。这次响应延迟太多了
			finishedMap[result.fromWho] = true
			continue
		}
		if curTerm < result.reply.Term {
			// 对端任期更高，转为follower
			rf.mu.Lock()
			rf.updateTerm(result.reply.Term)
			rf.switchRole(raftFollower)
			rf.mu.Unlock()
			return
		} else {
			// 当前任期与对端一致，但是没有成功，说明对端日志冲突，重试
			rf.updateNextIndexLocked(result.fromWho, result.reply.XIndex, result.reply.XTerm, result.reply.XLen)

			var entryInfo *appendEntryInfo
			rf.mu.Lock()
			entryInfo = rf.buildAppendEntryInfoByID(result.fromWho)
			rf.mu.Unlock()

			go appendEntryToNode(rf, result.fromWho, entryInfo, resultChan)
		}

	}

	// 改为每成功一次，就做一次
	// 根据matchIndex[i]更新自己的commitIndex
	// rf.updateCommitIndexLocked()

	rfc = rf.getInfoLocked()
	rf.DPrintf(">>>[HB AE End] originTerm: %d, me: %d/ae-%dth, isLeader %v, endTerm: %d, endCommitIndex: %d, nodes: %v",
		originTerm, rfc.me, rfc.appendEntryTimes, rfc.role == raftLeader, rfc.term, rfc.commitIndex, finishedMap)
}

type appendEntryResult struct {
	args    *AppendEntryArgs
	reply   *AppendEntryReply
	fromWho int
	ok      bool
}

func appendEntryToNode(rf *Raft, who int, entryInfo *appendEntryInfo, resultChan chan *appendEntryResult) {
	rfc := rf.getInfoLocked()
	term, commitIndex := rfc.term, rfc.commitIndex

	args := &AppendEntryArgs{
		Term:              term,
		LeaderID:          rf.me,
		PreLogIndex:       entryInfo.preLogIndex,
		PreLogTerm:        entryInfo.preLogTerm,
		Entries:           entryInfo.entries,
		LeaderCommitIndex: commitIndex,
	}
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
	rfc := rf.getInfoLocked()
	rf.DPrintf("<%d-%s>: applier start", rfc.me, rfc.getRole())

	ticker := time.NewTicker(applierInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rf.mu.Lock()

			if rf.killed() {
				rf.mu.Unlock()
				rf.DPrintf("<%d-%s>: applier exit because node is killed %p", rf.me, rf.getRole(), rf)
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
