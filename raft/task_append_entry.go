package raft

import (
	"fmt"
	"time"
)

type appendEntryInfo struct {
	preLogIndex uint64
	preLogTerm  uint64
	entries     []*raftLog
}

type appendEntryResult struct {
	args    *AppendEntryArgs
	reply   *AppendEntryReply
	fromWho int
	ok      bool
}

func (rf *Raft) buildAppendEntryInfoByID(who int) *appendEntryInfo {
	preLogIndex := rf.nextIndex[who] - 1
	defer func() {
		// used to debug
		if err := recover(); err != nil {
			fmt.Println(err, who, rf.me, rf.role, preLogIndex, len(rf.logs), rf.nextIndex)
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

// appendEntry 追加日志条目
func appendEntry(rf *Raft) {
	rf.mu.Lock()

	if !(rf.role == raftLeader) {
		rf.DPrintf("HB: <%d-%s>: (double check, not leader anymore)")
		rf.mu.Unlock()
		return
	}
	rf.DPrintf("HB: <%d-%s>: (to append entry)", rf.me, rf.getRole())
	rf.rstLeaderTimer()

	oldTerm := rf.term
	rf.appendEntryTimes += 1

	rf.DPrintf("[HB AE Begin]>>> term: %d, leader: %d/ae-%dth, commitIndex: %d, leader len(logs): %d", rf.term, rf.me, rf.appendEntryTimes, rf.commitIndex, len(rf.logs))

	// mark leaderCommitIndex 和 日志追加信息要在同一次加锁中获取，防止中间有更新
	infoMap := rf.buildAppendEntryInfo()
	peerLen, me, commitIndex, appendEntryTimes := len(rf.peers), rf.me, rf.commitIndex, rf.appendEntryTimes
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
			appendEntryToNode(rf, no, args, resultChan) // todo appendEntry退出后，chan的状态是什么？
		}(i, oldTerm)
	}

	ticker := time.NewTicker(time.Millisecond * 50)
	defer ticker.Stop()

	// mark 控制goroutine数量
	timeoutTimer := time.NewTimer(ElectionTimeout * 3)
	defer timeoutTimer.Stop()

	for {
		if len(finishedMap) == peerLen-1 {
			// 全部节点已处理完成
			break
		}

		var result *appendEntryResult
		select {
		case <-timeoutTimer.C:
			rf.DPrintf("leader: %d/ae-%dth with term %d, goroutine exit.", me, appendEntryTimes, oldTerm)
			return
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
				appendEntryToNode(rf, result.fromWho, args, resultChan)
			}(cmtIdxNow, termNow)
		}
	}
}

func appendEntryToNode(rf *Raft, who int, args *AppendEntryArgs, resultChan chan *appendEntryResult) {
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
