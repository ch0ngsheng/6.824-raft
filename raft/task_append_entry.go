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

type sendEntryUtil struct {
}

var seu = sendEntryUtil{}

func (sendEntryUtil) buildAppendEntryInfoByID(rf *Raft, who int) *appendEntryInfo {
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

func (sendEntryUtil) buildAppendEntryInfo(rf *Raft) map[int]*appendEntryInfo {
	m := make(map[int]*appendEntryInfo)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		m[i] = seu.buildAppendEntryInfoByID(rf, i)
	}
	return m
}

func (sendEntryUtil) prepareLocked(rf *Raft) (chan *appendEntryResult, uint64, uint32, int, int, bool) {
	rf.mu.Lock()

	if rf.role != raftLeader {
		rf.DPrintf("HB: <%d-%s>: (double check, not leader anymore)", rf.me, rf.getRole())
		// 有两种原因导致这种情况
		// 1. leader刚收到了高任期AppendEntry，刚刚变为follower（会重置定时器）
		// 2. leader刚收到了高任期RequestVote，刚刚变为follower，可能刚重置了定时器（投出票），也可能没有重置（没投票）
		// 为了防止出现该节点再无定时器触发的情况，这里要重置一次定时器
		rf.rstElectionTimer()
		rf.mu.Unlock()
		return nil, 0, 0, 0, 0, false
	}

	rf.DPrintf("[HB AE Begin]>>> term: %d, leader: %d/ae-%dth, commitIndex: %d, leader len(logs): %d",
		rf.term, rf.me, rf.appendEntryTimes, rf.commitIndex, len(rf.logs))

	rf.rstLeaderTimer()
	rf.appendEntryTimes += 1

	// mark leaderCommitIndex 和 日志追加信息要在同一次加锁中获取，防止中间有更新
	infoMap := seu.buildAppendEntryInfo(rf)
	oldTerm, peerLen, me, commitIndex, appendEntryTimes := rf.term, len(rf.peers), rf.me, rf.commitIndex, rf.appendEntryTimes

	rf.mu.Unlock()

	resultChan := make(chan *appendEntryResult, peerLen-1) // todo appendEntry退出后，chan的状态是什么？

	commonArg := &AppendEntryArgs{
		Term:              oldTerm,
		LeaderID:          me,
		LeaderCommitIndex: commitIndex,
	}
	seu.broadcastAsync(rf, me, peerLen, commonArg, infoMap, resultChan)

	return resultChan, oldTerm, appendEntryTimes, me, peerLen, true
}

func (sendEntryUtil) broadcastAsync(rf *Raft, me, peerLen int, commonArg *AppendEntryArgs,
	entryMap map[int]*appendEntryInfo, resultChan chan *appendEntryResult) {

	for i := 0; i < peerLen; i++ {
		if i == me {
			continue
		}

		go func(who int) {
			args := &AppendEntryArgs{
				Term:              commonArg.Term,
				LeaderID:          commonArg.LeaderID,
				PreLogIndex:       entryMap[who].preLogIndex,
				PreLogTerm:        entryMap[who].preLogTerm,
				Entries:           entryMap[who].entries,
				LeaderCommitIndex: commonArg.LeaderCommitIndex,
			}

			seu.appendEntryToNodeSync(rf, who, args, resultChan)
		}(i)
	}
}

func (sendEntryUtil) checkStateLocked(rf *Raft) bool {
	if rf.killed() {
		return false
	}

	rf.mu.Lock()
	if rf.role != raftLeader {
		rf.DPrintf("[HB AE] term: %d, me: %d/ae-%dth, NOT leader anymore", rf.term, rf.me, rf.appendEntryTimes)
		rf.mu.Unlock()
		return false
	}
	rf.mu.Unlock()

	return true
}

func (sendEntryUtil) appendEntryToNodeSync(rf *Raft, who int, args *AppendEntryArgs, resultChan chan *appendEntryResult) {
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

func (sendEntryUtil) checkResultLocked(rf *Raft, result *appendEntryResult, finishedMap map[int]bool,
	resultChan chan *appendEntryResult, oldTerm uint64, oldAppendTimes uint32) (goon bool) {

	// mark 收到响应时，节点状态可能发生了变化
	if rf.killed() {
		return false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != raftLeader {
		// mark 已不是leader，停止发送。防止因自己的日志回滚导致后面计算preLogIndex过大触发越界。
		rf.DPrintf("[HB AE RESP] term: %d, me: %d/ae-%dth, NOT leader anymore", rf.term, rf.me, rf.appendEntryTimes)
		return false
	}

	if rf.term > result.args.Term {
		// 收到了自己之前任期时发出的RPC响应，放弃处理这次响应。这次响应延迟太多了
		rf.DPrintf("[HB AE RESP] term: %d, leader: %d, receive old resp with term %d, IGNORE", rf.term, rf.me, result.args.Term)
		// 当前goroutine的channel收到的term是旧的，退出当前goroutine，交给新的goroutine处理
		return false
	}

	// 失败，网络不可达，本次忽略这个节点
	if !result.ok {
		finishedMap[result.fromWho] = true
		rf.DPrintf("[HB AE RESP] term: %d, leader: %d/ae-%dth, node %d network down", oldTerm, rf.me, oldAppendTimes, result.fromWho)
		return true
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
		return true
	}

	// 以下处理，返回的Success值为false的情况
	if rf.term < result.reply.Term {
		// 对端任期更高，转为follower
		rf.DPrintf("[HB AE RESP] term: %d, me: %d, receive higher term %d from %d, be follower", rf.term, rf.me, result.reply.Term, result.fromWho)
		rf.switchRole(raftFollower)
		rf.updateTermAndPersist(result.reply.Term)
		return false
	} else {
		// 当前任期与对端一致，但是没有成功，说明对端日志冲突，重试
		rf.updateNextIndex(result.fromWho, result.reply.XIndex, result.reply.XTerm, result.reply.XLen)
		rf.DPrintf("[HB AE RESP CONFLICT]me %d, from %d XIdx %d XTerm %d XLen %d, len(logs) %d newNextIndex %d",
			rf.me, result.fromWho, result.reply.XIndex, result.reply.XTerm, result.reply.XLen, len(rf.logs), rf.nextIndex[result.fromWho])
		entryInfo := seu.buildAppendEntryInfoByID(rf, result.fromWho)
		cmtIdxNow := rf.commitIndex
		termNow := rf.term

		go func(cmtIdx, term uint64, me int) {
			args := &AppendEntryArgs{
				Term:              term,
				LeaderID:          me,
				PreLogIndex:       entryInfo.preLogIndex,
				PreLogTerm:        entryInfo.preLogTerm,
				Entries:           entryInfo.entries,
				LeaderCommitIndex: cmtIdx,
			}
			seu.appendEntryToNodeSync(rf, result.fromWho, args, resultChan)
		}(cmtIdxNow, termNow, rf.me)
		return true
	}
}

// appendEntry 追加日志条目，由leader调用
func appendEntry(rf *Raft) {
	resultChan, oldTerm, oldAppendTimes, me, peerLen, ok := seu.prepareLocked(rf)
	if !ok {
		return
	}

	finishedMap := make(map[int]bool)
	ticker := time.NewTicker(time.Millisecond * 50)
	defer ticker.Stop()

	// mark 控制goroutine数量
	timeoutTimer := time.NewTimer(ElectionTimeout * 3)
	defer timeoutTimer.Stop()

	for {
		if len(finishedMap) == peerLen-1 {
			// 全部节点已处理完成
			return
		}

		var result *appendEntryResult
		select {
		case <-timeoutTimer.C:
			rf.DPrintf("leader: %d/ae-%dth with term %d, goroutine exit.", me, oldAppendTimes, oldTerm)
			return
		case <-ticker.C:
			if ok = seu.checkStateLocked(rf); !ok {
				return
			}
		case result = <-resultChan:
			if ok = seu.checkResultLocked(rf, result, finishedMap, resultChan, oldTerm, oldAppendTimes); !ok {
				return
			}
		}
	}
}
