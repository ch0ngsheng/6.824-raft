package raft

import (
	"fmt"
	"sync/atomic"
	"time"
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

type requestVoteResult struct {
	args    *RequestVoteArgs
	reply   *RequestVoteReply
	ok      bool
	fromWho int
}

type sendReqVoteUtil struct {
}

var srv = sendReqVoteUtil{}

func (sendReqVoteUtil) prepareLocked(rf *Raft) (*RequestVoteArgs, chan *requestVoteResult, chan struct{}, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 再次检查，是不是刚刚收到了来自leader的心跳
	now := time.Now().Unix()
	if rf.timerResetTime.Add(HeartbeatInterval/2).Unix() > now {
		rf.DPrintf("Ignore this timeout, because just receive HB from leader")
		return nil, nil, nil, false
	}

	// 成为candidate
	rf.requestVoteTimes += 1
	rf.switchRole(raftCandidate)
	rf.updateTermAndPersist(rf.term + 1)
	rf.updateVotedForAndPersist(int32(rf.me))
	rf.voteCounter.Reset()
	rf.DPrintf("HB: <%d-%s>: (to rise election) at term %d", rf.me, rf.getRole(), rf.term)

	// 构造投票参数
	lastIndex, lastTerm := rf.getLastLogNumber()
	request := &RequestVoteArgs{
		Term:         rf.term,
		CandidateID:  rf.me,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}
	rf.DPrintf("HB: <%d-%s>: rise vote at new term %d, number%d", rf.me, rf.getRole(), rf.term, rf.requestVoteTimes)

	rf.rstElectionTimer()

	resultChan := make(chan *requestVoteResult, len(rf.peers)-1)
	rf.electionStopChan = make(chan struct{}, 1)
	electionStopChan := rf.electionStopChan

	return request, resultChan, electionStopChan, true
}

func (sendReqVoteUtil) ReqVoteBroadcastAsync(rf *Raft, request *RequestVoteArgs, resultChan chan *requestVoteResult) {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(server, request, reply)
			result := &requestVoteResult{
				args:    request,
				reply:   reply,
				ok:      ok,
				fromWho: server,
			}
			resultChan <- result
		}(i)
	}
}

func (sendReqVoteUtil) checkRoleLocked(rf *Raft) (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		return voteResultKilled, false
	}

	if rf.role != raftCandidate {
		return voteResultNotCandidate, false
	}

	return -1, true
}

func (sendReqVoteUtil) countVotes(rf *Raft, res *requestVoteResult) {
	if !res.ok {
		// 节点响应超时
		rf.voteCounter.Deny(res.fromWho)
		rf.DPrintf("HB: <%d-%s>: term: %d, vote from %d, result: %s",
			rf.me, rf.getRole(), rf.term, res.fromWho, "node not ok")
		return
	}

	if res.reply.Voted {
		rf.voteCounter.Vote(res.fromWho)
		rf.DPrintf("HB: <%d-%s>: term: %d, vote resp from %d %s",
			rf.me, rf.getRole(), rf.term, res.fromWho, "voted")
		return
	}

	rf.voteCounter.Deny(res.fromWho)
	rf.DPrintf("HB: <%d-%s>: term: %d, vote resp from %d %s",
		rf.me, rf.getRole(), rf.term, res.fromWho, "deny")
}

func (sendReqVoteUtil) checkPeerRespLocked(rf *Raft,
	res *requestVoteResult) (int, bool) {

	if rf.killed() {
		return voteResultKilled, false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != raftCandidate {
		return voteResultNotCandidate, false
	}

	if res.args.Term < rf.term {
		// 来自之前发出的消息结果，网络是不可信的
		rf.DPrintf("HB: <%d-%s>: term %d, receive old RV resp from %d with old term %d, ignore",
			rf.me, rf.getRole(), rf.term, res.fromWho, res.args.Term)
		// 收到老term的消息，说明这个协程已经是老的了，直接退出
		return voteResultOutdated, false
	}

	if res.ok && res.reply.Term > atomic.LoadUint64(&rf.term) {
		rf.DPrintf("HB: <%d-%s>: term: %d, vote quit because higher term %d",
			rf.me, rf.getRole(), rf.term, res.reply.Term)

		rf.updateTermAndPersist(res.reply.Term)
		// 对方在高任期
		return voteResultFoundHigherTerm, false
	}

	// 计票
	srv.countVotes(rf, res)

	if rf.voteCounter.VoteNum() > len(rf.peers)/2 {
		rf.DPrintf("HB: <%d-%s>: term: %d, voted: %v, deny: %v result: %s",
			rf.me, rf.getRole(), rf.term, rf.voteCounter.Voted(), rf.voteCounter.Denied(), "win!")
		return voteResultWin, false
	}
	if rf.voteCounter.DenyNum() > len(rf.peers)/2 {
		rf.DPrintf("HB: <%d-%s>: term: %d, voted: %v, deny: %v result: %s",
			rf.me, rf.getRole(), rf.term, rf.voteCounter.Voted(), rf.voteCounter.Denied(), "lose!")
		return voteResultLose, false
	}

	// 继续等待
	return -1, true
}

func (sendReqVoteUtil) handleVoteResultLocked(rf *Raft, result int) heartbeatHandler {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	switch result {
	case voteResultKilled, voteResultNotCandidate, voteResultOutdated:
		return nil
	case voteResultFoundHigherTerm, voteResultLose:
		// mark 选举失败，不重置定时器
		rf.DPrintf("HB: <%d-%s>: vote end, be follower because of %d", rf.me, rf.getRole(), result)
		rf.switchRole(raftFollower)
		return nil
	case voteResultTimeout:
		// 超时重新选举
		rf.DPrintf("HB: <%d-%s>: vote timeout at number %d, try again", rf.me, rf.getRole(), rf.requestVoteTimes)
		rf.switchRole(raftFollower)

		return followerHandler
	case voteResultWin:
		// 变成leader，立即发送心跳
		rf.DPrintf("HB: <%d-%s>: vote win", rf.me, rf.getRole())
		rf.switchRole(raftLeader)

		return leaderHandler
	default:
		panic(fmt.Sprintf("HB: unknown vote result %v", result))
	}
}

// requestVote 发起投票，follower在心跳超时时执行
func requestVote(rf *Raft) {
	request, peerRespChan, electionStopChan, ok := srv.prepareLocked(rf)
	if !ok {
		return
	}

	// 发出投票请求
	srv.ReqVoteBroadcastAsync(rf, request, peerRespChan)

	ticker := time.NewTicker(time.Millisecond * 40)
	defer ticker.Stop()

	var goon = true
	var voteResult int

	// mark 从发出RPC，到接收到RPC响应，raft的状态可能发生了变化
	for goon {
		if voteResult, goon = srv.checkRoleLocked(rf); !goon {
			break
		}

		select {
		case <-electionStopChan:
			// 选举超时
			voteResult = voteResultTimeout
			goon = false
		case <-ticker.C:
			voteResult, goon = srv.checkRoleLocked(rf)
		case resp := <-peerRespChan:
			voteResult, goon = srv.checkPeerRespLocked(rf, resp)
		}
	}

	if handler := srv.handleVoteResultLocked(rf, voteResult); handler != nil {
		handler(rf)
	}
}
