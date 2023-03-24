package raft

import (
	"fmt"
	"time"
)

const (
	// 节点被kill
	voteResultKilled = iota + 1
	// 选举响应过期，收到响应时自己的任期变高了
	// voteResultOutdated
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
	args      *RequestVoteArgs
	reply     *RequestVoteReply
	networkOk bool
	fromWho   int
}

type RequestVoteUtil struct {
}

var requestVoteUtil = RequestVoteUtil{}

// requestVote 发起投票，follower在心跳超时时执行一次
func requestVote(rf *Raft) {
	request, receiveResponseChan, ok := requestVoteUtil.PrepareLocked(rf)
	if !ok {
		return
	}

	requestVoteUtil.ReqVoteBroadcastAsync(rf, request, receiveResponseChan)

	ticker := time.NewTicker(time.Millisecond * 40)
	defer ticker.Stop()

	var goon = true
	var voteResult int

	// mark 从发出RPC，到接收到RPC响应，raft的状态可能发生了变化
	for goon {
		if voteResult, goon = requestVoteUtil.checkIsCandidateLocked(rf); !goon {
			break
		}

		select {
		case <-rf.electionTimeoutSignal:
			// 选举超时
			voteResult = voteResultTimeout
			goon = false
		case <-ticker.C:
			voteResult, goon = requestVoteUtil.checkIsCandidateLocked(rf)
		case resp := <-receiveResponseChan:
			voteResult, goon = requestVoteUtil.checkVoteResponseLocked(rf, resp)
		}
	}

	if handler := requestVoteUtil.handleVoteResultLocked(rf, voteResult); handler != nil {
		handler(rf)
	}
}

// PrepareLocked 发起投票前的数据结构准备
func (RequestVoteUtil) PrepareLocked(rf *Raft) (*RequestVoteArgs, chan *requestVoteResult, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Follower {
		return nil, nil, false
	}

	rf.switchToCandidateAndPersist()
	args := requestVoteUtil.buildVoteArgs(rf)
	receiveResponseChan := make(chan *requestVoteResult, len(rf.peers)-1)
	rf.resetElectionTimer()

	DPrintf("HB: <%d-%s>: rise vote at new term %d, %d times.", rf.me, rf.role, rf.term, rf.requestVoteTimes)
	return args, receiveResponseChan, true
}

// ReqVoteBroadcastAsync 异步发出投票请求
func (RequestVoteUtil) ReqVoteBroadcastAsync(rf *Raft, args *RequestVoteArgs, resultChan chan *requestVoteResult) {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		// 每个节点使用独立的goroutine发出，防止某个节点延迟较长
		go func(id int) {
			reply := &RequestVoteReply{}

			// 测试框架保证方法会返回，status表示节点是否正常响应
			status := rf.sendRequestVote(id, args, reply)
			result := &requestVoteResult{
				args:      args,
				reply:     reply,
				networkOk: status,
				fromWho:   id,
			}
			resultChan <- result
		}(i)
	}
}

func (RequestVoteUtil) buildVoteArgs(rf *Raft) *RequestVoteArgs {
	lastIndex, lastTerm := rf.getLastLogNumber()
	request := &RequestVoteArgs{
		Term:         rf.term,
		CandidateID:  rf.me,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}
	return request
}

func (RequestVoteUtil) checkIsCandidateLocked(rf *Raft) (int, bool) {
	if rf.killed() {
		return voteResultKilled, false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Candidate {
		return voteResultNotCandidate, false
	}

	return -1, true
}

func (RequestVoteUtil) countVotes(rf *Raft, res *requestVoteResult) {
	if !res.networkOk {
		// 节点响应超时
		rf.voteCounter.DenyFrom(res.fromWho)
		DPrintf("HB: <%d-%s>: term: %d, vote from %d, result: %s",
			rf.me, rf.role, rf.term, res.fromWho, "node not ok")
		return
	}

	if res.reply.Voted {
		rf.voteCounter.AcceptFrom(res.fromWho)
		DPrintf("HB: <%d-%s>: term: %d, vote resp from %d %s",
			rf.me, rf.role, rf.term, res.fromWho, "accepted")
		return
	}

	rf.voteCounter.DenyFrom(res.fromWho)
	DPrintf("HB: <%d-%s>: term: %d, vote resp from %d %s",
		rf.me, rf.role, rf.term, res.fromWho, "deny")
}

func (RequestVoteUtil) checkVoteResponseLocked(rf *Raft, res *requestVoteResult) (int, bool) {
	if rf.killed() {
		return voteResultKilled, false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Candidate {
		return voteResultNotCandidate, false
	}

	if res.args.Term < rf.term {
		// 来自之前发出的消息结果，网络是不可信的
		DPrintf("HB: <%d-%s>: term %d, receive old RV resp from %d with old term %d, ignore",
			rf.me, rf.role, rf.term, res.fromWho, res.args.Term)
		// 收到老term的消息，忽略
		return -1, true
	}

	if res.networkOk && res.reply.Term > rf.term {
		DPrintf("HB: <%d-%s>: term: %d, vote quit because higher term %d",
			rf.me, rf.role, rf.term, res.reply.Term)

		rf.switchToFollowerAndPersist(res.reply.Term)
		// 对方在高任期
		return voteResultFoundHigherTerm, false
	}

	// 计票
	requestVoteUtil.countVotes(rf, res)

	if rf.voteCounter.TotalAccepted() > len(rf.peers)/2 {
		DPrintf("HB: <%d-%s>: term: %d, accepted: %v, deny: %v result: %s",
			rf.me, rf.role, rf.term, rf.voteCounter.AcceptedList(), rf.voteCounter.DeniedList(), "win!")
		// 当选
		return voteResultWin, false
	}
	if rf.voteCounter.TotalDenied() > len(rf.peers)/2 {
		DPrintf("HB: <%d-%s>: term: %d, accepted: %v, deny: %v result: %s",
			rf.me, rf.role, rf.term, rf.voteCounter.AcceptedList(), rf.voteCounter.DeniedList(), "lose!")
		rf.switchToFollowerAndPersist(rf.term)
		// 未当选
		return voteResultLose, false
	}

	// 继续等待
	return -1, true
}

func (RequestVoteUtil) handleVoteResultLocked(rf *Raft, result int) heartbeatHandler {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	switch result {
	case voteResultFoundHigherTerm, voteResultLose, voteResultKilled, voteResultNotCandidate:
		// mark 选举失败，不重置定时器，因为期间应该能收到leader的心跳
		DPrintf("HB: <%d-%s>: vote end, be follower because of %d", rf.me, rf.role, result)
		return nil
	case voteResultTimeout:
		// 超时重新选举
		DPrintf("HB: <%d-%s>: vote timeout at number %d, try again", rf.me, rf.role, rf.requestVoteTimes)
		rf.switchToFollowerAndPersist(rf.term)
		return followerHandler
	case voteResultWin:
		// 变成leader，立即发送心跳
		DPrintf("HB: <%d-%s>: vote win", rf.me, Leader)
		rf.switchToLeaderAndPersist()
		return leaderHandler
	default:
		panic(fmt.Sprintf("HB: unknown vote result %v", result))
	}
}

// requestVoteTimeout Candidate状态超时
func requestVoteTimeout(rf *Raft) {
	rf.mu.Lock()
	if rf.role != Candidate {
		rf.mu.Unlock()
		return
	}
	DPrintf("HB: <%d-%d>: (election timeout)", rf.me, rf.role)
	rf.mu.Unlock()

	rf.electionTimeoutSignal <- struct{}{} // 选举超时
}
