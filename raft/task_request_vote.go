package raft

import (
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

func (sendReqVoteUtil) prepareLocked(rf *Raft) (*RequestVoteArgs, chan *requestVoteResult, chan struct{}) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

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

	return request, resultChan, electionStopChan
}

func (sendReqVoteUtil) sendAsync(rf *Raft, request *RequestVoteArgs, resultChan chan *requestVoteResult) {
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

func (sendReqVoteUtil) checkRoleLocked(rf *Raft, returnChan chan int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		returnChan <- voteResultKilled
		return false
	}

	if rf.role != raftCandidate {
		returnChan <- voteResultNotCandidate
		return false
	}

	return true
}

func (sendReqVoteUtil) checkResultLocked(rf *Raft, returnChan chan int,
	res *requestVoteResult, votedNodes, denyNodes []int) ([]int, []int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		returnChan <- voteResultKilled
		return votedNodes, denyNodes, false
	}
	if rf.role != raftCandidate {
		returnChan <- voteResultNotCandidate
		return votedNodes, denyNodes, false
	}

	if res.args.Term < rf.term {
		// 来自之前发出的消息结果，网络是不可信的
		rf.DPrintf("HB: <%d-%s>: term %d, receive old RV resp from %d with old term %d, ignore",
			rf.me, rf.getRole(), rf.term, res.fromWho, res.args.Term)
		// 收到老term的消息，说明这个协程已经是老的了，直接退出
		returnChan <- voteResultOutdated
		return votedNodes, denyNodes, false
	}

	if !res.ok {
		// 节点响应超时
		denyNodes = append(denyNodes, res.fromWho)
		rf.DPrintf("HB: <%d-%s>: term: %d, vote from %d, result: %s",
			rf.me, rf.getRole(), rf.term, res.fromWho, "node not ok")
	} else {
		if res.reply.Term > atomic.LoadUint64(&rf.term) {
			rf.DPrintf("HB: <%d-%s>: term: %d, vote quit because higher term %d",
				rf.me, rf.getRole(), rf.term, res.reply.Term)

			rf.updateTermAndPersist(res.reply.Term) // mark 收到高term响应
			returnChan <- voteResultFoundHigherTerm
			return votedNodes, denyNodes, false
		}
		if res.reply.Voted {
			votedNodes = append(votedNodes, res.fromWho)
			rf.DPrintf("HB: <%d-%s>: term: %d, vote resp from %d %s",
				rf.me, rf.getRole(), rf.term, res.fromWho, "voted")
		} else {
			denyNodes = append(denyNodes, res.fromWho)
			rf.DPrintf("HB: <%d-%s>: term: %d, vote resp from %d %s",
				rf.me, rf.getRole(), rf.term, res.fromWho, "deny")
		}
	}

	if len(votedNodes) > len(rf.peers)/2 {
		// 当选
		rf.DPrintf("HB: <%d-%s>: term: %d, voted: %v, deny: %v result: %s",
			rf.me, rf.getRole(), rf.term, votedNodes, denyNodes, "win!")
		returnChan <- voteResultWin
		return votedNodes, denyNodes, false
	}
	if len(denyNodes) > len(rf.peers)/2 {
		// 未当选
		rf.DPrintf("HB: <%d-%s>: term: %d, voted: %v, deny: %v result: %s",
			rf.me, rf.getRole(), rf.term, votedNodes, denyNodes, "lose!")
		returnChan <- voteResultLose
		return votedNodes, denyNodes, false
	}

	// 继续等待
	return votedNodes, denyNodes, true
}

// requestVote 发起投票，follower在心跳超时时执行
func requestVote(rf *Raft, returnChan chan int) {
	request, resultChan, electionStopChan := srv.prepareLocked(rf)

	srv.sendAsync(rf, request, resultChan)

	votedNodes := []int{rf.me}
	var denyNodes []int
	var goon bool

	ticker := time.NewTicker(time.Millisecond * 40)
	defer ticker.Stop()

	// mark 从发出RPC，到接收到RPC响应，raft的状态可能发生了变化
	for {
		if ok := srv.checkRoleLocked(rf, returnChan); !ok {
			return
		}

		select {
		case <-electionStopChan:
			// 选举超时
			returnChan <- voteResultTimeout
			return
		case <-ticker.C:
			if ok := srv.checkRoleLocked(rf, returnChan); !ok {
				return
			}
		case res := <-resultChan:
			votedNodes, denyNodes, goon = srv.checkResultLocked(rf, returnChan, res, votedNodes, denyNodes)
			if !goon {
				return
			}
		}
	}
}
