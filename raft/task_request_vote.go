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

	rf.resetTimer(heartbeatInterval)
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
