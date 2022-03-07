package raft

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         uint64
	CandidateID  int
	LastLogIndex uint64
	LastLogTerm  uint64
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term  uint64
	Voted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//rf.DPrintf("RPC Receive RV begin, me: %d, from: %d, term: %d", rf.me, args.CandidateID, args.Term)
	defer func() {
		rf.DPrintf("Receive RV, me: %d, from: %d with term: %d, voted: %v, args: %v, reply: %v", rf.me, args.CandidateID, args.Term, reply.Voted, *args, *reply)
	}()

	curTerm := rf.term
	if args.Term < curTerm {
		reply.Term = curTerm
		reply.Voted = false
		// 不能重置定时器
		return
	}
	if args.Term > curTerm {
		// 停止选举
		if rf.role == raftCandidate {
			go func() {
				rf.votingStopChan <- struct{}{}
			}()
		}
		// 停止发送AE
		if rf.role == raftLeader {
			go func() {
				rf.appendEntryStopChan <- struct{}{}
			}()

		}

		rf.updateTerm(args.Term)
		rf.switchRole(raftFollower)
	}

	if rf.votedFor == -1 || rf.votedFor == int32(args.CandidateID) {
		lastLogIndex, lastLogTerm := rf.getLastLogNumber()
		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
			reply.Voted = true
			reply.Term = rf.term
			// 重置定时器
			du := rf.appendEntryTimerDuration
			rf.appendEntryTimer.Reset(du)
			rf.DPrintf("<RST AE Timer><%d-%s> val: %v", rf.me, rf.getRole(), du)
			rf.updateVotedFor(int32(args.CandidateID))
		} else {
			reply.Voted = false
			reply.Term = rf.term
			return
		}
	} else {
		// 本任期已投票
		reply.Voted = false
		reply.Term = rf.term
		// 不能重置定时器
		return
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	// blocked, guaranteed to return, have a delay， no need to implement your own timeouts
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
