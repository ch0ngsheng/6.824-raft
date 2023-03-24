package raft

type VoteRPCUtil struct {
}

var voteRPCUtil = VoteRPCUtil{}

// RequestVoteArgs RequestVote RPC的请求参数
type RequestVoteArgs struct {
	Term         TermType
	CandidateID  int
	LastLogIndex uint64
	LastLogTerm  TermType
}

// RequestVoteReply RequestVote RPC 的响应参数
type RequestVoteReply struct {
	Term  TermType
	Voted bool
}

// RequestVote 处理 RequestVote RPC
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[RPC Receive RV begin] me: %d, from: %d with term: %d",
		rf.me, args.CandidateID, args.Term,
	)
	defer func() {
		DPrintf("[RPC Receive RV end] me: %d, from: %d with term: %d, accepted: %v, args: %v, reply: %v",
			rf.me, args.CandidateID, args.Term, reply.Voted, *args, *reply,
		)
	}()

	if ok := voteRPCUtil.checkTermAndRole(rf, args, reply); !ok {
		return
	}

	if voteRPCUtil.hasVoted(rf, args) {
		// 本任期已投票
		DPrintf("Receive RV, me: %d, from: %d with term: %d, accepted: false, for accepted already.",
			rf.me, args.CandidateID, args.Term,
		)
		voteRPCUtil.deny(rf, reply)
		return
	}

	if voteRPCUtil.canVote(rf, args) {
		voteRPCUtil.vote(rf, args, reply)
		return
	}

	DPrintf("Receive RV, me: %d, from: %d with term: %d, accepted: false, for old log.",
		rf.me, args.CandidateID, args.Term,
	)
	voteRPCUtil.deny(rf, reply)
}

func (VoteRPCUtil) checkTermAndRole(rf *Raft, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	curTerm := rf.term
	if args.Term < curTerm {
		reply.Term = curTerm
		reply.Voted = false
		// 对方term太小，不能重置定时器
		return false
	}

	if args.Term > curTerm {
		rf.switchToFollowerAndPersist(args.Term)
	}

	return true
}

func (VoteRPCUtil) hasVoted(rf *Raft, args *RequestVoteArgs) bool {
	if rf.votedFor == -1 || rf.votedFor == int32(args.CandidateID) {
		return false
	}
	return true
}

func (VoteRPCUtil) canVote(rf *Raft, args *RequestVoteArgs) bool {
	lastLogIndex, lastLogTerm := rf.getLastLogNumber()
	if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
		return true
	}
	return false
}

func (VoteRPCUtil) vote(rf *Raft, args *RequestVoteArgs, reply *RequestVoteReply) {
	reply.Voted = true
	reply.Term = rf.term

	rf.updateVotedForAndPersist(int32(args.CandidateID))
	rf.resetElectionTimer()
}

func (VoteRPCUtil) deny(rf *Raft, reply *RequestVoteReply) {
	reply.Voted = false
	reply.Term = rf.term
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	// blocked, guaranteed to return, have a delay， no need to implement your own timeouts
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
