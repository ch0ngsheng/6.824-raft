package raft

type reqVoteUtil struct {
}

var rv = reqVoteUtil{}

func (reqVoteUtil) checkTermAndRole(rf *Raft, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	curTerm := rf.term
	if args.Term < curTerm {
		reply.Term = curTerm
		reply.Voted = false
		// 对方term太小，不能重置定时器
		return false
	}

	if args.Term > curTerm {
		rf.updateTermAndPersist(args.Term)
		rf.switchRole(raftFollower)
	}

	return true
}

func (reqVoteUtil) hasVoted(rf *Raft, args *RequestVoteArgs) bool {
	if rf.votedFor == -1 || rf.votedFor == int32(args.CandidateID) {
		return false
	}
	return true
}

func (reqVoteUtil) canVote(rf *Raft, args *RequestVoteArgs) bool {
	lastLogIndex, lastLogTerm := rf.getLastLogNumber()
	if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
		return true
	}
	return false
}

func (reqVoteUtil) vote(rf *Raft, args *RequestVoteArgs, reply *RequestVoteReply) {
	reply.Voted = true
	reply.Term = rf.term

	rf.updateVotedForAndPersist(int32(args.CandidateID))
	rf.rstElectionTimer()
}

func (reqVoteUtil) deny(rf *Raft, reply *RequestVoteReply) {
	reply.Voted = false
	reply.Term = rf.term
}

// RequestVoteArgs Request Vote RPC的请求参数
type RequestVoteArgs struct {
	Term         uint64
	CandidateID  int
	LastLogIndex uint64
	LastLogTerm  uint64
}

// RequestVoteReply Request Vote RPC 的响应参数
type RequestVoteReply struct {
	Term  uint64
	Voted bool
}

// RequestVote 处理 Request Vote RPC
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.DPrintf("[RPC Receive RV begin] me: %d, from: %d with term: %d",
		rf.me, args.CandidateID, args.Term,
	)
	defer func() {
		rf.DPrintf("[RPC Receive RV end] me: %d, from: %d with term: %d, voted: %v, args: %v, reply: %v",
			rf.me, args.CandidateID, args.Term, reply.Voted, *args, *reply,
		)
	}()

	if ok := rv.checkTermAndRole(rf, args, reply); !ok {
		return
	}

	if rv.hasVoted(rf, args) {
		// 本任期已投票
		rf.DPrintf("Receive RV, me: %d, from: %d with term: %d, voted: false, for voted already.",
			rf.me, args.CandidateID, args.Term,
		)
		rv.deny(rf, reply)
		return
	}

	if rv.canVote(rf, args) {
		rv.vote(rf, args, reply)
		return
	}

	rf.DPrintf("Receive RV, me: %d, from: %d with term: %d, voted: false, for old log.",
		rf.me, args.CandidateID, args.Term,
	)
	rv.deny(rf, reply)
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
