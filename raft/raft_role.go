package raft

const (
	Follower uint32 = iota
	Leader
	Candidate

	FollowerRole  = "follower"
	LeaderRole    = "leader"
	CandidateRole = "candidate"
)

var roleMap = map[uint32]string{}

type heartbeatHandler func(raft *Raft)

var heartbeatHandlerMap = map[uint32]heartbeatHandler{}

func init() {
	roleMap[Follower] = FollowerRole
	roleMap[Candidate] = CandidateRole
	roleMap[Leader] = LeaderRole

	heartbeatHandlerMap[Follower] = followerHandler
	heartbeatHandlerMap[Candidate] = candidateHandler
	heartbeatHandlerMap[Leader] = leaderHandler
}

// leaderHandler leader的心跳定时器到期
func leaderHandler(rf *Raft) {
	appendEntry(rf)
}

// candidateHandler candidate的心跳定时器到期
func candidateHandler(rf *Raft) {
	requestVoteTimeout(rf)
}

// followerHandler follower的心跳定时器到期
func followerHandler(rf *Raft) {
	requestVote(rf)
}
