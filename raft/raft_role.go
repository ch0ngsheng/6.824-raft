package raft

const (
	raftFollower uint32 = iota
	raftLeader
	raftCandidate

	raftFollowerRole  = "follower"
	raftLeaderRole    = "leader"
	raftCandidateRole = "candidate"
)

var roleMap = map[uint32]string{}

type heartbeatHandler func(raft *Raft)

var heartbeatHandlerMap = map[uint32]heartbeatHandler{}

func init() {
	roleMap[raftFollower] = raftFollowerRole
	roleMap[raftCandidate] = raftCandidateRole
	roleMap[raftLeader] = raftLeaderRole

	heartbeatHandlerMap[raftFollower] = followerHandler
	heartbeatHandlerMap[raftCandidate] = candidateHandler
	heartbeatHandlerMap[raftLeader] = leaderHandler
}

// leaderHandler leader的心跳定时器到期
func leaderHandler(rf *Raft) {
	appendEntry(rf)
}

// candidateHandler candidate的心跳定时器到期
func candidateHandler(rf *Raft) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != raftCandidate {
		return
	}
	rf.electionStopChan <- struct{}{} // 选举超时
	rf.DPrintf("HB: <%d-%s>: (election timeout)", rf.me, rf.getRole())
}

// followerHandler follower的心跳定时器到期
func followerHandler(rf *Raft) {
	requestVote(rf)
}
