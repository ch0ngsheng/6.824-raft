package raft

// VoteCounter 统计选举期间的票数
type VoteCounter struct {
	belongsTo int
	voteCnt   int
	denyCnt   int
	voted     []int
	denied    []int
}

func NewVoteCounter(belongsTo int) *VoteCounter {
	return &VoteCounter{
		belongsTo: belongsTo,
		voted:     []int{belongsTo},
		voteCnt:   1,
	}
}

func (vc *VoteCounter) Reset() {
	vc.denied = []int{}
	vc.voted = []int{vc.belongsTo}
	vc.voteCnt = 1
	vc.denyCnt = 0
}

func (vc *VoteCounter) Vote(from int) {
	vc.voteCnt += 1
	vc.voted = append(vc.voted, from)
}

func (vc *VoteCounter) Deny(from int) {
	vc.denyCnt += 1
	vc.denied = append(vc.denied, from)
}

func (vc *VoteCounter) Voted() []int {
	return vc.voted
}

func (vc *VoteCounter) Denied() []int {
	return vc.denied
}

func (vc *VoteCounter) VoteNum() int {
	return vc.voteCnt
}

func (vc *VoteCounter) DenyNum() int {
	return vc.denyCnt
}
