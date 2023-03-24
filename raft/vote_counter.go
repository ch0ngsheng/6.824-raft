package raft

// VoteCounter 统计选举期间的票数
type VoteCounter struct {
	raftID    int
	acceptCnt int
	denyCnt   int
	accepted  []int
	denied    []int
}

func NewVoteCounter(id int) *VoteCounter {
	return &VoteCounter{
		raftID:    id,
		accepted:  []int{id},
		acceptCnt: 1,
	}
}

func (vc *VoteCounter) Reset() {
	vc.denied = []int{}
	vc.accepted = []int{vc.raftID}
	vc.acceptCnt = 1
	vc.denyCnt = 0
}

func (vc *VoteCounter) AcceptFrom(from int) {
	vc.acceptCnt += 1
	vc.accepted = append(vc.accepted, from)
}

func (vc *VoteCounter) DenyFrom(from int) {
	vc.denyCnt += 1
	vc.denied = append(vc.denied, from)
}

func (vc *VoteCounter) AcceptedList() []int {
	return vc.accepted
}

func (vc *VoteCounter) DeniedList() []int {
	return vc.denied
}

func (vc *VoteCounter) TotalAccepted() int {
	return vc.acceptCnt
}

func (vc *VoteCounter) TotalDenied() int {
	return vc.denyCnt
}
