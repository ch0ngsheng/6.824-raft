package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824-raft/labgob"
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.824-raft/labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type raftLog struct {
	Term  uint64
	Entry interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role uint32 // 当前角色

	timer          *time.Timer // 选举定时器
	timerResetTime time.Time   // 选举定时器重置时间

	// 持久化字段
	term     uint64     // 当前任期
	votedFor int32      // 本任期投票给了哪个节点，-1代表本任期内未投票
	logs     []*raftLog // 日志条目

	// 持久化字段，非Raft必须，仅用于优化
	logTermsLast  map[uint64]uint64 // 记录日志中的term号，及该任期内的最后一条日志索引位置
	logTermsFirst map[uint64]uint64 // 记录日志中的term号，及该任期内的第一条日志索引位置

	nextIndex    []uint64      // leader维护的下一次AppendEntry需要发送给各节点的日志条目索引
	matchIndex   []uint64      // leader维护的各节点日志条目已同步的最高索引
	commitIndex  uint64        // 日志条目已提交索引
	lastApplied  uint64        // 本节点日志条目已应用索引
	applyMsgChan chan ApplyMsg // 本节点日志条目应用目标chan

	// 用于调试记录的字段
	appendEntryTimes uint32 // 发起追加日志条目RPC次数
	requestVoteTimes uint64 // 发起请求选举RPC次数
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (termNo int, isLeader bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	termNo = int(rf.term)
	isLeader = rf.role == raftLeader
	return
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(rf.term); err != nil {
		panic(err)
	}
	if err := e.Encode(rf.votedFor); err != nil {
		panic(err)
	}
	if err := e.Encode(rf.logs); err != nil {
		panic(err)
	}
	if err := e.Encode(rf.logTermsFirst); err != nil {
		panic(err)
	}
	if err := e.Encode(rf.logTermsLast); err != nil {
		panic(err)
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.DPrintf("[Persist Loaded] for %d: none", rf.me)
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term uint64
	var votedFor int32
	var logs = make([]*raftLog, 0)
	var logTermsFirst = make(map[uint64]uint64)
	var logTermsLast = make(map[uint64]uint64)

	if err := d.Decode(&term); err != nil {
		panic(err)
	}
	if err := d.Decode(&votedFor); err != nil {
		panic(err)
	}
	if err := d.Decode(&logs); err != nil {
		panic(err)
	}
	if err := d.Decode(&logTermsFirst); err != nil {
		panic(err)
	}
	if err := d.Decode(&logTermsLast); err != nil {
		panic(err)
	}

	rf.term, rf.votedFor, rf.logs = term, votedFor, logs
	rf.logTermsFirst, rf.logTermsLast = logTermsFirst, logTermsLast

	rf.DPrintf("[Persist Loaded] for %d: term: %d votedFor %d len(logs) %d len(mapFirst) %d len(mapLast) %d",
		rf.me, rf.term, rf.votedFor, len(rf.logs), len(rf.logTermsFirst), len(rf.logTermsLast),
	)
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (index int, term int, leader bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != raftLeader {
		return 0, int(rf.term), false
	}

	// append log
	log := &raftLog{
		Term:  rf.term,
		Entry: command,
	}
	rf.logs = append(rf.logs, log)
	index = len(rf.logs) - 1

	// update logTermsFirst logTermsLast
	if _, ok := rf.logTermsFirst[rf.term]; !ok {
		rf.logTermsFirst[rf.term] = uint64(index)
	}
	rf.logTermsLast[rf.term] = uint64(index)

	rf.persist()

	// leader's matchIndex
	rf.matchIndex[rf.me] = uint64(index)

	rf.DPrintf("[Start] leader %d index %d term %d cmd %s logs %s", rf.me, index, rf.term, getMd5(command), logsToString(rf.logs))
	return index, int(rf.term), true
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.DPrintf("<%d-%s>: signal killed %p", rf.me, rf.getRole(), rf)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyMsgChan = applyCh
	rf.lastApplied = 0
	rf.commitIndex = 0

	// Your initialization code here (2A, 2B, 2C).
	rf.role = raftFollower
	rf.votedFor = -1
	rf.term = 0

	rf.timer = time.NewTimer(getRandomDuration(heartbeatInterval, rf.me))
	rf.timer.Stop()

	rf.logs = []*raftLog{{0, 0}} // log索引从1开始
	rf.logTermsFirst = map[uint64]uint64{0: 0}
	rf.logTermsLast = map[uint64]uint64{0: 0}

	rf.nextIndex = make([]uint64, len(rf.peers))
	rf.matchIndex = make([]uint64, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = uint64(len(rf.logs)) // 初始值为leader最后一个日志条目的索引值+1
		rf.matchIndex[i] = 0
	}

	go heatbeat(rf) // 心跳检测

	go applier(rf) // 日志应用

	rf.DPrintf("RAFT >>>>>>> %d", rf.me)
	return rf
}
