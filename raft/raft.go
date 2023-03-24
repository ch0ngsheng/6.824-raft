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
	"bytes"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"6.824-raft/labgob"
	"6.824-raft/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.

// ApplyMsg 向状态机应用的一条日志命令
type ApplyMsg struct {
	CommandValid bool        // 是否合法
	Command      interface{} // 命令信息，测试框架中使用int类型
	CommandIndex int         // 命令索引
}

type TermType uint32

// Log 日志条目
type Log struct {
	Term    TermType    // 任期
	Command interface{} // 命令
	NoOp    bool        // 是否是NO-OP日志
}

// Logs 日志列表，用于调试输出
type Logs []*Log

func (logs Logs) String() string {
	sb := strings.Builder{}
	for _, log := range logs {
		var command string
		switch log.Command.(type) {
		case string:
			command = log.Command.(string)
		case int:
			command = fmt.Sprintf("%d", log.Command.(int))
		}
		sb.WriteString(fmt.Sprintf("%d-%s,", log.Term, command)) // 测试框架中Command为int类型
	}
	return sb.String()
}

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	stopCh chan struct{}

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role uint32 // 当前角色

	electionTimer         *time.Timer   // 选举定时器
	timerResetTime        time.Time     // 选举定时器重置时间
	electionTimeoutSignal chan struct{} // 选举超时信号
	voteCounter           *VoteCounter  // 选举计票器，candidate计票用

	// 持久化字段
	term          TermType // 当前任期
	votedFor      int32    // 本任期投票给了哪个节点，-1代表本任期内未投票
	logs          []*Log   // 日志条目
	totalNoOPLogs uint64   // 总的NO-OP日志条数

	// 持久化字段，非Raft必须
	// 用于加速leader计算follower当前日志进度，索引和任期范围和logs一致
	LastIndexPerTerm  map[TermType]uint64 // 记录日志中的term号，及该任期内的最后一条日志索引位置
	FirstIndexPerTerm map[TermType]uint64 // 记录日志中的term号，及该任期内的第一条日志索引位置

	nextAppendEntryIndex []uint64      // leader维护的下一次AppendEntry需要发送给各节点的日志条目索引，初始为日志末尾
	matchIndex           []uint64      // leader维护的各节点日志条目已同步的最高索引，初始为0
	commitIndex          uint64        // leader维护的日志条目已提交索引
	lastAppliedIndex     uint64        // 本节点日志条目已应用索引，含NO-OP日志。节点重建后需要重新应用日志，因此不需要持久化
	lastAppliedOPIndex   uint64        // 不含NO-OP日志的已应用索引，用于维护状态机应用进度。
	applyMsgChan         chan ApplyMsg // 本节点日志条目应用目标chan

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
	isLeader = rf.role == Leader
	return
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	data := rf.buildPersistBytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) buildPersistBytes() []byte {
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
	if err := e.Encode(rf.totalNoOPLogs); err != nil {
		panic(err)
	}
	if err := e.Encode(rf.FirstIndexPerTerm); err != nil {
		panic(err)
	}
	if err := e.Encode(rf.LastIndexPerTerm); err != nil {
		panic(err)
	}
	return w.Bytes()
}

func (rf *Raft) loadPersistData(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	if err := d.Decode(&rf.term); err != nil {
		panic(err)
	}
	if err := d.Decode(&rf.votedFor); err != nil {
		panic(err)
	}
	if err := d.Decode(&rf.logs); err != nil {
		panic(err)
	}
	if err := d.Decode(&rf.totalNoOPLogs); err != nil {
		panic(err)
	}
	if err := d.Decode(&rf.FirstIndexPerTerm); err != nil {
		panic(err)
	}
	if err := d.Decode(&rf.LastIndexPerTerm); err != nil {
		panic(err)
	}
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		DPrintf("[Persist Loaded] for %d: none", rf.me)
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

	rf.loadPersistData(data)
	DPrintf("[Persist Loaded] for %d: term: %d votedFor %d len(logs) %d len(mapFirst) %d len(mapLast) %d",
		rf.me, rf.term, rf.votedFor, len(rf.logs), len(rf.FirstIndexPerTerm), len(rf.LastIndexPerTerm),
	)
}

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
func (rf *Raft) Start(command interface{}) (index int, term int, leader bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		return 0, int(rf.term), false
	}

	// append log
	log := &Log{
		Term:    rf.term,
		Command: command,
		NoOp:    false,
	}

	index = rf.WriteLog(log)

	DPrintf("[Start] leader %d index %d term %d cmd %v logs %s", rf.me, index, rf.term, command, Logs(rf.logs))
	return index, int(rf.term), true
}

func (rf *Raft) WriteLog(log *Log) int {
	rf.logs = append(rf.logs, log)
	index := len(rf.logs) - 1

	if _, ok := rf.FirstIndexPerTerm[rf.term]; !ok {
		rf.FirstIndexPerTerm[rf.term] = uint64(index)
	}
	rf.LastIndexPerTerm[rf.term] = uint64(index)

	if log.NoOp {
		rf.totalNoOPLogs += 1
	}

	rf.persist()

	// leader's matchIndex
	rf.matchIndex[rf.me] = uint64(index)

	return index - int(rf.totalNoOPLogs)
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.

	rf.stopCh <- struct{}{}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("<%d-%s>: signal killed %p", rf.me, rf.role, rf)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyMsgChan = applyCh
	rf.lastAppliedIndex = 0
	rf.lastAppliedOPIndex = 0
	rf.commitIndex = 0

	// Your initialization code here (2A, 2B, 2C).
	rf.role = Follower
	rf.votedFor = -1
	rf.term = 0
	rf.stopCh = make(chan struct{})

	rf.electionTimer = time.NewTimer(GetRandomDuration(ElectionTimeout, rf.me))
	rf.electionTimer.Stop()
	rf.electionTimeoutSignal = make(chan struct{})
	rf.voteCounter = NewVoteCounter(rf.me)

	rf.logs = []*Log{{0, 0, false}} // log索引从1开始
	rf.FirstIndexPerTerm = map[TermType]uint64{0: 0}
	rf.LastIndexPerTerm = map[TermType]uint64{0: 0}

	rf.nextAppendEntryIndex = make([]uint64, len(rf.peers))
	rf.matchIndex = make([]uint64, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go heartbeat(rf) // 心跳检测

	go applier(rf) // 日志应用

	DPrintf("RAFT Start >>>>>>> %d", rf.me)
	return rf
}
