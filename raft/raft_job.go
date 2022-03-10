package raft

import (
	"time"
)

const (
	heartbeatInterval  = time.Millisecond * 100
	requestVoteTimeout = time.Millisecond * 200
	applierInterval    = time.Millisecond * 100
)

// heatbeat 监控心跳事件
func heatbeat(rf *Raft) {
	rf.mu.Lock()

	timer := rf.timer
	rf.resetTimer(heartbeatInterval)

	rf.mu.Unlock()

	for {
		select {
		case <-timer.C:
			// 角色可能发生了变化
			rf.mu.Lock()

			if rf.killed() {
				rf.DPrintf("<%d-%s>: find killed when heartbeat %p", rf.me, rf.getRole(), rf)
				rf.mu.Unlock()
				return
			}

			handler := heartbeatHandlerMap[rf.role]
			rf.mu.Unlock()

			go handler(rf)
		}
	}
}

// applier 应用已提交的日志条目
func applier(rf *Raft) {
	rf.mu.Lock()
	rf.DPrintf("<%d-%s>: applier start", rf.me, rf.getRole())
	rf.mu.Unlock()

	ticker := time.NewTicker(applierInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rf.mu.Lock()

			if rf.killed() {
				rf.DPrintf("<%d-%s>: applier exit because node is killed %p", rf.me, rf.getRole(), rf)
				rf.mu.Unlock()
				return
			}

			lastApplied, commitIndex := rf.lastApplied, rf.commitIndex
			rf.DPrintf("APPLY <%d-%s>: apply status: lastApplied %d, commitIndex %d", rf.me, rf.getRole(), lastApplied, commitIndex)
			logs := make([]*raftLog, commitIndex-lastApplied)
			copy(logs, rf.logs[lastApplied+1:commitIndex+1])

			if len(logs) == 0 {
				rf.mu.Unlock()
				continue
			}

			rf.DPrintf("<%d-%s>: applying %d logs", rf.me, rf.getRole(), len(logs))

			for i := 0; i < len(logs); i++ {
				idx := int(lastApplied+1) + i
				msg := ApplyMsg{
					CommandValid: true,
					Command:      logs[i].Entry,
					CommandIndex: idx,
				}
				rf.applyMsgChan <- msg
				rf.lastApplied = uint64(idx)
			}
			rf.mu.Unlock()
		}
	}
}
