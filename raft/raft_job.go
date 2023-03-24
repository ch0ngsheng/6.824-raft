package raft

import (
	"time"
)

// heartbeat 监控心跳事件
func heartbeat(rf *Raft) {
	rf.mu.Lock()
	rf.resetElectionTimer()
	rf.mu.Unlock()

	for {
		select {
		case <-rf.stopCh:
			return
		case <-rf.electionTimer.C:
			func() {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				// 角色可能发生了变化
				if rf.killed() {
					DPrintf("<%d-%s>: find killed when heartbeat.", rf.me, rf.role)
					return
				}

				DPrintf("<%d-%s>: election-timer-reset %s", rf.me, rf.role, time.Now().Format(time.RFC3339Nano))
				go heartbeatHandlerMap[rf.role](rf)
			}()
		}
	}
}

// applier 应用已提交的日志条目
func applier(rf *Raft) {
	ticker := time.NewTicker(ApplierInterval)
	defer ticker.Stop()

	for {
		select {
		case <-rf.stopCh:
			return
		case <-ticker.C:
			if rf.killed() {
				rf.mu.Lock()
				DPrintf("<%d-%s>: applier exit because node is killed.", rf.me, rf.role)
				rf.mu.Unlock()
				return
			}

			applyLogBatchLocked(rf)
		}
	}
}

func applyLogBatchLocked(rf *Raft) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("APPLY <%d-%s>: apply status: lastAppliedIndex %d, commitIndex %d",
		rf.me, rf.role, rf.lastAppliedIndex, rf.commitIndex)

	logs := rf.logs[rf.lastAppliedIndex+1 : rf.commitIndex+1]
	if len(logs) == 0 {
		return
	}

	DPrintf("<%d-%s>: applying %d logs", rf.me, rf.role, len(logs))

	for i := 0; i < len(logs); i++ {
		if logs[i].NoOp {
			continue
		}
		msg := ApplyMsg{
			CommandValid: true,
			Command:      logs[i].Command,
			CommandIndex: int(rf.lastAppliedOPIndex) + 1,
		}
		rf.applyMsgChan <- msg
		rf.lastAppliedOPIndex += 1
	}

	rf.lastAppliedIndex += uint64(len(logs))
}
