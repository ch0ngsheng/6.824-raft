# 6.824-raft

## 问题一
[此时，0的日志长度为]
怀疑原因：panic的是一个leader L
把某个节点A的日志追加到918，并设置nextLogIndex[A] = 919
L的日志由另一个leader，回滚到415长度（此时应是follower，应停止发送AE）
L(可能又成为leader)给A发送AE RPC，根据nextLogIndex计算的preLogIndex为918，越界

处理方法：
变为follower时，停止发送AE；
发送或者重试AE RPC前，判断自己是否还是leader

panic: runtime error: index out of range [918] with length 415

goroutine 15004 [running]:
6.824-raft/raft.(*Raft).buildAppendEntryInfoByID(...)
/Users/chongshengyu/Code/etcd/6.824-raft/raft/raft_append_entry.go:29
6.824-raft/raft.appendEntry(0xc000126460)
/Users/chongshengyu/Code/etcd/6.824-raft/raft/raft_job.go:266 +0xb25
created by 6.824-raft/raft.followerHandler
/Users/chongshengyu/Code/etcd/6.824-raft/raft/raft_role.go:79 +0x5cf
exit status 2
FAIL	6.824-raft/raft	35.523s

## 还是有问题
Test (2C): Figure 8 (unreliable) ...
runtime error: index out of range [300] with length 280 1 3 1 300 280
panic: runtime error: index out of range [300] with length 280 [recovered]
panic: runtime error: index out of range [300] with length 280

goroutine 58870 [running]:
6.824-raft/raft.(*Raft).buildAppendEntryInfoByID.func1()
/Users/chongshengyu/Code/etcd/6.824-raft/raft/raft_append_entry.go:36 +0x1fe
panic({0x1261560, 0xc00001fe00})
/Users/chongshengyu/.g/go/src/runtime/panic.go:1047 +0x266
6.824-raft/raft.(*Raft).buildAppendEntryInfoByID(0xc0002a8b60, 0x1)
/Users/chongshengyu/Code/etcd/6.824-raft/raft/raft_append_entry.go:40 +0x396
6.824-raft/raft.appendEntry(0xc0002a8b60)
/Users/chongshengyu/Code/etcd/6.824-raft/raft/raft_job.go:311 +0xc7e
6.824-raft/raft.followerHandler.func1(0x0)
/Users/chongshengyu/Code/etcd/6.824-raft/raft/raft_role.go:95 +0x2f
created by 6.824-raft/raft.followerHandler
/Users/chongshengyu/Code/etcd/6.824-raft/raft/raft_role.go:94 +0xada

## 问题二

Fig8. unreliable的测试 达不成一致性：
L0收到C3回复的更高的term号，L0应立即变为follower，停止AE。