package raft

import "time"

const (
	HeartbeatInterval = time.Millisecond * 50
	ElectionTimeout   = time.Millisecond * 150
	ApplierInterval   = time.Millisecond * 100
)

/*
测试发现，在不同性能的执行机上，这几个参数需要做调整，才能通过benchmark。
否则会出现 raft failed to reach agreement 之类的超时问题。

参考数据

8核心笔记本
leader发送心跳间隔：50ms
follower选举超时：150ms
应用到状态机间隔：100ms

单核心ECS
leader发送心跳间隔：150ms
follower选举超时：350ms
应用到状态机间隔：200ms
*/
