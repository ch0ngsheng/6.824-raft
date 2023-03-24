# 6.824-raft
## 介绍

## 实现细节
1. 在Candidate发出投票RPC或Leader发出心跳RPC后，等待响应的过程中，要检查自己是否还是Candidate/Leader状态。
   * 如果不再是Candidate/Leader，应该停止等待响应
   * 这种情况发生在收到其他高任期节点的消息后，变为Follower时
   * 类似的，在发出RPC或者重试RPC前，也要先确认自己的身份

2. 不要过早优化，直接使用sync.Mutex上大锁即可
   * 不要在一开始就考虑怎么使用atomic包减小锁争用

3. 需要实现一种论文中提到的快速匹配Follower日志进度的方法
   * 集群网络不稳定时，部分Follower的日志和Leader差异较大，Leader需要快速匹配到每个Follower的日志进度

4. 需要实现论文中提到的No-OP日志
   * 成为Leader后，需要向日志中追加当前任期的No-OP日志
   * 防止因客户端无写入请求导致低任期的日志无法提交，进而无法应用到状态机
   * 因为根据`安全性`要求，Leader只能提交自己任期的日志

## 测试结果
```shell
Test (2A): initial election ...
  ... Passed --   3.0  3  120   33022    0
Test (2A): election after network failure ...
  ... Passed --   4.5  3  268   54316    0
PASS
ok  	6.824-raft/raft	7.988s

Test (2B): basic agreement ...
  ... Passed --   0.6  3   26    6826    3
Test (2B): RPC byte count ...
  ... Passed --   1.4  3   58  116454   11
Test (2B): agreement despite follower disconnection ...
  ... Passed --   3.7  3  174   43092    7
Test (2B): no agreement if too many followers disconnect ...
  ... Passed --   3.4  5  410   83336    3
Test (2B): concurrent Start()s ...
  ... Passed --   0.6  3   26    7194    6
Test (2B): rejoin of partitioned leader ...
  ... Passed --   4.1  3  297   67898    4
Test (2B): leader backs up quickly over incorrect follower logs ...
  ... Passed --  16.6  5 2909 2162131  102
Test (2B): RPC counts aren't too high ...
  ... Passed --   2.2  3   90   25274   12
PASS
ok  	6.824-raft/raft	32.952s


Test (2C): basic persistence ...
labgob warning: Decoding into a non-default variable/field int32 may not work
  ... Passed --   3.3  3  161   41705    6
Test (2C): more persistence ...
  ... Passed --  15.2  5 1725  362817   16
Test (2C): partitioned leader and one follower crash, leader restarts ...
  ... Passed --   1.5  3   65   16396    4
Test (2C): Figure 8 ...
  ... Passed --  36.6  5 3047  702232   54
Test (2C): unreliable agreement ...
  ... Passed --   5.2  5  415  126281  246
Test (2C): Figure 8 (unreliable) ...
  ... Passed --  35.3  5 6981 11407610   98
Test (2C): churn ...
  ... Passed --  16.2  5 2004 1021379   85
Test (2C): unreliable churn ...
  ... Passed --  16.3  5 1419  426589  289
PASS
ok  	6.824-raft/raft	129.868s
```
