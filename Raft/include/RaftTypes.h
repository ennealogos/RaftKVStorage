#pragma once

#include <string>
#include <vector>

// raft 节点角色
enum class Role {
    Follower,  // 接收 leader 心跳和日志
    Candidate, // 选举超时拉票
    Leader, // 发送心跳并同步日志
};

// 竞选者拉票请求
struct RequestVoteArgs {
    int term = 0;  // 竞选者的竞选任期
    int candidateId = 0; // 竞选者的节点 id

    // 竞选者最后一条日志的全局索引
    // 空日志，无快照时为 0， 有快照至少等于快照最后的索引
    int lastLogIndex = 0;
    int lastLogTerm = 0; // 空日志时为 0；有快照时至少等于快照的 lastIncludedTerm。
};

// 节点对拉票请求的回复
struct RequestVoteReply {
    // 如果它比 candidate 的 term 大，candidate 应立即降级为 Follower。
    int term = 0; // 节点记录的任期
    bool voteGranted = false; // 是否同意投票
};

// 一条 raft 日志
struct LogEntry {
    // 创建该日志时的 领导者的任期号
    int term = 0;
    // 全局索引，通常从 1 开始递增
    // 它不是 logs_ 数组的下标
    int index = 0;
    // 状态机命令的序列化字节，raft 层只负责复制，不解析命令
    std::string command;
};

// 快照元数据
struct SnapshotMeta {
    // 快照覆盖到的最后一条日志索引
    // index <= lastIncludeIndex 的日志已经包含在快照中，可以被截断
    int lastIncludedIndex = 0;
    // 对应的任期号
    int lastIncludedTerm = 0;
};

// 领导者发送的日志复制/心跳请求
struct AppendEntriesArgs {
    // 领导者的任期号
    int term = 0;
    // 领导者的节点 id
    int leaderId = 0;
    // 本次 entries 前一条日志的全局索引
    // 用于日志一致性检查，entries 为空时用于心跳检查
    int prevLogIndex = 0;
    // 对应的任期号
    int prevLogTerm = 0;
    // 领导者已经提交的最新日志索引，也就是领导者的 commitIndex
    int leaderCommit = 0;
    // 本次需要复制的日志条目
    // 为空表示心跳；可以包含多条日志
    std::vector<LogEntry> entries;
};

// 跟随者对领导者 日志复制/心跳请求的回复
struct AppendEntriesReply {
    // 响应者当前的任期号，如果大于领导者的任期号，领导者退位
    int term = 0;
    // prevLogIndex/prevLogTerm 是否匹配，以及 entries 是否追加成功。
    bool success = false;

    // 响应者检查：我是否有 prevLogIndex 这条日志？如果有，它的 term 是否等于 prevLogTerm？
    /*
    情况一：
    prevLogIndex > follower.lastLogIndex，跟随者没有这条日志。
    此时：conflictTerm  = -1, conflictIndex = follower.lastLogIndex + 1
    意思是：我没有你要求的那条日志，你从我的 lastLogIndex + 1 开始重新发。

    情况二：
    follower.termAt(prevLogIndex) != prevLogTerm
    follower 有这条日志，但 term 不匹配
    此时：conflictTerm  = follower.termAt(prevLogIndex)， conflictIndex = follower 日志中这个 term 的第一条 index;
    意思是：我在 prevLogIndex 处有一条日志，但它属于 conflictTerm；这个 term 从 conflictIndex 开始。
    */ 
    int conflictTerm = -1; // -1 表示响应者没有 prevLogIndex 位置的日志。这是对应的任期号
    int conflictIndex = 0; // 
};


// 需要持久化的 raft 状态
struct PersistentState {
    // 当前任期号
    // 任期变化后，必须在发送回复 rpc 之前持久化
    int currentTerm = 0;
    // 当前任期为谁投了票，-1表示没有投票，任期变化时必须重置为 -1
    int votedFor = -1;

    // 2A 暂不持久化日志和快照；2B/2C 会扩展这里。
    std::vector<LogEntry> logs;
    SnapshotMeta snapshot;
};

// raft 推送给状态机（存储器）的消息
// commandValid 和 snapshotValid 应当二选一。
struct ApplyMsg {
    // 为true表示这是一条普通日志命令
    bool commandValid = false;
    // 命令的序列化字节，由状态机自行解析
    std::string command;
    // 命令对应的全局日志索引
    int commandIndex = 0;

    // true 表示这是一条快照安装消息。
    bool snapshotValid = false;
    // 快照的序列化字节。
    std::string snapshot;
    // 快照覆盖到的最后日志 index。
    int snapshotIndex = 0;
    // 快照最后日志的 term。
    int snapshotTerm = 0;
};
