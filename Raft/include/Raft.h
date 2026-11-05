#pragma once

#include "Persister.h"
#include "RaftTypes.h"
#include "Transport.h"

#include <chrono>
#include <cstdint>
#include <mutex>
#include <random>
#include <set>
#include <vector>
#include <map>
#include <string>
#include <functional>

struct RaftConfig {
    // 选举超时下限：跟随者在超过这个时间后就可能发起选举。
    int electionTimeoutMinMs = 300;
    // 选举超时上限：与下限组成随机区间，避免多个节点同时发起选举
    int electionTimeoutMaxMs = 600;
    // 领导者隔多久发起一次心跳
    int heartbeatMs = 100;
    // 0 表示使用 random_device 自动生成种子
    std::uint32_t seed = 0;
};

// Raft 2A/2B：领导人选举、日志复制与提交。
//
// 设计约定：
//   - 所有 Raft 状态由 mu_ 保护；
//   - Transport 必须是异步/入队投递，不能在 sendXxx 内同步回调本对象；
//   - tick() 由外部定时器或测试器驱动；
//   - 2A 的 AppendEntries 只作为心跳，2B 开始携带日志并推进 commitIndex。
class Raft {
public:
    using Clock = std::chrono::steady_clock;
    using ApplyCallback = std::function<void(const ApplyMsg)>;

    Raft(int id,
         std::vector<int> peers,
         Transport* transport,
         Persister* persister,
         RaftConfig config = {});

    // 节点时钟，由外部周期调用，now 用于逻辑/真实时间
    void tick(Clock::time_point now);

    // 领导者收到客户端命令，向自己的日志数组追加日志，并向其他节点发起日志复制
    // 返回 true：日志已成功追加到 Leader 本地日志
    // 返回 false：未接受；此时 isLeader 表示当前是否仍为 Leader
    bool start(const std::string& command,
               int* index,
               int* term,
               bool* isLeader);

    // 设置提交回调
    void setApplyCallback(ApplyCallback callback);

    // 收到其他节点的 RPC
    // 收到其他节点的投票请求
    void onRequestVote(int from, const RequestVoteArgs& args);
    // 收到来自其他节点的投票
    void onRequestVoteReply(int from, const RequestVoteReply& reply);
    // 收到来自领导者的日志同步 / 心跳
    void onAppendEntries(int from, const AppendEntriesArgs& args);
    // 领导者收到来自其他节点对日志同步 / 心跳的回复
    void onAppendEntriesReply(int from, const AppendEntriesReply& reply);

    // 获取节点状态
    Role role() const;
    // 节点记录的任期号
    int currentTerm() const;
    // 节点记录的领导者 id
    int leaderId() const;
    // 节点是否为领导者
    bool isLeader() const;

private:
    // 角色转为竞选者，开始竞选，初始化投票集合，向每个节点发送拉票请求
    void startElectionLocked();
    // 角色转为跟随者，如果提供的任期号更大，更新自己的记录
    void becomeFollowerLocked(int term);
    // 角色转位领导者，并立即发送一轮心跳
    void becomeLeaderLocked();
    
    // 向其他节点发送 RPC
    // 向其他节点广播心跳
    void broadcastHeartbeatLocked();
    // 设置选举超时截止时间（竞选者） 或 发起选举的截止时间（跟随者）
    void resetElectionTimerLocked();
    // 向某节点发送 日志同步消息（领导者调用）
    void sendAppendEntriesLocked(int peer);

    // 统计多数派，推进 commitIndex
    void advanceCommitIndexLocked();
    // 把 [lastApplied+1, commitIndex] 推给状态机
    void applyCommittedLocked();

    // 日志下标工具
    // 在日志数组中查询全局 index 所属的任期（如果日志已经被压缩，返回 -1）
    int termAtLocked(int index) const;
    // 根据全局 index 在日志数组中查询对应的日志
    const LogEntry* logAtLocked(int index) const;
    // 根据提供的任期和全局索引，查找属于该任期的日志数组中的第一条日志的全局索引
    int firstIndexOfTermLocked(int term, int maxIndex) const;
    // 在日志数组中查找提供任期的最后一条日志的全局索引（返回 -1 表示未找到）
    int lastIndexOfTermLocked(int term) const;
    // 删除日志数组中 index 及其之后的所有日志
    void truncateLogFromLocked(int index);
    
    // 拉票的对应的节点是否有不晚于自己的状态（日志索引，任期号）
    bool upToDateLocked(const RequestVoteArgs& args) const;
    // 节点日志中最新的全局日志索引
    int lastLogIndexLocked() const;
    // 节点日志中最新的任期号
    int lastLogTermLocked() const;
    
    // 节点信息持久化
    void persistLocked();

    int id_;
    std::vector<int> peers_;
    Transport* transport_;
    Persister* persister_;

    // 领导者保存每个节点的日志复制进度
    std::map<int, int> nextIndex_; // 领导者下一次发给节点的日志全局索引 （可回退）
    std::map<int, int> matchIndex_; // 领导者已经确认的节点成功复制的日志索引（只增不减）

    // 领导者心跳相关
    int heartbeatMs_;
    // 一个均匀分布的整数生成器
    std::uniform_int_distribution<int> electionDist_;
    std::mt19937 rng_;

    mutable std::mutex mu_;

    Role role_ = Role::Follower;
    int currentTerm_ = 0;
    int votedFor_ = -1;
    int leaderId_ = -1;
    int commitIndex_ = 0;

    // 每个节点保留一个在途 AppendEntries，避免乱序回复
    struct PendingAppend {
        int prevLogIndex = 0;
        int entriesCount = 0;
        std::uint64_t requestId = 0;
        Clock::time_point sentAt{};
    };
    std::map<int, PendingAppend> pendingAppend_; // 每个节点对应的在途 AppendEntry
    std::uint64_t nextAppendRequestId_ = 1;
    int lastApplied_ = 0;
    ApplyCallback applyCallback_;

    // 2C 快照边界；2B 中为 0。
    int snapshotIndex_ = 0;
    int snapshotTerm_ = 0;
    std::vector<LogEntry> logs_;

    std::set<int> votes_;

    Clock::time_point now_; // 只在 tick() 内更新
    // 跟随者：超时未收到心跳就发起选举 / 竞选者：选举没成功，超时后重新选举
    Clock::time_point electionDeadline_; 
    Clock::time_point nextHeartbeat_; // 针对领导者：下次应该发送心跳的时间点
};
