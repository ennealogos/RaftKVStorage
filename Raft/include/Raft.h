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

// Raft 2A：领导人选举。
//
// 设计约定：
//   - 所有 Raft 状态由 mu_ 保护；
//   - Transport 必须是异步/入队投递，不能在 sendXxx 内同步回调本对象；
//   - tick() 由外部定时器或测试器驱动；
//   - 2A 阶段日志为空，AppendEntries 只作为心跳。
class Raft {
public:
    using Clock = std::chrono::steady_clock;

    Raft(int id,
         std::vector<int> peers,
         Transport* transport,
         Persister* persister,
         RaftConfig config = {});

    // 节点时钟，由外部周期调用，now 用于逻辑/真实时间
    void tick(Clock::time_point now);

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
    // 开始竞选，角色转为竞选者，初始化投票集合，向每个节点发送拉票请求
    void startElectionLocked();
    // 角色转为跟随者，如果提供的任期号更大，更新自己的记录
    void becomeFollowerLocked(int term);
    // 角色转位领导者，并立即发送一轮心跳
    void becomeLeaderLocked();
    
    // 向其他节点广播心跳
    void broadcastHeartbeatLocked();
    // 设置选举超时截止时间（竞选者） 或 发起选举的截止时间（跟随者）
    void resetElectionTimerLocked();
    
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

    // 2A 日志为空；保留下标字段，2B/2D 直接扩展。
    int snapshotIndex_ = 0;
    int snapshotTerm_ = 0;
    std::vector<LogEntry> logs_;

    std::set<int> votes_;

    Clock::time_point now_; // 只在 tick() 内更新
    // 跟随者：超时未收到心跳就发起选举 / 竞选者：选举没成功，超时后重新选举
    Clock::time_point electionDeadline_; 
    Clock::time_point nextHeartbeat_; // 针对领导者：下次应该发送心跳的时间点
};
