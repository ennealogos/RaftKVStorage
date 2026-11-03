#include "Raft.h"

#include <algorithm>
#include <utility>

namespace {

int normalizedMinTimeout(int a, int b) {
    return std::max(1, std::min(a, b));
}

int normalizedMaxTimeout(int a, int b) {
    return std::max(1, std::max(a, b));
}

std::uint32_t normalizedSeed(std::uint32_t seed) {
    return seed == 0 ? std::random_device{}() : seed;
}

}  // namespace

Raft::Raft(int id,
           std::vector<int> peers,
           Transport* transport,
           Persister* persister,
           RaftConfig config)
    : id_(id),
      peers_(std::move(peers)),
      transport_(transport),
      persister_(persister),
      heartbeatMs_(std::max(1, config.heartbeatMs)),
      electionDist_(normalizedMinTimeout(config.electionTimeoutMinMs,
                                         config.electionTimeoutMaxMs),
                    normalizedMaxTimeout(config.electionTimeoutMinMs,
                                         config.electionTimeoutMaxMs)),
      rng_(normalizedSeed(config.seed)),
      now_(Clock::now()) {
    if (persister_ != nullptr) {
        PersistentState state;
        if (persister_->load(&state)) {
            currentTerm_ = state.currentTerm;
            votedFor_ = state.votedFor;
        }
    }

    resetElectionTimerLocked();
}

void Raft::tick(Clock::time_point now) {
    std::lock_guard<std::mutex> lock(mu_);
    now_ = now;

    if (role_ != Role::Leader) {
        // Follower 或 Candidate 超时后重新发起选举
        if (now_ >= electionDeadline_) {
            startElectionLocked();
        }
    } else if (now_ >= nextHeartbeat_) {
        // 角色是领导者，时间到了，发起心跳
        broadcastHeartbeatLocked();
    }
}

// 收到其他节点的投票请求
/*
对方的任期号更小，拒绝。
对方的任期号更大，状态退回跟随者。
本次任期还没投票，或者已经投给该竞选者，且竞选者日志不落后，为其投票
*/
void Raft::onRequestVote(int from, const RequestVoteArgs& args) {
    std::lock_guard<std::mutex> lock(mu_);

    RequestVoteReply reply;
    reply.term = currentTerm_;
    reply.voteGranted = false;

    // 旧 term 请求直接拒绝
    if (args.term < currentTerm_) {
        if (transport_ != nullptr) {
            transport_->sendRequestVoteReply(id_, from, reply);
        }
        return;
    }

    // 见到更高 term 立即降级，并清空本轮投票
    if (args.term > currentTerm_) {
        becomeFollowerLocked(args.term);
    }

    // 本轮还没投票，或已经投给该 candidate，且 candidate 日志不落后
    if ((votedFor_ == -1 || votedFor_ == args.candidateId) &&
        upToDateLocked(args)) {
        votedFor_ = args.candidateId;
        persistLocked();
        resetElectionTimerLocked();
        reply.voteGranted = true;
    }

    reply.term = currentTerm_;
    if (transport_ != nullptr) {
        transport_->sendRequestVoteReply(id_, from, reply);
    }
}

// 收到来自其他节点的投票
void Raft::onRequestVoteReply(int from, const RequestVoteReply& reply) {
    std::lock_guard<std::mutex> lock(mu_);

    // 对方记录的任期号更高，退回跟随者状态
    if (reply.term > currentTerm_) {
        becomeFollowerLocked(reply.term);
        return;
    }

    // 只统计当前 term 的有效投票
    if (role_ != Role::Candidate ||
        reply.term != currentTerm_ ||
        !reply.voteGranted) {
        return;
    }

    votes_.insert(from); // 不会存在重复票，使用了 set

    // 如果拿到了多数票，成功上位为领导者
    if (votes_.size() > (peers_.size() + 1) / 2) {
        becomeLeaderLocked();
    }
}

// 收到了来自领导者的日志同步 / 心跳
void Raft::onAppendEntries(int from, const AppendEntriesArgs& args) {
    std::lock_guard<std::mutex> lock(mu_);

    AppendEntriesReply reply;
    reply.term = currentTerm_;
    reply.success = false;

    // 对方的任期号更小，拒绝，发送自己记录的任期号，对方退回跟随者状态
    if (args.term < currentTerm_) {
        if (transport_ != nullptr) {
            transport_->sendAppendEntriesReply(id_, from, reply);
        }
        return;
    }

    // 对方任期号更高，检查自己的状态退回跟随者
    if (args.term > currentTerm_) {
        becomeFollowerLocked(args.term);
    }

    // 2A 阶段只当心跳处理：不检查 prevLogIndex / prevLogTerm
    role_ = Role::Follower;
    leaderId_ = args.leaderId;
    resetElectionTimerLocked(); // 重置选举计时器

    reply.term = currentTerm_;
    reply.success = true;
    if (transport_ != nullptr) {
        transport_->sendAppendEntriesReply(id_, from, reply);
    }
}

// 收到了来自其他节点对日志同步 / 心跳的回复
void Raft::onAppendEntriesReply(int, const AppendEntriesReply& reply) {
    std::lock_guard<std::mutex> lock(mu_);

    // 退回跟随者
    if (reply.term > currentTerm_) {
        becomeFollowerLocked(reply.term);
    }

    // 2A 不处理日志复制，成功/失败回复暂时忽略。
}

Role Raft::role() const {
    std::lock_guard<std::mutex> lock(mu_);
    return role_;
}

int Raft::currentTerm() const {
    std::lock_guard<std::mutex> lock(mu_);
    return currentTerm_;
}

int Raft::leaderId() const {
    std::lock_guard<std::mutex> lock(mu_);
    return leaderId_;
}

bool Raft::isLeader() const {
    std::lock_guard<std::mutex> lock(mu_);
    return role_ == Role::Leader;
}

void Raft::startElectionLocked() {
    // 自己的竞选任期号就是记录的最新任期号 + 1
    ++currentTerm_;
    // 角色转换为竞选者
    role_ = Role::Candidate;
    // 自己一定给自己投票
    votedFor_ = id_;
    // 竞选者不会有领导者了
    leaderId_ = -1;

    // 投票集合状态重置，并加入自己
    votes_.clear();
    votes_.insert(id_);

    // 持久化
    persistLocked();
    // 设置本次选举的截止时间，超时后选举失败
    resetElectionTimerLocked();

    // 单节点集群：自己就是多数派
    if (votes_.size() > (peers_.size() + 1) / 2) {
        becomeLeaderLocked();
        return;
    }

    // 创建拉票请求
    RequestVoteArgs args;
    args.term = currentTerm_;
    args.candidateId = id_;
    args.lastLogIndex = lastLogIndexLocked();
    args.lastLogTerm = lastLogTermLocked();

    // 为每个节点发送拉票请求
    for (int peer : peers_) {
        if (transport_ != nullptr) {
            transport_->sendRequestVote(id_, peer, args);
        }
    }
}


void Raft::becomeFollowerLocked(int term) {
    bool persistentChanged = false;

    // 如果输入的任期号大于自己记录的任期号，更新自己的记录
    if (term > currentTerm_) {
        currentTerm_ = term;
        votedFor_ = -1;
        persistentChanged = true;
    }

    // 角色转为跟随者
    role_ = Role::Follower;
    leaderId_ = -1;
    // 设置发起选举的deadline
    resetElectionTimerLocked();

    // 持久化
    if (persistentChanged) {
        persistLocked();
    }
}


void Raft::becomeLeaderLocked() {
    role_ = Role::Leader;
    leaderId_ = id_;
    votes_.clear();

    // 当选后立即发一轮心跳
    broadcastHeartbeatLocked();
}

void Raft::broadcastHeartbeatLocked() {
    AppendEntriesArgs args;
    args.term = currentTerm_;
    args.leaderId = id_;
    args.prevLogIndex = lastLogIndexLocked();
    args.prevLogTerm = lastLogTermLocked();
    args.leaderCommit = commitIndex_;
    // 2A 不携带日志；2B 再填充 entries。

    for (int peer : peers_) {
        if (transport_ != nullptr) {
            transport_->sendAppendEntries(id_, peer, args);
        }
    }

    nextHeartbeat_ = now_ + std::chrono::milliseconds(heartbeatMs_);
}

// 设置选举超时截止时间
void Raft::resetElectionTimerLocked() {
    electionDeadline_ =
        now_ + std::chrono::milliseconds(electionDist_(rng_));
}


bool Raft::upToDateLocked(const RequestVoteArgs& args) const {
    const int myIndex = lastLogIndexLocked();
    const int myTerm = lastLogTermLocked();

    if (args.lastLogTerm != myTerm) {
        return args.lastLogTerm > myTerm;
    }
    return args.lastLogIndex >= myIndex;
}

int Raft::lastLogIndexLocked() const {
    return snapshotIndex_ + static_cast<int>(logs_.size());
}

int Raft::lastLogTermLocked() const {
    if (logs_.empty()) {
        return snapshotTerm_;
    }
    return logs_.back().term;
}

void Raft::persistLocked() {
    if (persister_ == nullptr) {
        return;
    }

    PersistentState state;
    state.currentTerm = currentTerm_;
    state.votedFor = votedFor_;
    // 2A 不持久化日志/快照；2B/2C 在这里填入 state.logs / state.snapshot。
    persister_->save(state);
}
