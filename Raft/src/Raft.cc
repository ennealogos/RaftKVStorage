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
            logs_ = state.logs; // 2b 新增
        }
    }

    resetElectionTimerLocked();
}

void Raft::setApplyCallback(ApplyCallback callback) {
    std::lock_guard<std::mutex> lock(mu_);
    applyCallback_ = std::move(callback);

    // 如果设置回调前已经有提交日志，补应用到状态机。
    applyCommittedLocked();
}

// 仅领导者调用
// 领导者收到客户端命令，向自己的日志数组追加日志，并向其他节点发起日志复制
// 返回 true：日志已成功追加到 Leader 本地日志
// 返回 false：未接受；此时 isLeader 表示当前是否仍为 Leader
bool Raft::start(const std::string& command,
                 int* index,
                 int* term,
                 bool* isLeader) {
    // 加锁
    std::lock_guard<std::mutex> lock(mu_);

    if (isLeader != nullptr) {
        *isLeader = (role_ == Role::Leader);
    }
    // 其他节点调用返回 false
    if (role_ != Role::Leader) {
        return false;
    }

    // 创建日志
    LogEntry entry;
    entry.term = currentTerm_;
    entry.index = lastLogIndexLocked() + 1;
    entry.command = command;

    // 追加到本地日志数组
    logs_.push_back(std::move(entry));

    // 持久化
    persistLocked();

    // 这条新日志的返回信息
    if (index != nullptr) {
        *index = logs_.back().index;
    }
    if (term != nullptr) {
        *term = logs_.back().term;
    }

    // 单节点集群没有 peer 回复，需要在这里直接推进 commitIndex。
    // 多节点时 matchIndex_ 还未更新，这里通常不会提交。
    advanceCommitIndexLocked();

    // 向所有节点发起日志复制请求
    for (int peer : peers_) {
        sendAppendEntriesLocked(peer);
    }
    return true;
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

// 向某节点发送日志复制请求
void Raft::sendAppendEntriesLocked(int peer) {
    // 角色必须是领导者
    if (role_ != Role::Leader) {
        return;
    }

    // 每个节点同一时间只允许一个在途请求；
    // 但超过一个心跳周期还没回复时，认为请求丢失，允许重发。
    auto pendingIt = pendingAppend_.find(peer);
    if (pendingIt != pendingAppend_.end()) {
        const auto retryAfter =
            std::chrono::milliseconds(std::max(1, heartbeatMs_));
        if (now_ - pendingIt->second.sentAt < retryAfter) {
            return;
        }
        pendingAppend_.erase(pendingIt);
    }

    auto nextIt = nextIndex_.find(peer);

    // 如果领导者没有保存对应节点的日志同步进度，将本次请求加入
    if (nextIt == nextIndex_.end()) {
        // 根据自己记录的索引生成正确的索引，并加入集合
        nextIndex_[peer] = lastLogIndexLocked() + 1;
        nextIt = nextIndex_.find(peer);
    }

    int nextIndex = std::max(1, nextIt->second);
    const int lastIndex = lastLogIndexLocked(); // 领导者最新的日志索引

    // 说明对应节点的记录与领导者不一致，日志同步请求按照领导者的来发送
    if (nextIndex > lastIndex + 1) {
        nextIndex = lastIndex + 1;
        nextIndex_[peer] = nextIndex;
    }

    // 构造日志同步请求
    AppendEntriesArgs args;
    args.term = currentTerm_;
    args.leaderId = id_;
    args.prevLogIndex = nextIndex - 1;
    args.prevLogTerm = termAtLocked(args.prevLogIndex);
    args.leaderCommit = commitIndex_;
    args.requestId = nextAppendRequestId_++;

    // 可以一次性发送多条日志
    for (int index = nextIndex; index <= lastIndex; ++ index) {
        // 从领导者的日志数组中获取对应的日志
        const LogEntry* entry = logAtLocked(index);
        if (entry == nullptr) break;

        args.entries.push_back(*entry);
    }

    // 更新记录的对方节点的在途请求
    PendingAppend pending;
    pending.prevLogIndex = args.prevLogIndex;
    pending.entriesCount = static_cast<int>(args.entries.size());
    pending.requestId = args.requestId;
    pending.sentAt = now_;
    pendingAppend_[peer] = pending;

    // 通过 transport 发送
    if (transport_ != nullptr) {
        transport_->sendAppendEntries(id_, peer, args);
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
    reply.requestId = args.requestId;

    // 旧 term 请求，对方的任期号更小，拒绝，发送自己记录的任期号，对方退回跟随者状态
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

    // 2b 阶段，检查日志是否一致，回送不一致信息，追加日志到数组中，
    // 跟随领导者推进 commitIndex 并应用到状态机
    role_ = Role::Follower;
    leaderId_ = args.leaderId;
    resetElectionTimerLocked(); // 重置选举计时器

    // 1. 跟随者的日志太短，需要领导者先发送之前的日志
    const int lastIndex = lastLogIndexLocked();
    if (args.prevLogIndex > lastIndex) {
        reply.conflictTerm = -1;
        reply.conflictIndex = lastIndex + 1;

        if (transport_ != nullptr) {
            transport_->sendAppendEntriesReply(id_, from, reply);
        }
        return;
    }

    // 2. 跟随者有 prevLogIndex 对应的日志，但是其所属的任期不匹配
    // 回送冲突的任期号与该任期在日志数组中的第一条日志
    if (args.prevLogIndex > snapshotIndex_ &&
        termAtLocked(args.prevLogIndex) != args.prevLogTerm) {
        const int conflictTerm = termAtLocked(args.prevLogIndex);
        reply.conflictTerm = conflictTerm;
        reply.conflictIndex = firstIndexOfTermLocked(conflictTerm, args.prevLogIndex);
        if (transport_ != nullptr) {
            transport_->sendAppendEntriesReply(id_, from, reply);
        }
         return;
    }

    // 3. 逐条检查并追加日志
    bool changed = false;

    for (std::size_t i = 0; i < args.entries.size(); ++i) {
        // 计算第一条日志的索引
        const int index = args.prevLogIndex + 1 + static_cast<int>(i);

        // 跟随者已经有该条日志
        if (index <= lastLogIndexLocked()) {
            // index + term 相同，跳过该条日志
            if (termAtLocked(index) == args.entries[i].term) {
                continue;
            }

            // term 不同，冲突，截断跟随者日志数组中从 index 开始的旧日志
            truncateLogFromLocked(index);
            changed = true;
        }

        // 没有该条日志，追加到日志数组
        LogEntry entry = args.entries[i];
        entry.index = index;
        logs_.push_back(std::move(entry));
        changed = true;
    }

    // 日志变化后，必须先持久化再回复领导者
    if (changed) {
        persistLocked();
    }

    // 4. 更新跟随者的 commitIndex
    // 如果发现领导者的 commitIndex 进行了推进，跟随者也推进 commitIndex 并应用到状态机
    if (args.leaderCommit > commitIndex_) {
        // 不能超过本地的最新日志索引
        commitIndex_ = std::min(args.leaderCommit, lastLogIndexLocked());
        // 应用提交
        applyCommittedLocked();
    }

    reply.term = currentTerm_;
    reply.success = true;
    if (transport_ != nullptr) {
        transport_->sendAppendEntriesReply(id_, from, reply);
    }
}

// 收到了来自其他节点对日志同步 / 心跳的回复
/*
收到回复后：
1. 检查 term
2. 找到这个 peer 的在途请求 pendingAppend_
3. 从 pendingAppend_ 中删除它
4. 如果成功：
      更新 matchIndex_ / nextIndex_
      尝试推进 commitIndex_
   如果失败：
      根据 conflictTerm/conflictIndex 回退 nextIndex_
5. 立即给这个 peer 发下一批 AppendEntries
*/
void Raft::onAppendEntriesReply(int from, const AppendEntriesReply& reply) {
    std::lock_guard<std::mutex> lock(mu_);

    // 退回跟随者
    if (reply.term > currentTerm_) {
        becomeFollowerLocked(reply.term);
        pendingAppend_.clear();
        return;
    }

    // 调用者不是领导者 或 任期不一致（可能遭遇了故障，导致任期已经过去，直接返回）
    if (role_ != Role::Leader || reply.term != currentTerm_) {
        return;
    }

    auto pendingIt = pendingAppend_.find(from);
    if (pendingIt == pendingAppend_.end()) {
        // 在映射表中找不到发给 from 对应的请求，说明请求过期或者这是一个重复请求
        return;
    }

    const PendingAppend pending = pendingIt->second;

    // 迟到/旧请求的回复不能用来更新最新进度
    if (pending.requestId != reply.requestId) {
        return;
    }

    pendingAppend_.erase(pendingIt);  // 收到回复后将其从在途映射表中删除

    if (reply.success) {
        // 同步请求被跟随者确认
        const int newMatch = pending.prevLogIndex + pending.entriesCount;
        matchIndex_[from] = std::max(matchIndex_[from], newMatch); // 防御性编程
        nextIndex_[from] = matchIndex_[from] + 1;
        advanceCommitIndexLocked(); // 检查请求是否收到了多数派的确认，若是，推进 commitIndex

        // 只有在还有后续日志时才立即续发。
        // 心跳成功回复后不能再发心跳，否则测试网络一次 drain 会无限来回。
        if (nextIndex_[from] > lastLogIndexLocked()) {
            return;
        }
    } else {
        // 请求没有被确认，对方日志出现了冲突
        int nextIndex = 0;

        // 跟随者日志太短，需要同步之前的日志
        if (reply.conflictTerm == -1) {
            nextIndex = reply.conflictIndex;
        } else {
            // 存在对应索引的日志，但是其所属任期号不一致
            // 应该是跟随者对应 index 的任期号更小（更大领导者状态回退），所以领导者对应任期号的最后一条日志应该与跟随者一致
            // 应该发送的就是领导者的 lastIndexOfTerm + 1
            const int lastIndexOfTerm = lastIndexOfTermLocked(reply.conflictTerm);

            if (lastIndexOfTerm != -1) {
                nextIndex = lastIndexOfTerm + 1;
            } else {
                nextIndex = reply.conflictIndex;
            }
        }
        nextIndex_[from] = std::max(1, nextIndex);
    }

    // 失败回退后立即重发；成功且有后续日志也立即续发。
    sendAppendEntriesLocked(from);
}

// 统计多数派，推进 commitIndex
void Raft::advanceCommitIndexLocked() {
    const int majority = (static_cast<int>(peers_.size()) + 1) / 2 + 1;

    for (int index = commitIndex_ + 1; index <= lastLogIndexLocked(); ++index) {
        int count = 1; // 来自领导者自己的一票

        for (int peer : peers_) {
            if (matchIndex_[peer] >= index) {
                ++ count; // 说明日志已经被其他节点确认了
            }
        }

        // 只能提交当前任期的日志
        if (count >= majority && termAtLocked(index) == currentTerm_) {
            commitIndex_ = index;
        }
    }

    // 应用到状态机
    applyCommittedLocked();
}

// 把 [lastApplied+1, commitIndex] 推给状态机
void Raft::applyCommittedLocked() {
    // 没有回调时不要把 lastApplied_ 推过已提交日志，否则之后设置回调会丢日志
    if (!applyCallback_) {
        return;
    }

    while (lastApplied_ < commitIndex_) {
        int newApply = lastApplied_ + 1;

        const LogEntry* entry = logAtLocked(newApply);
        if (entry == nullptr) {
            break;
        }

        ApplyMsg msg;
        msg.commandValid = true; // 这是一条普通日志命令
        msg.command = entry->command;
        msg.commandIndex = entry->index;

        if (applyCallback_) {
            // 回调只负责入队，不能在回调中再次调用 Raft，否则可能死锁
            applyCallback_(msg);
            lastApplied_ = newApply;
        }
    }
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
    pendingAppend_.clear();
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

    nextIndex_.clear();
    matchIndex_.clear();
    pendingAppend_.clear();
    nextAppendRequestId_ = 1;

    const int lastIndex = lastLogIndexLocked();
    for (int peer : peers_) {
        nextIndex_[peer] = lastIndex + 1;
        matchIndex_[peer] = 0;
    }

    // 当选后立即发一轮心跳
    broadcastHeartbeatLocked();
}

void Raft::broadcastHeartbeatLocked() {

    for (int peer : peers_) {
        sendAppendEntriesLocked(peer);
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
    state.logs = logs_;
    // 2C 快照暂未实现；2B 先持久化日志。
    persister_->save(state);
}


// --------------- 日志下标工具 -------------------
// 查询日志全局 index 所属的任期
int Raft::termAtLocked(int index) const {
    // 日志起点（0 表示第一条日志之前）的哨兵任期
    if (index == 0) {
        return 0;
    }

    // 当前 index 已经被快照覆盖，日志数组中没有这条日志，用快照边界 term 表示
    if (index <= snapshotIndex_) {
        // 当 index == snapShotIndex_ 时正确，但是 < snapShotIndex_ 时，日志已经被压缩，不可查
        // 需要在后续阶段处理
        return snapshotTerm_;
    }
    // 将快照全局 index 转换为 logs_ 数组的下标
    const int arrayIndex = index - snapshotIndex_ - 1;

    // 越界检查：index 太小 或超过数组中最后一条日志都返回 -1 ，表示数组中没有该日志
    if (arrayIndex < 0 || arrayIndex >= static_cast<int>(logs_.size())) {
        return -1;
    }

    return logs_[static_cast<std::size_t>(arrayIndex)].term;
}

// 根据全局 index 在日志数组中查询对应的日志
const LogEntry* Raft::logAtLocked(int index) const {
    // 日志已经被压缩，不再日志数组中
    if (index <= snapshotIndex_) {
        return nullptr;
    }

    // 计算对应日志在日志数组中的下标
    const int arrayIndex = index - snapshotIndex_ - 1;
    // 越界检查
    if (arrayIndex < 0 || arrayIndex >= static_cast<int>(logs_.size())) {
        return nullptr;
    }

    return &logs_[static_cast<std::size_t>(arrayIndex)];
}

// 根据提供的任期和全局索引，查找属于该任期的日志数组中的第一条日志的全局索引
int Raft::firstIndexOfTermLocked(int term, int maxIndex) const {
    int index = maxIndex;

    // 当 index == snapshotIndex_ + 1 时，正好到达日志数组的第一条日志
    while (index > snapshotIndex_ + 1 && termAtLocked(index - 1) == term) {
        -- index;
    }

    return index;
}

// 在日志数组中查找提供任期的最后一条日志的全局索引
int Raft::lastIndexOfTermLocked(int term) const {
    for (int index = lastLogIndexLocked(); index > snapshotIndex_; -- index) {
        if (termAtLocked(index) == term) {
            return index;
        }
    }
    return -1;
}


// 删除日志数组中 index 及其之后的所有日志
void Raft::truncateLogFromLocked(int index) {
    if (index <= snapshotIndex_) {
        return; // 已经被快照覆盖的日志无法删除
    }

    // 计算保留的日志数量 = index 在日志数组中的索引
    const int newSize = index - snapshotIndex_ - 1;
    if (newSize >= 0 && newSize < static_cast<int>(logs_.size())) {
        logs_.resize(static_cast<std::size_t>(newSize));
    }
}
