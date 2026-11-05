#include "MemoryNetwork.h"
#include "MemoryPersister.h"
#include "Raft.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <set>
#include <string>
#include <vector>

namespace {

int gChecks = 0;
int gFailures = 0;

#define CHECK(cond)                                                     \
    do {                                                                \
        ++gChecks;                                                      \
        if (!(cond)) {                                                  \
            std::cerr << "FAILED: " #cond " at " << __FILE__ << ":"     \
                      << __LINE__ << std::endl;                         \
            ++gFailures;                                                \
        }                                                               \
    } while (0)

bool contains(const std::vector<std::string>& commands,
              const std::string& command) {
    return std::find(commands.begin(), commands.end(), command) !=
           commands.end();
}

int countOf(const std::vector<std::string>& commands,
            const std::string& command) {
    return static_cast<int>(
        std::count(commands.begin(), commands.end(), command));
}

class CaptureTransport : public Transport {
public:
    std::vector<AppendEntriesReply> replies;

    void sendRequestVote(int, int, const RequestVoteArgs&) override {}
    void sendRequestVoteReply(int, int, const RequestVoteReply&) override {}
    void sendAppendEntries(int, int, const AppendEntriesArgs&) override {}

    void sendAppendEntriesReply(int,
                                int,
                                const AppendEntriesReply& reply) override {
        replies.push_back(reply);
    }
};

struct Cluster {
    MemoryNetwork net;
    std::vector<std::unique_ptr<MemoryPersister>> persisters;
    std::vector<std::unique_ptr<Raft>> nodes;
    std::vector<std::vector<std::string>> applied;
    Raft::Clock::time_point now = Raft::Clock::now();

    explicit Cluster(int nodeCount, std::uint32_t baseSeed = 5000) {
        applied.resize(static_cast<std::size_t>(nodeCount));
        persisters.reserve(static_cast<std::size_t>(nodeCount));
        nodes.reserve(static_cast<std::size_t>(nodeCount));

        for (int id = 0; id < nodeCount; ++id) {
            std::vector<int> peers;
            for (int other = 0; other < nodeCount; ++other) {
                if (other != id) {
                    peers.push_back(other);
                }
            }

            persisters.push_back(std::make_unique<MemoryPersister>());

            RaftConfig config;
            config.electionTimeoutMinMs = 100;
            config.electionTimeoutMaxMs = 250;
            config.heartbeatMs = 50;
            config.seed = baseSeed + static_cast<std::uint32_t>(id);

            nodes.push_back(std::make_unique<Raft>(
                id, peers, &net, persisters.back().get(), config));

            // 必须在第一次 tick 前设置，否则已提交日志会被认为已 apply。
            nodes.back()->setApplyCallback(
                [this, id](const ApplyMsg& msg) {
                    if (msg.commandValid) {
                        applied[static_cast<std::size_t>(id)]
                            .push_back(msg.command);
                    }
                });

            net.addNode(id, nodes.back().get());
        }
    }

    void tick(int steps) {
        for (int i = 0; i < steps; ++i) {
            now += std::chrono::milliseconds(10);
            net.tick(now);
        }
    }

    int leader() const {
        for (int id = 0; id < static_cast<int>(nodes.size()); ++id) {
            if (!net.isDown(id) && nodes[id]->isLeader()) {
                return id;
            }
        }
        return -1;
    }

    bool elect(int maxSteps = 500) {
        for (int step = 0; step < maxSteps; ++step) {
            tick(1);
            if (leader() != -1) {
                return true;
            }
        }
        return false;
    }

    int countApplied(int id, const std::string& command) const {
        return countOf(applied[static_cast<std::size_t>(id)], command);
    }

    bool hasApplied(int id, const std::string& command) const {
        return contains(applied[static_cast<std::size_t>(id)], command);
    }
};

void testBasicReplication() {
    Cluster cluster(5, 5000);
    CHECK(cluster.elect());

    const int leader = cluster.leader();
    CHECK(leader != -1);

    int index = 0;
    int term = 0;
    bool isLeader = false;
    CHECK(cluster.nodes[leader]->start("cmd-1", &index, &term, &isLeader));
    CHECK(isLeader);
    CHECK(index == 1);
    CHECK(term > 0);

    cluster.tick(300);

    for (int id = 0; id < 5; ++id) {
        CHECK(cluster.countApplied(id, "cmd-1") == 1);
    }

    // 非 Leader 不能接受 Start
    const int follower = (leader + 1) % 5;
    int ignoredIndex = 0;
    int ignoredTerm = 0;
    bool followerIsLeader = true;
    CHECK(!cluster.nodes[follower]->start(
        "wrong", &ignoredIndex, &ignoredTerm, &followerIsLeader));
    CHECK(!followerIsLeader);
}

void testMissingPrevLogReply() {
    CaptureTransport transport;

    RaftConfig config;
    config.seed = 7;

    Raft raft(0, {}, &transport, nullptr, config);

    AppendEntriesArgs args;
    args.term = 1;
    args.leaderId = 1;
    args.prevLogIndex = 1;
    args.prevLogTerm = 1;

    raft.onAppendEntries(1, args);

    // 必须只返回一个失败回复，不能继续 append 后再返回成功
    CHECK(transport.replies.size() == 1);
    if (!transport.replies.empty()) {
        CHECK(!transport.replies[0].success);
        CHECK(transport.replies[0].conflictTerm == -1);
        CHECK(transport.replies[0].conflictIndex == 1);
    }
}

void testConflictTruncationAndApply() {
    CaptureTransport transport;

    MemoryPersister persister;
    PersistentState state;
    state.currentTerm = 1;
    state.votedFor = -1;
    state.logs.push_back(LogEntry{1, 1, "old"});
    CHECK(persister.save(state));

    RaftConfig config;
    config.seed = 7;

    Raft raft(0, {}, &transport, &persister, config);

    std::vector<std::string> applied;
    raft.setApplyCallback([&applied](const ApplyMsg& msg) {
        if (msg.commandValid) {
            applied.push_back(msg.command);
        }
    });

    AppendEntriesArgs args;
    args.term = 2;
    args.leaderId = 99;
    args.prevLogIndex = 0;
    args.prevLogTerm = 0;
    args.leaderCommit = 1;
    args.requestId = 42;
    args.entries.push_back(LogEntry{2, 1, "new"});

    raft.onAppendEntries(99, args);

    CHECK(transport.replies.size() == 1);
    if (!transport.replies.empty()) {
        CHECK(transport.replies[0].success);
        CHECK(transport.replies[0].requestId == 42);
    }

    CHECK(raft.role() == Role::Follower);
    CHECK(raft.currentTerm() == 2);
    CHECK(applied.size() == 1);
    if (applied.size() == 1) {
        CHECK(applied[0] == "new");
    }
    CHECK(!contains(applied, "old"));
}

void testLostFollowerRetry() {
    Cluster cluster(5, 7000);
    CHECK(cluster.elect());

    const int leader = cluster.leader();
    CHECK(leader != -1);

    const int victim = (leader + 1) % 5;
    cluster.net.down(victim);

    int index = 0;
    int term = 0;
    bool isLeader = false;
    CHECK(cluster.nodes[leader]->start("cmd-retry", &index, &term, &isLeader));
    CHECK(isLeader);

    // 多数派仍然可以提交
    cluster.tick(300);
    CHECK(!cluster.hasApplied(victim, "cmd-retry"));

    // 恢复 victim；leader 应该在心跳超时后重发，而不是一直卡在 pending
    cluster.net.up(victim);
    cluster.tick(500);

    CHECK(cluster.countApplied(victim, "cmd-retry") == 1);
}

void testLeaderFailoverKeepsCommittedLog() {
    Cluster cluster(5, 9000);
    CHECK(cluster.elect());

    const int oldLeader = cluster.leader();
    CHECK(oldLeader != -1);

    int index = 0;
    int term = 0;
    bool isLeader = false;
    CHECK(cluster.nodes[oldLeader]->start(
        "before-failover", &index, &term, &isLeader));

    cluster.tick(300);

    for (int id = 0; id < 5; ++id) {
        CHECK(cluster.countApplied(id, "before-failover") == 1);
    }

    cluster.net.down(oldLeader);
    CHECK(cluster.elect());

    const int newLeader = cluster.leader();
    CHECK(newLeader != -1);
    CHECK(newLeader != oldLeader);

    CHECK(cluster.nodes[newLeader]->start(
        "after-failover", &index, &term, &isLeader));

    cluster.tick(300);

    for (int id = 0; id < 5; ++id) {
        if (id == oldLeader) {
            continue;
        }
        CHECK(cluster.hasApplied(id, "before-failover"));
        CHECK(cluster.hasApplied(id, "after-failover"));
    }
}

void testConflictTermReplyAndIdempotentAppend() {
    CaptureTransport transport;

    MemoryPersister persister;
    PersistentState state;
    state.currentTerm = 3;
    state.votedFor = -1;
    state.logs.push_back(LogEntry{1, 1, "a"});
    state.logs.push_back(LogEntry{1, 2, "b"});
    state.logs.push_back(LogEntry{2, 3, "c"});
    CHECK(persister.save(state));

    RaftConfig config;
    config.seed = 7;

    Raft raft(0, {}, &transport, &persister, config);

    std::vector<std::string> applied;
    raft.setApplyCallback([&applied](const ApplyMsg& msg) {
        if (msg.commandValid) {
            applied.push_back(msg.command);
        }
    });

    // 索引 3 实际上属于 term 2，却声明 term 1；应返回冲突任期与首条冲突索引。
    AppendEntriesArgs conflictArgs;
    conflictArgs.term = 3;
    conflictArgs.leaderId = 99;
    conflictArgs.prevLogIndex = 3;
    conflictArgs.prevLogTerm = 1;
    conflictArgs.requestId = 11;

    raft.onAppendEntries(99, conflictArgs);

    CHECK(transport.replies.size() == 1);
    if (!transport.replies.empty()) {
        CHECK(!transport.replies[0].success);
        CHECK(transport.replies[0].conflictTerm == 2);
        CHECK(transport.replies[0].conflictIndex == 3);
        CHECK(transport.replies[0].requestId == 11);
    }

    // 失败回复不能修改本地日志。
    CHECK(persister.state().logs.size() == 3);
    CHECK(applied.empty());

    AppendEntriesArgs appendArgs;
    appendArgs.term = 3;
    appendArgs.leaderId = 99;
    appendArgs.prevLogIndex = 3;
    appendArgs.prevLogTerm = 2;
    appendArgs.leaderCommit = 4;
    appendArgs.requestId = 12;
    appendArgs.entries.push_back(LogEntry{3, 4, "d"});

    raft.onAppendEntries(99, appendArgs);

    CHECK(transport.replies.size() == 2);
    if (transport.replies.size() == 2) {
        CHECK(transport.replies[1].success);
        CHECK(transport.replies[1].requestId == 12);
    }
    CHECK(applied.size() == 4);
    CHECK(applied == std::vector<std::string>({"a", "b", "c", "d"}));

    // 同一个 AppendEntries 重放应保持幂等，不得重复 apply。
    raft.onAppendEntries(99, appendArgs);
    CHECK(transport.replies.size() == 3);
    if (transport.replies.size() == 3) {
        CHECK(transport.replies[2].success);
    }
    CHECK(applied.size() == 4);
    CHECK(persister.state().logs.size() == 4);
}

void testPartitionHealsAndOverwritesUncommitted() {
    Cluster cluster(5, 11000);
    CHECK(cluster.elect());

    const int oldLeader = cluster.leader();
    CHECK(oldLeader != -1);

    const int minorityFollower = (oldLeader + 1) % 5;
    std::set<int> minority{oldLeader, minorityFollower};
    std::set<int> majority;
    for (int id = 0; id < 5; ++id) {
        if (minority.count(id) == 0) {
            majority.insert(id);
        }
    }

    cluster.net.partition(minority, majority);

    // 旧 leader 只能写少数派，永远无法提交这条日志。
    int index = 0;
    int term = 0;
    bool isLeader = false;
    CHECK(cluster.nodes[oldLeader]->start(
        "uncommitted-minority", &index, &term, &isLeader));
    CHECK(isLeader);

    cluster.tick(300);
    CHECK(!cluster.hasApplied(oldLeader, "uncommitted-minority"));

    // 多数派应选出新 leader，并能正常提交。
    int newLeader = -1;
    for (int step = 0; step < 500 && newLeader == -1; ++step) {
        cluster.tick(1);
        for (int id : majority) {
            if (cluster.nodes[id]->isLeader()) {
                newLeader = id;
                break;
            }
        }
    }
    CHECK(newLeader != -1);
    CHECK(newLeader != oldLeader);

    CHECK(cluster.nodes[newLeader]->start(
        "committed-majority", &index, &term, &isLeader));
    cluster.tick(300);

    // 恢复网络，旧 leader 应退位，未提交日志应被新 leader 覆盖。
    cluster.net.clearPartition();
    cluster.tick(800);

    for (int id = 0; id < 5; ++id) {
        CHECK(cluster.countApplied(id, "committed-majority") == 1);
        CHECK(cluster.countApplied(id, "uncommitted-minority") == 0);
    }
}

void testMultipleCommandsOrdering() {
    Cluster cluster(5, 13000);
    CHECK(cluster.elect());

    const int leader = cluster.leader();
    CHECK(leader != -1);

    constexpr int kCommandCount = 20;
    for (int i = 1; i <= kCommandCount; ++i) {
        int index = 0;
        int term = 0;
        bool isLeader = false;
        const std::string command = "cmd-" + std::to_string(i);
        CHECK(cluster.nodes[leader]->start(command, &index, &term, &isLeader));
        CHECK(isLeader);
        CHECK(index == i);
    }

    cluster.tick(500);

    for (int id = 0; id < 5; ++id) {
        const auto& commands = cluster.applied[static_cast<std::size_t>(id)];
        CHECK(commands.size() == kCommandCount);
        if (commands.size() == kCommandCount) {
            for (int i = 1; i <= kCommandCount; ++i) {
                CHECK(commands[static_cast<std::size_t>(i - 1)] ==
                      "cmd-" + std::to_string(i));
            }
        }
    }
}

void testSingleNodeCommitAndLateApplyCallback() {
    CaptureTransport transport;

    RaftConfig config;
    config.electionTimeoutMinMs = 50;
    config.electionTimeoutMaxMs = 100;
    config.heartbeatMs = 25;
    config.seed = 7;

    Raft raft(0, {}, &transport, nullptr, config);

    // 先不设置回调，验证单节点 start 不会丢 apply 进度
    auto now = Raft::Clock::now() + std::chrono::milliseconds(100);
    raft.tick(now);
    CHECK(raft.isLeader());

    int index = 0;
    int term = 0;
    bool isLeader = false;
    CHECK(raft.start("single-node", &index, &term, &isLeader));
    CHECK(isLeader);
    CHECK(index == 1);

    std::vector<std::string> applied;
    raft.setApplyCallback([&applied](const ApplyMsg& msg) {
        if (msg.commandValid) {
            applied.push_back(msg.command);
        }
    });

    CHECK(applied.size() == 1);
    if (applied.size() == 1) {
        CHECK(applied[0] == "single-node");
    }
}

}  // namespace

int main() {
    testBasicReplication();
    testMissingPrevLogReply();
    testConflictTruncationAndApply();
    testConflictTermReplyAndIdempotentAppend();
    testLostFollowerRetry();
    testLeaderFailoverKeepsCommittedLog();
    testPartitionHealsAndOverwritesUncommitted();
    testMultipleCommandsOrdering();
    testSingleNodeCommitAndLateApplyCallback();

    if (gFailures == 0) {
        std::cout << "ALL RAFT 2B TESTS PASSED (" << gChecks
                  << " checks)" << std::endl;
        return 0;
    }

    std::cerr << gFailures << " / " << gChecks << " checks FAILED" << std::endl;
    return 1;
}
