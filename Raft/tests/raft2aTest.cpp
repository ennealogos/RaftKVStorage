#include "MemoryNetwork.h"
#include "MemoryPersister.h"
#include "Raft.h"

#include <chrono>
#include <iostream>
#include <map>
#include <memory>
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

void checkElectionSafety(const std::vector<std::unique_ptr<Raft>>& nodes) {
    std::map<int, int> leaderByTerm;

    for (const auto& node : nodes) {
        if (node->role() != Role::Leader) {
            continue;
        }

        const int term = node->currentTerm();
        const int id = node->leaderId();
        auto [it, inserted] = leaderByTerm.emplace(term, id);

        // 同一个 term 至多一个 leader
        if (!inserted) {
            CHECK(it->second == id);
        }
    }
}

int activeLeader(const MemoryNetwork& net,
                 const std::vector<std::unique_ptr<Raft>>& nodes) {
    for (int id = 0; id < static_cast<int>(nodes.size()); ++id) {
        if (!net.isDown(id) && nodes[id]->role() == Role::Leader) {
            return id;
        }
    }
    return -1;
}

// 多轮随机种子测试：专门抓“同一 term 出现两个 leader”和选举无法收敛。
void testElectionRounds(int rounds) {
    constexpr int kNodes = 5;

    for (int round = 0; round < rounds; ++round) {
        MemoryNetwork net;
        std::vector<std::unique_ptr<MemoryPersister>> persisters;
        std::vector<std::unique_ptr<Raft>> nodes;

        for (int id = 0; id < kNodes; ++id) {
            std::vector<int> peers;
            for (int other = 0; other < kNodes; ++other) {
                if (other != id) {
                    peers.push_back(other);
                }
            }

            persisters.push_back(std::make_unique<MemoryPersister>());

            RaftConfig config;
            config.electionTimeoutMinMs = 100;
            config.electionTimeoutMaxMs = 250;
            config.heartbeatMs = 50;
            config.seed = static_cast<std::uint32_t>(10000 + round * 100 + id);

            nodes.push_back(std::make_unique<Raft>(
                id, peers, &net, persisters.back().get(), config));
            net.addNode(id, nodes.back().get());
        }

        auto now = Raft::Clock::now();
        int leader = -1;

        for (int step = 0; step < 500; ++step) {
            now += std::chrono::milliseconds(10);
            net.tick(now);
            checkElectionSafety(nodes);

            leader = activeLeader(net, nodes);
            if (leader != -1) {
                break;
            }
        }

        CHECK(leader != -1);
    }
}

}  // namespace

int main() {
    // 多轮随机种子选举
    testElectionRounds(100);

    constexpr int kNodes = 5;

    MemoryNetwork net;
    std::vector<std::unique_ptr<MemoryPersister>> persisters;
    std::vector<std::unique_ptr<Raft>> nodes;

    for (int id = 0; id < kNodes; ++id) {
        std::vector<int> peers;
        for (int other = 0; other < kNodes; ++other) {
            if (other != id) {
                peers.push_back(other);
            }
        }

        persisters.push_back(std::make_unique<MemoryPersister>());

        RaftConfig config;
        config.electionTimeoutMinMs = 100;
        config.electionTimeoutMaxMs = 250;
        config.heartbeatMs = 50;
        config.seed = static_cast<std::uint32_t>(1000 + id);

        nodes.push_back(std::make_unique<Raft>(
            id, peers, &net, persisters.back().get(), config));
        net.addNode(id, nodes.back().get());
    }

    auto now = Raft::Clock::now();

    // 1. 正常选举：5 节点最终选出 leader
    int leader = -1;
    for (int step = 0; step < 1000; ++step) {
        now += std::chrono::milliseconds(10);
        net.tick(now);
        checkElectionSafety(nodes);

        leader = activeLeader(net, nodes);
        if (leader != -1) {
            break;
        }
    }
    CHECK(leader != -1);

    if (leader >= 0) {
        CHECK(persisters[leader]->saveCount() > 0);
    }

    // 2. kill leader，剩余节点应重新选主
    const int oldLeader = leader;
    net.down(oldLeader);

    bool electedNewLeader = false;
    for (int step = 0; step < 1000; ++step) {
        now += std::chrono::milliseconds(10);
        net.tick(now);
        checkElectionSafety(nodes);

        const int newLeader = activeLeader(net, nodes);
        if (newLeader != -1 && newLeader != oldLeader) {
            electedNewLeader = true;
            break;
        }
    }
    CHECK(electedNewLeader);

    // 3. 单节点集群应立即成为 leader
    {
        MemoryNetwork singleNet;
        MemoryPersister singlePersister;
        RaftConfig config;
        config.electionTimeoutMinMs = 50;
        config.electionTimeoutMaxMs = 100;
        config.heartbeatMs = 25;
        config.seed = 7;

        auto single = std::make_unique<Raft>(
            0, std::vector<int>{}, &singleNet, &singlePersister, config);
        singleNet.addNode(0, single.get());

        auto singleNow = Raft::Clock::now() + std::chrono::milliseconds(60);
        singleNet.tick(singleNow);
        CHECK(single->isLeader());
    }

    // 4. 更高 term 的 RequestVote 应让节点降级并持久化新 term/vote
    {
        int follower = -1;
        for (int id = 0; id < kNodes; ++id) {
            if (id != oldLeader && !net.isDown(id)) {
                follower = id;
                break;
            }
        }
        CHECK(follower != -1);

        if (follower >= 0) {
            RequestVoteArgs args;
            args.term = nodes[follower]->currentTerm() + 10;
            args.candidateId = 99;
            nodes[follower]->onRequestVote(99, args);

            CHECK(nodes[follower]->role() == Role::Follower);
            CHECK(nodes[follower]->currentTerm() == args.term);
            CHECK(persisters[follower]->state().currentTerm == args.term);
            CHECK(persisters[follower]->state().votedFor == 99);
        }
    }

    if (gFailures == 0) {
        std::cout << "ALL RAFT 2A TESTS PASSED (" << gChecks
                  << " checks)" << std::endl;
        return 0;
    }

    std::cerr << gFailures << " / " << gChecks << " checks FAILED" << std::endl;
    return 1;
}
