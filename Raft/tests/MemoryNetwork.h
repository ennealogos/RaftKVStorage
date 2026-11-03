#pragma once

#include "Raft.h"
#include "Transport.h"

#include <functional>
#include <map>
#include <set>
#include <utility>
#include <vector>

// 内存网络：消息先入队，稍后投递。
// 这样 Raft 在持锁发送消息时不会同步回调自己，避免重入死锁。
class MemoryNetwork : public Transport {
public:
    using Clock = Raft::Clock;

    void addNode(int id, Raft* node) {
        nodes_[id] = node;
    }

    void down(int id) {
        down_.insert(id);
    }

    void up(int id) {
        down_.erase(id);
    }

    bool isDown(int id) const {
        return down_.find(id) != down_.end();
    }

    // a 组和 b 组之间模拟分区
    void partition(std::set<int> a, std::set<int> b) {
        groupA_ = std::move(a);
        groupB_ = std::move(b);
        partitioned_ = true;
    }

    void clearPartition() {
        groupA_.clear();
        groupB_.clear();
        partitioned_ = false;
    }

    void sendRequestVote(int from,
                         int to,
                         const RequestVoteArgs& args) override {
        enqueue(from, to, [this, from, to, args] {
            auto it = nodes_.find(to);
            if (it != nodes_.end()) {
                it->second->onRequestVote(from, args);
            }
        });
    }

    void sendRequestVoteReply(int from,
                              int to,
                              const RequestVoteReply& reply) override {
        enqueue(from, to, [this, from, to, reply] {
            auto it = nodes_.find(to);
            if (it != nodes_.end()) {
                it->second->onRequestVoteReply(from, reply);
            }
        });
    }

    void sendAppendEntries(int from,
                           int to,
                           const AppendEntriesArgs& args) override {
        enqueue(from, to, [this, from, to, args] {
            auto it = nodes_.find(to);
            if (it != nodes_.end()) {
                it->second->onAppendEntries(from, args);
            }
        });
    }

    void sendAppendEntriesReply(int from,
                                int to,
                                const AppendEntriesReply& reply) override {
        enqueue(from, to, [this, from, to, reply] {
            auto it = nodes_.find(to);
            if (it != nodes_.end()) {
                it->second->onAppendEntriesReply(from, reply);
            }
        });
    }

    // 推进逻辑时间：驱动节点定时器，再投递消息和回复
    void tick(Clock::time_point now) {
        for (auto& [id, node] : nodes_) {
            if (!isDown(id)) {
                node->tick(now);
            }
        }
        deliverAll();
    }

private:
    bool blocked(int from, int to) const {
        if (isDown(from) || isDown(to)) {
            return true;
        }

        if (!partitioned_) {
            return false;
        }

        const bool fromA = groupA_.count(from) != 0;
        const bool toA = groupA_.count(to) != 0;
        const bool fromB = groupB_.count(from) != 0;
        const bool toB = groupB_.count(to) != 0;

        return (fromA && toB) || (fromB && toA);
    }

    template <typename Fn>
    void enqueue(int from, int to, Fn fn) {
        if (blocked(from, to)) {
            return;
        }
        queue_.push_back(std::move(fn));
    }

    void deliverAll() {
        while (!queue_.empty()) {
            auto fn = std::move(queue_.front());
            queue_.erase(queue_.begin());
            fn();
        }
    }

private:
    std::map<int, Raft*> nodes_;
    std::set<int> down_;
    std::set<int> groupA_;
    std::set<int> groupB_;
    bool partitioned_ = false;
    std::vector<std::function<void()>> queue_;
};
