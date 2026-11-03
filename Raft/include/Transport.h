#pragma once

#include "RaftTypes.h"

// Raft 与网络之间的边界。
//
// 约定：
//   - sendXxx 只负责把消息投递出去，不要在调用栈内同步回调 Raft；
//   - 真实 RPC 实现可以在收到响应后回调 Raft::onXxxReply；
//   - 内存测试网络通过队列延迟投递，避免在 Raft 持锁时重入。
class Transport {
public:
    virtual ~Transport() = default;

    virtual void sendRequestVote(int from,
                                 int to,
                                 const RequestVoteArgs& args) = 0;
    virtual void sendRequestVoteReply(int from,
                                      int to,
                                      const RequestVoteReply& reply) = 0;

    virtual void sendAppendEntries(int from,
                                   int to,
                                   const AppendEntriesArgs& args) = 0;
    virtual void sendAppendEntriesReply(int from,
                                        int to,
                                        const AppendEntriesReply& reply) = 0;
};
