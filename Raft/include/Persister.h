#pragma once

#include "RaftTypes.h"

// Raft 持久化边界。
// 2A 只需要保存 currentTerm / votedFor；后续扩展 logs 和 snapshot。
class Persister {
public:
    virtual ~Persister() = default;

    virtual bool load(PersistentState* state) = 0;
    virtual bool save(const PersistentState& state) = 0;
};
