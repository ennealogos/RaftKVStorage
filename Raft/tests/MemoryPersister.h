#pragma once

#include "Persister.h"

class MemoryPersister : public Persister {
public:
    bool load(PersistentState* state) override {
        if (!hasState_ || state == nullptr) {
            return false;
        }
        *state = state_;
        return true;
    }

    bool save(const PersistentState& state) override {
        state_ = state;
        hasState_ = true;
        ++saveCount_;
        return true;
    }

    bool hasState() const { return hasState_; }
    int saveCount() const { return saveCount_; }
    const PersistentState& state() const { return state_; }

private:
    PersistentState state_;
    bool hasState_ = false;
    int saveCount_ = 0;
};
