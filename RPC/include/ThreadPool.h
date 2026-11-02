#pragma once

#include "Logger.h"

#include <condition_variable>
#include <cstddef>
#include <exception>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

// 固定大小线程池：
//   - 有界任务队列，队列满时 enqueue 返回 false（由调用方决定拒绝策略）
//   - worker 内部捕获异常，避免一个 handler 抛异常导致整个进程 terminate
//   - stop() 停止接收新任务，并 drain 已入队任务后 join
class ThreadPool {
public:
    explicit ThreadPool(size_t threadCount = 4, size_t maxQueueSize = 10000)
        : maxQueueSize_(maxQueueSize) {
        if (threadCount == 0) {
            threadCount = 1;
        }
        for (size_t i = 0; i < threadCount; ++i) {
            workers_.emplace_back([this]() { workerLoop(); });
        }
    }

    ~ThreadPool() { stop(); }

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    // 返回 false：线程池已停止，或队列已满
    bool enqueue(std::function<void()> task) {
        {
            std::lock_guard<std::mutex> lock(mtx_);
            if (stopping_ || tasks_.size() >= maxQueueSize_) {
                return false;
            }
            tasks_.push(std::move(task));
        }
        cv_.notify_one();
        return true;
    }

    // 停止接收新任务，等待已入队任务全部执行完
    // 注意：不能在 worker 线程中调用，否则 join 自己会死锁
    void stop() {
        std::call_once(stopOnce_, [this]() {
            {
                std::lock_guard<std::mutex> lock(mtx_);
                stopping_ = true;
            }
            cv_.notify_all();
            for (auto& t : workers_) {
                if (t.joinable()) {
                    t.join();
                }
            }
            workers_.clear();
        });
    }

private:
    void workerLoop() {
        while (true) {
            std::function<void()> task;
            {
                std::unique_lock<std::mutex> lock(mtx_);
                cv_.wait(lock, [this]() { return stopping_ || !tasks_.empty(); });
                if (stopping_ && tasks_.empty()) {
                    return;  // drain 完再退出
                }
                task = std::move(tasks_.front());
                tasks_.pop();
            }

            try {
                task();
            } catch (const std::exception& e) {
                LOG_ERROR << "ThreadPool: task threw exception: " << e.what();
            } catch (...) {
                LOG_ERROR << "ThreadPool: task threw unknown exception";
            }
        }
    }

    std::vector<std::thread> workers_;
    std::queue<std::function<void()>> tasks_;
    std::mutex mtx_;
    std::condition_variable cv_;
    std::once_flag stopOnce_;
    bool stopping_ = false;
    size_t maxQueueSize_;
};
