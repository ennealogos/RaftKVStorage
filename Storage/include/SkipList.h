#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <random>
#include <utility>
#include <vector>

/*
跳表：对有序单链表的改造，优化其查找效率
在基础的单链表连接之外，增加多层的指针，除第零层每个节点指向下一个相临节点外，往上的增加的指针，其指向的下一个节点由随机数决定，
如果其下一个节点摇到了需要的随机数，那么它就会指向下一个节点，否则跳过这下一个节点，往后遍历。
*/

// key / value 需要可默认构造，用于哨兵节点
template <typename Key, typename Value, typename Compare = std::less<Key>>
class SkipList {
private:
    struct Node {
        Key key;
        Value value;
        std::vector<Node*> next; // 层级指针
        // 构造函数
        Node (int level, const Key& k, const Value& v)
            : key(k), value(v), next(static_cast<std::size_t>(level), nullptr) {}
    };

public:
    explicit SkipList (int maxLevel = 16,
                       double probability = 0.25,
                       std::uint32_t seed = std::random_device{}())
        : maxLevel_(std::max(1, maxLevel)),
          probability_(probability > 0.0 && probability < 1.0 ? probability : 0.25),
          rng_(seed),
          dist_(0.0, 1.0),
          level_(1),
          size_(0),
          head_(new Node(maxLevel_, Key{}, Value{})) {}  // 哨兵节点是每个层级的入口，因此每个层级都需要
    
    ~SkipList () {
        clear();
        delete head_;
    }

    // 禁用拷贝构造和拷贝赋值
    SkipList(const SkipList&) = delete;
    SkipList& operator= (const SkipList&) = delete;

    // 向跳表加入新key，返回 true 表示新建节点成功，false 表示 key 已经存在，只更新了 value
    bool put (const Key& key, const Value& value) {
        // 存储第一个大于等于 key 的节点的前驱
        std::vector<Node*> prev(static_cast<std::size_t>(maxLevel_), nullptr);
        Node* node = findGreaterOrEqual(key, &prev); // 第一个大于等于 key 的节点

        // 如果存在大于等于 key 的节点，并且该节点的 key 等于传入的 key，更新 value 并返回 false
        if (node != nullptr && equal(node->key, key)) {
            node->value = value;
            return false;
        }

        const int newLevel = randomLevel(); // 新节点的层数
        // 当新节点的层数高于当前跳表的层数，多出来的层从哨兵开始
        if (newLevel > level_) {
            for (int i = level_; i < newLevel; ++i) {
                prev[static_cast<std::size_t>(i)] = head_; // 由于其他已经存在的节点的层数已经确定，所以这一层的实际节点只有新增的节点
            }
            level_ = newLevel;
        }

        Node* newNode = new Node(newLevel, key, value);
        const std::size_t levelCount = static_cast<std::size_t>(newLevel);
        for (std::size_t i = 0; i < levelCount; ++i) {
            // 连接每一层
            newNode->next[i] = prev[i]->next[i];
            prev[i]->next[i] = newNode;
        }

        ++ size_;
        return true;
    }

    // 从跳表中获取 key 对应的 value，value存入传入的指针，返回 true 说明获取成功，否则失败
    bool get(const Key& key, Value* value) const {
        Node* node = findGreaterOrEqual(key, nullptr);
        if (node == nullptr || !equal(node->key, key)) {
            return false;
        }

        if (value != nullptr) {
            *value = node->value;
        }
        return true;
    }

    // 检查跳表是否包含指定的 key
    bool contains(const Key& key) const {
        return get(key, nullptr);
    }

    // 从跳表中删除指定的 key，如果跳表中没有指定的 key，返回 false
    bool erase(const Key& key) {
        std::vector<Node*> prev(static_cast<size_t>(maxLevel_), nullptr);
        Node* node = findGreaterOrEqual(key, &prev);

        if (node == nullptr || !equal(node->key, key)) {
            return false;
        }

        for (int i = 0; i < level_; ++ i) {
            auto idx = static_cast<size_t>(i);
            if (prev[idx]->next[idx] == node) {
                prev[idx]->next[idx] = node->next[idx];
            }
        }

        delete node;
        --size_;

        // 最高层已经空了，降低当前层数
        while (level_ > 1 &&
               head_->next[static_cast<std::size_t>(level_ - 1)] == nullptr) {
            --level_;
        }
        return true;
    }

    // 返回跳表中的节点数量
    size_t size() const { return size_; }
    // 检查跳表是否为空
    bool empty() const { return size_ == 0; }

    // 清空跳表
    void clear() {
        Node* node = head_->next[0];
        while (node != nullptr) {
            Node* next = node->next[0];
            delete node;
            node = next;
        }

        // 清理哨兵的层级指针
        std::fill(head_->next.begin(), head_->next.end(), nullptr);
        level_ = 1;
        size_ = 0;
    }

    // 按 key 升序访问，用于 dump 快照
    template <typename Fn>
    void forEach (Fn&& fn) const {
        for (Node* node = head_->next[0]; node != nullptr; node = node->next[0]) {
            fn (node->key, node->value);
        }
    }

    // load 时用原子替换
    void swap (SkipList& other) {
        using std::swap;
        swap(maxLevel_, other.maxLevel_);
        swap(probability_, other.probability_);
        swap(rng_, other.rng_);
        swap(dist_, other.dist_);
        swap(level_, other.level_);
        swap(size_, other.size_);
        swap(comp_, other.comp_);
        swap(head_, other.head_);
    }

private:
    // 找到第一个 >= key 的节点，prev 中保存每一层的前驱
    Node* findGreaterOrEqual (const Key& key, std::vector<Node*>* prev) const {
        Node* node = head_;

        for (int i = level_ - 1; i >= 0; --i) {
            auto idx = static_cast<std::size_t>(i);
            while (node->next[idx] != nullptr && comp_(node->next[idx]->key, key)) {
                // node->next[idx]->key < key 继续往后查找
                node = node->next[idx];
            }
            if (prev != nullptr) {
                (*prev)[idx] = node; // 将前驱存入 prev
            }
        }
        return node->next[0]; // 返回第一个大于等于 key 的节点
    }

    // 决定一个节点的最大层数
    int randomLevel () {
        int level = 1;
        while (level < maxLevel_ && dist_(rng_) < probability_) {
            ++ level;
        }
        return level;
    }

    // 判断给定的两个 key 是否相等
    bool equal(const Key& a, const Key& b) const {
        return !comp_(a, b) && !comp_(b, a);
    }

    int maxLevel_;  // 允许的最大层级数
    double probability_; // 节点向上升一层的概率
    std::mt19937 rng_;  // 随机数引擎
    std::uniform_real_distribution<double> dist_; // 均匀分布 [0, 1)
    int level_; // 当前最高层级，查找时从 level - 1 层开始
    size_t size_;   // 当前 key-vlaue 对的真实数量
    Compare comp_{};  // key 比较器
    Node* head_;    // 哨兵节点
};