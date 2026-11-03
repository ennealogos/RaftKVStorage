#pragma once

#include "SkipList.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <random>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

class StringKVStore {
public:
    using List = SkipList<std::string, std::string>;

    explicit StringKVStore(int maxLevel = 16,
                           double probability = 0.25,
                           std::uint32_t seed = std::random_device{}())
        : maxLevel_(maxLevel),
          probability_(probability),
          list_(maxLevel, probability, seed) {}

    // 向表中插入新节点，成功返回 true，如果 key 已经存在则更新 value 并返回 false
    bool put (const std::string& key, const std::string& value) {
        return list_.put(key, value);
    }

    // 从表中获取 key 对应的 value，成功返回 true， 否则返回 false
    bool get (const std::string& key, std::string* value) const {
        return list_.get(key, value);
    }

    // 从表中删除 key 对应的节点，成功返回 true
    bool erase (const std::string& key) {
        return list_.erase(key);
    }

    // 检查表中是否有 key 对应的节点
    bool contains (const std::string& key) const {
        return list_.contains(key);
    }

    // 返回表中的节点数
    size_t size() const { return list_.size(); }
    // 检查表是否为空
    bool empty() const { return list_.empty(); }
    // 清空表
    void clear() { list_.clear(); }

    // 将表中数据压缩为快照，快照存储在 std::strig 中
    /*
    快照格式：
        magic(4B) | version(4B) | count(8B)
        repeated:
        key_len(4B) | key bytes | value_len(4B) | value bytes
    */
   std::string dump () const {
        std::string out; // 快照
        appendU32(out, kMagic); // 先填魔数
        appendU32(out, kVersion);
        appendU64(out, static_cast<std::uint64_t>(list_.size())); // 存储的数据量

        bool ok = true;
        //通过 forEach 遍历表中每个数据，将其追加到 out 中
        list_.forEach([&](const std::string& key, const std::string& value) {
            if (!ok) { // 键或值的大小超出了预留的 key_len （uint32_t）
                return;
            }

            if (key.size() > std::numeric_limits<std::uint32_t>::max() || 
                value.size() > std::numeric_limits<std::uint32_t>::max()) {
                    ok = false;
                    return;
            }
            
            appendU32(out, static_cast<std::uint32_t>(key.size())); // 将 key_len 追加到 快照
            out.append(key); // 再追加 key
            appendU32(out, static_cast<std::uint32_t>(value.size()));
            out.append(value);
        });

        return ok ? out : std::string {}; // ok == false 说明表中有数据超出了设定好的大小。
    }

    // 从传入的快照中读取数据
    bool load (const std::string& data) {
        std::string_view in(data);
        size_t pos = 0;

        uint32_t magic = 0;
        uint32_t version = 0;
        uint64_t count = 0;

        // 魔数不一致说明不是本系统的快照，版本不一致也不进行读取
        if (!readU32(in, pos, &magic) || magic != kMagic) {
            return false;
        }
        if (!readU32(in, pos, &version) || version != kVersion) {
            return false;
        }
        if (!readU64(in, pos, &count)) {
            return false;
        }

        // 每个条目至少有 key_len + value_len 共 8 字节
        if (count > (in.size() - pos) / 8) {
            return false;
        }

        std::vector<std::pair<std::string, std::string>> entries;
        entries.reserve(static_cast<size_t>(std::min<uint64_t>(count, 1'000'000))); // 先预留好空间

        for (uint64_t i = 0; i < count; ++i) {
            uint32_t keyLen = 0;
            uint32_t valueLen = 0;

            if (!readU32(in, pos, &keyLen) || keyLen > in.size() - pos) {
                return false;
            }
            std::string key(in.substr(pos, keyLen)); // 复制 key
            pos += keyLen; // 推进光标

            if (!readU32(in, pos, &valueLen) || valueLen > in.size() - pos) {
                return false;
            }
            std::string value(in.substr(pos, valueLen));
            pos += valueLen;

            entries.emplace_back(std::move(key), std::move(value));
        }

        if (pos != in.size()) {
            // 尾部有脏数据
            return false;
        }

        // 先全部解析再进行替换，避免损坏快照破坏了表的当前状态
        List tmp(maxLevel_, probability_);
        for (auto& [key, value] : entries) {
            // 正常 dump 出来的快照不会有重复 key
            if (!tmp.put(key, value)) {
                return false;
            }
        }

        // 交换
        list_.swap(tmp);
        return true;
    }
private:
    static constexpr std::uint32_t kMagic = 0x4b56534c;  // 魔数，表示 "KVSL"
    static constexpr std::uint32_t kVersion = 1;

    // 按大端序将 uint32 追加到 string 中
    static void appendU32(std::string& out, std::uint32_t value) {
        for (int shift = 24; shift >= 0; shift -= 8) {
            out.push_back(static_cast<char>((value >> shift) & 0xff));
        }
    }

    // 按大端序将 uint64 追加到 string 中
    static void appendU64(std::string& out, std::uint64_t value) {
        for (int shift = 56; shift >= 0; shift -= 8) {
            out.push_back(static_cast<char>((value >> shift) & 0xff));
        }
    }

    // 按大端序将 uint32 从 string_view 中读取出来，并推进 pos
    static bool readU32(std::string_view in, std::size_t& pos, std::uint32_t* value) {
        if (value == nullptr || pos > in.size() || in.size() - pos < 4) {
            return false;
        }

        *value = (static_cast<std::uint32_t>(static_cast<unsigned char>(in[pos])) << 24) |
                 (static_cast<std::uint32_t>(static_cast<unsigned char>(in[pos + 1])) << 16) |
                 (static_cast<std::uint32_t>(static_cast<unsigned char>(in[pos + 2])) << 8) |
                 (static_cast<std::uint32_t>(static_cast<unsigned char>(in[pos + 3])));
        pos += 4;
        return true;
    }

    // 按大端序将 uint64 从 string_view 中读取出来，并推进 pos
    static bool readU64(std::string_view in, std::size_t& pos, std::uint64_t* value) {
        if (value == nullptr) {
            return false;
        }

        std::uint32_t high = 0;
        std::uint32_t low = 0;
        if (!readU32(in, pos, &high) || !readU32(in, pos, &low)) {
            return false;
        }

        *value = (static_cast<std::uint64_t>(high) << 32) | low;
        return true;
    }

private:
    // 保存配置，load 重建临时表时保持一致
    int maxLevel_;
    double probability_;
    List list_;
};