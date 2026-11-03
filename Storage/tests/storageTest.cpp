// Storage 层测试：
//   1. SkipList 基本操作
//   2. 与 std::map 随机对拍
//   3. 排序/自定义比较器/边界层数
//   4. StringKVStore 快照往返、二进制 key/value
//   5. 损坏快照不破坏当前状态
#include "SkipList.h"
#include "StringKVStore.h"

#include <algorithm>
#include <cstdint>
#include <functional>
#include <iostream>
#include <map>
#include <random>
#include <string>
#include <vector>

namespace {

int g_checks = 0;
int g_failures = 0;

#define CHECK(cond)                                                     \
    do {                                                                \
        ++g_checks;                                                     \
        if (!(cond)) {                                                  \
            std::cerr << "FAILED: " #cond " at " << __FILE__ << ":"     \
                      << __LINE__ << std::endl;                         \
            ++g_failures;                                               \
        }                                                               \
    } while (0)

void testBasic() {
    SkipList<std::string, std::string> list(16, 0.25, 42);
    std::string value;

    CHECK(list.empty());
    CHECK(list.size() == 0);
    CHECK(!list.get("missing", &value));
    CHECK(!list.contains("missing"));

    CHECK(list.put("a", "1"));   // 新 key
    CHECK(!list.put("a", "2"));  // 已存在，更新
    CHECK(list.size() == 1);
    CHECK(list.get("a", &value));
    CHECK(value == "2");
    CHECK(list.contains("a"));
    CHECK(list.get("a", nullptr));  // 允许不取 value

    CHECK(!list.erase("missing"));
    CHECK(list.erase("a"));
    CHECK(list.empty());
    CHECK(!list.contains("a"));
}

void testOrderingAndCustomComparator() {
    SkipList<int, int, std::greater<int>> list(8, 0.5, 7);
    for (int i = 0; i < 20; ++i) {
        CHECK(list.put(i, i * 10));
    }

    std::vector<int> keys;
    list.forEach([&](int key, int) { keys.push_back(key); });

    CHECK(keys.size() == 20);
    CHECK(std::is_sorted(keys.begin(), keys.end(), std::greater<int>{}));

    int value = 0;
    CHECK(list.get(13, &value));
    CHECK(value == 130);
}

void testLevelEdges() {
    // maxLevel = 1：只有第 0 层
    SkipList<int, int> oneLevel(1, 0.25, 1);
    for (int i = 0; i < 100; ++i) {
        CHECK(oneLevel.put(i, i));
    }
    CHECK(oneLevel.size() == 100);
    for (int i = 0; i < 100; i += 2) {
        CHECK(oneLevel.erase(i));
    }
    CHECK(oneLevel.size() == 50);
    for (int i = 0; i < 100; ++i) {
        int value = 0;
        CHECK(oneLevel.get(i, &value) == (i % 2 == 1));
    }

    // maxLevel <= 0 会被构造函数兜底为 1
    SkipList<int, int> clamped(0, 0.25, 1);
    CHECK(clamped.put(1, 1));
    CHECK(clamped.get(1, nullptr));

    // probability 非法时兜底为 0.25
    SkipList<int, int> badProbability(4, 0.0, 1);
    CHECK(badProbability.put(2, 2));
}

void testRandomAgainstMap() {
    SkipList<std::string, std::string> list(16, 0.25, 123);
    std::map<std::string, std::string> reference;
    std::mt19937 rng(20261103);

    constexpr int kOperations = 100000;
    for (int i = 0; i < kOperations; ++i) {
        const std::string key = "key-" + std::to_string(rng() % 5000);
        const int op = static_cast<int>(rng() % 4);

        if (op == 0 || op == 1) {
            const std::string value = "value-" + std::to_string(i);
            const bool existed = reference.find(key) != reference.end();
            const bool inserted = list.put(key, value);

            CHECK(inserted != existed);  // 新 key 返回 true，更新返回 false
            reference[key] = value;
        } else if (op == 2) {
            const bool erased = list.erase(key);
            const bool referenceErased = reference.erase(key) > 0;
            CHECK(erased == referenceErased);
        } else {
            std::string value;
            const bool found = list.get(key, &value);
            const auto it = reference.find(key);

            CHECK(found == (it != reference.end()));
            if (found) {
                CHECK(value == it->second);
            }
        }

        CHECK(list.size() == reference.size());
    }

    // 全量校验，并检查有序遍历
    auto refIt = reference.begin();
    list.forEach([&](const std::string& key, const std::string& value) {
        CHECK(refIt != reference.end());
        if (refIt != reference.end()) {
            CHECK(key == refIt->first);
            CHECK(value == refIt->second);
            ++refIt;
        }
    });
    CHECK(refIt == reference.end());
}

void testSnapshotRoundTrip() {
    StringKVStore store(16, 0.25, 42);
    CHECK(store.put("a", "1"));
    CHECK(store.put("b", "2"));
    CHECK(store.put("c", "3"));

    const std::string snapshot = store.dump();
    CHECK(!snapshot.empty());

    StringKVStore restored;
    CHECK(restored.load(snapshot));
    CHECK(restored.size() == 3);

    std::string value;
    CHECK(restored.get("a", &value));
    CHECK(value == "1");
    CHECK(restored.get("b", &value));
    CHECK(value == "2");
    CHECK(restored.get("c", &value));
    CHECK(value == "3");

    // 空表也能往返
    StringKVStore empty;
    const std::string emptySnapshot = empty.dump();
    StringKVStore restoredEmpty;
    CHECK(restoredEmpty.put("old", "value"));
    CHECK(restoredEmpty.load(emptySnapshot));
    CHECK(restoredEmpty.empty());
    CHECK(!restoredEmpty.contains("old"));
}

void testBinarySnapshot() {
    StringKVStore store(16, 0.25, 42);

    const std::string key("k\0ey", 4);
    const std::string value("v\0al", 4);
    CHECK(store.put(key, value));

    const std::string snapshot = store.dump();
    CHECK(!snapshot.empty());

    StringKVStore restored;
    CHECK(restored.load(snapshot));
    CHECK(restored.size() == 1);

    std::string got;
    CHECK(restored.get(key, &got));
    CHECK(got.size() == value.size());
    CHECK(got == value);
}

void testCorruptSnapshotDoesNotModifyState() {
    StringKVStore source;
    CHECK(source.put("a", "1"));
    CHECK(source.put("b", "2"));

    const std::string good = source.dump();
    StringKVStore target;
    CHECK(target.put("keep", "value"));

    // 完全错误的输入
    CHECK(!target.load("bad-data"));

    // magic 错误
    std::string wrongMagic = good;
    wrongMagic[0] = static_cast<char>(wrongMagic[0] ^ 0xff);
    CHECK(!target.load(wrongMagic));

    // version 错误
    std::string wrongVersion = good;
    wrongVersion[4] = static_cast<char>(wrongVersion[4] ^ 0xff);
    CHECK(!target.load(wrongVersion));

    // 截断
    CHECK(!target.load(good.substr(0, good.size() - 1)));

    // 尾部脏数据
    std::string trailing = good;
    trailing.push_back('x');
    CHECK(!target.load(trailing));

    // 任何一次失败都不应该破坏原数据
    std::string value;
    CHECK(target.get("keep", &value));
    CHECK(value == "value");
    CHECK(target.size() == 1);
}

void testSnapshotReplacesExistingState() {
    StringKVStore source;
    CHECK(source.put("new", "data"));

    StringKVStore target;
    CHECK(target.put("old", "data"));

    CHECK(target.load(source.dump()));
    CHECK(!target.contains("old"));
    CHECK(target.contains("new"));

    std::string value;
    CHECK(target.get("new", &value));
    CHECK(value == "data");
}

void testLargeValueSnapshot() {
    StringKVStore store(16, 0.25, 42);
    const std::string key = "large";
    const std::string value(1024 * 1024, 'x');
    CHECK(store.put(key, value));

    const std::string snapshot = store.dump();
    StringKVStore restored;
    CHECK(restored.load(snapshot));

    std::string got;
    CHECK(restored.get(key, &got));
    CHECK(got.size() == value.size());
    CHECK(got == value);
}

void testCustomConfigSnapshot() {
    // maxLevel = 1 的配置也应当能正常 dump/load
    StringKVStore store(1, 0.35, 7);
    for (int i = 0; i < 50; ++i) {
        CHECK(store.put("key-" + std::to_string(i), std::to_string(i)));
    }

    StringKVStore restored(1, 0.35, 7);
    CHECK(restored.load(store.dump()));
    CHECK(restored.size() == 50);

    std::string value;
    CHECK(restored.get("key-0", &value));
    CHECK(value == "0");
    CHECK(restored.get("key-49", &value));
    CHECK(value == "49");
}

}  // namespace

int main() {
    testBasic();
    testOrderingAndCustomComparator();
    testLevelEdges();
    testRandomAgainstMap();
    testSnapshotRoundTrip();
    testBinarySnapshot();
    testCorruptSnapshotDoesNotModifyState();
    testSnapshotReplacesExistingState();
    testLargeValueSnapshot();
    testCustomConfigSnapshot();

    if (g_failures == 0) {
        std::cout << "ALL STORAGE TESTS PASSED (" << g_checks << " checks)" << std::endl;
        return 0;
    }

    std::cerr << g_failures << " / " << g_checks << " checks FAILED" << std::endl;
    return 1;
}
