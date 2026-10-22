// 协议编解码冒烟测试：
//   上层命令 Op -> 序列化 -> LogEntry.command -> AppendEntriesArgs
//   -> 模拟网络传输 -> 反序列化 -> 还原 Op -> 校验字段
#include <cassert>
#include <iostream>
#include <string>

#include "kv.pb.h"
#include "raft.pb.h"

int main() {
  // 1. 构造上层命令
  kvrpc::Op op;
  op.set_kind(kvrpc::Op::PUT);
  op.set_key("x");
  op.set_value("42");
  op.set_client_id("client-1");
  op.set_request_id(7);

  // 2. 命令序列化后放入日志条目
  raftrpc::AppendEntriesArgs args;
  args.set_term(3);
  args.set_leader_id(1);
  args.set_prev_log_index(5);
  args.set_prev_log_term(2);
  args.set_leader_commit(5);

  raftrpc::LogEntry* entry = args.add_entries();
  entry->set_term(3);
  entry->set_index(6);
  entry->set_command(op.SerializeAsString());

  // 3. 模拟网络传输
  std::string wire;
  assert(args.SerializeToString(&wire));

  raftrpc::AppendEntriesArgs decoded;
  assert(decoded.ParseFromString(wire));
  assert(decoded.term() == 3);
  assert(decoded.entries_size() == 1);

  // 4. 还原上层命令
  kvrpc::Op decoded_op;
  assert(decoded_op.ParseFromString(decoded.entries(0).command()));
  assert(decoded_op.kind() == kvrpc::Op::PUT);
  assert(decoded_op.key() == "x");
  assert(decoded_op.value() == "42");
  assert(decoded_op.client_id() == "client-1");
  assert(decoded_op.request_id() == 7);

  std::cout << "[OK] protobuf round-trip, wire size = " << wire.size() << " bytes\n";
  return 0;
}
