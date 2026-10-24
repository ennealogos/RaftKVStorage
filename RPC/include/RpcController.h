#pragma once

#include <google/protobuf/service.h>
#include <string>

// 存储客户端错误
class RpcController : public google::protobuf::RpcController {
public:
    // 重置 controller 状态
    void Reset() override {
        failed_ = false;
        errTxt_.clear();
    }

    // 是否存在错误
    bool Failed() const override { return failed_; }
    // 返回错误信息
    std::string ErrorText() const override { return errTxt_; }

    void StartCancel() override {}
    bool IsCanceled() const override { return false; }
    void NotifyOnCancel(google::protobuf::Closure*) {}

    // 根据传入信息，设置 controller 状态
    void SetFailed(const std::string& reason) override {
        failed_ = true;
        errTxt_ = reason;
    }


private:
    bool failed_ = false;
    std::string errTxt_; // 失败信息
};