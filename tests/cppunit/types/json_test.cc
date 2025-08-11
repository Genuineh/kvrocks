#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <types/redis_json.h>

#include "test_base.h"

class RedisJsonTest : public TestBase {
 protected:
  explicit RedisJsonTest() : json_(std::make_unique<redis::Json>(storage_.get(), "json_ns")) {}
  ~RedisJsonTest() override = default;

  void SetUp() override { key_ = "test_json_key"; }
  void TearDown() override {}

  std::unique_ptr<redis::Json> json_;
  JsonValue json_val_;
};

TEST_F(RedisJsonTest, Get) {
  ASSERT_TRUE(json_->Set(*ctx_, key_, "$", R"({"a": 1})").ok());
  ASSERT_TRUE(json_->Get(*ctx_, key_, {}, &json_val_).ok());
  ASSERT_EQ(json_val_.Dump().GetValue(), R"({"a":1})");
}
