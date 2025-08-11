/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#pragma once

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <limits>
#include <string>

#include "common/string_util.h"
#include "server/redis_reply.h"
#include "json_path.h"
#include "sonic/sonic.h"
#include "status.h"
#include "storage/redis_metadata.h"

template <class T>
using Optionals = std::vector<std::optional<T>>;

struct JsonValue {
  enum class NumOpEnum : uint8_t {
    Incr = 1,
    Mul = 2,
  };

  static const size_t default_max_nesting_depth = 1024;

  JsonValue() = default;

  static StatusOr<JsonValue> FromString(std::string_view str, int = default_max_nesting_depth) {
    JsonValue val;
    val.value.Parse(str);
    if (val.value.HasParseError()) {
      return {Status::NotOK, "json parse error"};
    }
    return val;
  }

  StatusOr<std::string> Dump(int max_nesting_depth = default_max_nesting_depth) const {
    std::string res;
    GET_OR_RET(Dump(&res, max_nesting_depth));
    return res;
  }

  Status Dump(std::string *buffer, int = default_max_nesting_depth) const {
    sonic_json::WriteBuffer wb;
    value.Serialize(wb);
    *buffer = wb.ToString();
    return Status::OK();
  }

  StatusOr<std::string> Print(uint8_t = 0, bool = false, const std::string & = "") const {
    std::string res;
    GET_OR_RET(Dump(&res));
    return res;
  }

  Status Print(std::string *buffer, uint8_t = 0, bool = false, const std::string & = "") const {
    // TODO: sonic-cpp does not support pretty print yet.
    return Dump(buffer);
  }

  Status Set(std::string_view, JsonValue &&) {
    // TODO: implement this
    return Status::OK();
  }

  StatusOr<Optionals<uint64_t>> StrAppend(std::string_view, const std::string &) {
    return {Status::NotOK, "not implemented"};
  }

  StatusOr<Optionals<uint64_t>> StrLen(std::string_view) const { return {Status::NotOK, "not implemented"}; }

  StatusOr<JsonValue> Get(std::string_view path_str) const {
    redis::JsonPath path(path_str);
    auto s = path.Parse();
    if (!s.IsOK()) {
      return {Status::NotOK, s.Msg()};
    }

    auto s_nodes = path.Evaluate(&value);
    if (!s_nodes.IsOK()) {
      return s_nodes.ToStatus();
    }
    const auto &nodes = *s_nodes;

    if (nodes.empty()) {
      return {Status::NotFound, "No value at the given path"};
    }

    JsonValue result;
    if (nodes.size() == 1) {
      result.value.CopyFrom(*nodes[0], result.value.GetAllocator());
    } else {
      result.value.SetArray();
      auto &allocator = result.value.GetAllocator();
      for (const auto *node : nodes) {
        result.value.PushBack(sonic_json::Node(*node, allocator), allocator);
      }
    }

    return result;
  }

  StatusOr<Optionals<size_t>> ArrAppend(std::string_view, const std::vector<sonic_json::Node> &) {
    return {Status::NotOK, "not implemented"};
  }

  StatusOr<Optionals<uint64_t>> ArrInsert(std::string_view, const int64_t &,
                                          const std::vector<sonic_json::Node> &) {
    return {Status::NotOK, "not implemented"};
  }

  static std::pair<ssize_t, ssize_t> NormalizeArrIndices(ssize_t start, ssize_t end, ssize_t len) {
    if (start < 0) {
      start = std::max<ssize_t>(0, len + start);
    } else {
      start = std::min<ssize_t>(start, len - 1);
    }
    if (end == 0) {
      end = len;
    } else if (end < 0) {
      end = std::max<ssize_t>(0, len + end);
    }
    end = std::min<ssize_t>(end, len);
    return {start, end};
  }

  StatusOr<Optionals<ssize_t>> ArrIndex(std::string_view, const sonic_json::Node &, ssize_t, ssize_t) const {
    return {Status::NotOK, "not implemented"};
  }

  StatusOr<std::vector<std::string>> Type(std::string_view) const { return {Status::NotOK, "not implemented"}; }

  StatusOr<Optionals<bool>> Toggle(std::string_view) { return {Status::NotOK, "not implemented"}; }

  StatusOr<size_t> Clear(std::string_view) { return {Status::NotOK, "not implemented"}; }

  StatusOr<Optionals<uint64_t>> ArrLen(std::string_view) const { return {Status::NotOK, "not implemented"}; }

  StatusOr<bool> Merge(const std::string_view, const std::string &) { return {Status::NotOK, "not implemented"}; }

  StatusOr<Optionals<std::vector<std::string>>> ObjKeys(std::string_view) const {
    return {Status::NotOK, "not implemented"};
  }

  StatusOr<Optionals<uint64_t>> ObjLen(std::string_view) const { return {Status::NotOK, "not implemented"}; }

  StatusOr<Optionals<JsonValue>> ArrPop(std::string_view, int64_t = -1) {
    return {Status::NotOK, "not implemented"};
  }

  StatusOr<Optionals<uint64_t>> ArrTrim(std::string_view, int64_t, int64_t) {
    return {Status::NotOK, "not implemented"};
  }

  StatusOr<size_t> Del(const std::string &) { return {Status::NotOK, "not implemented"}; }

  Status NumOp(std::string_view, const JsonValue &, NumOpEnum, JsonValue *) {
    return {Status::NotOK, "not implemented"};
  }
  static void TransformResp(const sonic_json::Node &, std::string &, redis::RESP) {}

  StatusOr<std::vector<std::string>> ConvertToResp(std::string_view, redis::RESP) const {
    return {Status::NotOK, "not implemented"};
  }
  JsonValue(const JsonValue &) = default;
  JsonValue(JsonValue &&) = default;

  JsonValue &operator=(const JsonValue &) = default;
  JsonValue &operator=(JsonValue &&) = default;

  ~JsonValue() = default;

  sonic_json::Document value;
};
