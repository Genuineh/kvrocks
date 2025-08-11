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

#include <string>
#include <vector>

#include "sonic/sonic.h"
#include "status.h"

namespace redis {

class JsonPath {
 public:
  explicit JsonPath(std::string_view path);

  Status Parse();

  StatusOr<std::vector<const sonic_json::Node *>> Evaluate(const sonic_json::Node *root) const;

 private:
  enum class TokenType {
    kRoot,
    kKey,
    kIndex,
    kWildcard,
  };

  struct Token {
    TokenType type;
    std::string key;
    int64_t index = 0;
  };

  std::string_view path_;
  std::vector<Token> tokens_;
};

}  // namespace redis
