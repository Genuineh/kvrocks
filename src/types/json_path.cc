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

#include "json_path.h"

#include <charconv>

namespace redis {

JsonPath::JsonPath(std::string_view path) : path_(path) {}

Status JsonPath::Parse() {
  if (path_.empty()) {
    return {Status::NotOK, "JSONPath cannot be empty"};
  }

  if (path_[0] != '$') {
    return {Status::NotOK, "JSONPath must start with '$'"};
  }
  tokens_.push_back({TokenType::kRoot});

  size_t i = 1;
  while (i < path_.size()) {
    if (path_[i] == '.') {
      i++;  // Skip '.'
      if (i < path_.size() && path_[i] == '.') {
        return {Status::NotOK, "Recursive descent `..` is not supported"};
      }

      size_t start = i;
      while (i < path_.size() && path_[i] != '.' && path_[i] != '[') {
        i++;
      }
      if (i == start) {
        return {Status::NotOK, "Invalid key after '.'"};
      }
      tokens_.push_back({TokenType::kKey, std::string(path_.substr(start, i - start))});
    } else if (path_[i] == '[') {
      i++;  // Skip '['
      if (i >= path_.size()) return {Status::NotOK, "Invalid path: unclosed bracket"};

      if (path_[i] == '\'') {  // Quoted key
        i++;                    // Skip '\''
        size_t start = i;
        while (i < path_.size() && path_[i] != '\'') {
          i++;
        }
        if (i + 1 >= path_.size() || path_[i] != '\'' || path_[i + 1] != ']') {
          return {Status::NotOK, "Invalid path: unclosed quote in bracket"};
        }
        tokens_.push_back({TokenType::kKey, std::string(path_.substr(start, i - start))});
        i += 2;  // Skip '\']'
      } else if (path_[i] == '*') {
        if (i + 1 >= path_.size() || path_[i + 1] != ']') {
          return {Status::NotOK, "Invalid path: wildcard must be inside []"};
        }
        tokens_.push_back({TokenType::kWildcard});
        i += 2;  // Skip '*]'
      } else {   // Index
        size_t start = i;
        while (i < path_.size() && std::isdigit(path_[i])) {
          i++;
        }
        if (i == start || i >= path_.size() || path_[i] != ']') {
          return {Status::NotOK, "Invalid index in bracket"};
        }
        long long index_val;
        auto res = std::from_chars(path_.data() + start, path_.data() + i, index_val);
        if (res.ec != std::errc()) {
          return {Status::NotOK, "Invalid index in bracket"};
        }
        Token t;
        t.type = TokenType::kIndex;
        t.index = index_val;
        tokens_.push_back(t);
        i++;  // Skip ']'
      }
    } else {
      return {Status::NotOK, "Unsupported syntax in JSONPath, only '.' and '[]' are allowed"};
    }
  }
  return Status::OK();
}

StatusOr<std::vector<const sonic_json::Node *>> JsonPath::Evaluate(const sonic_json::Node *root) const {
  std::vector<const sonic_json::Node *> current_nodes;
  if (!root) return current_nodes;

  if (tokens_.empty() || tokens_[0].type != TokenType::kRoot) {
    return {Status::NotOK, "Invalid path, should have been parsed correctly"};
  }

  current_nodes.push_back(root);

  for (size_t i = 1; i < tokens_.size(); ++i) {
    const auto &token = tokens_[i];
    std::vector<const sonic_json::Node *> next_nodes;

    for (const auto *node : current_nodes) {
      if (token.type == TokenType::kKey) {
        if (node->IsObject()) {
          auto it = node->FindMember(token.key);
          if (it != node->MemberEnd()) {
            next_nodes.push_back(&it->value);
          }
        }
      } else if (token.type == TokenType::kIndex) {
        if (node->IsArray()) {
          if (token.index >= 0 && static_cast<uint64_t>(token.index) < node->Size()) {
            next_nodes.push_back(&(*node)[static_cast<uint64_t>(token.index)]);
          }
        }
      } else if (token.type == TokenType::kWildcard) {
        if (node->IsObject()) {
          for (auto it = node->MemberBegin(); it != node->MemberEnd(); ++it) {
            next_nodes.push_back(&it->value);
          }
        } else if (node->IsArray()) {
          for (auto it = node->Begin(); it != node->End(); ++it) {
            next_nodes.push_back(&(*it));
          }
        }
      }
    }
    current_nodes = next_nodes;
    if (current_nodes.empty()) {
      break;
    }
  }

  return current_nodes;
}

}  // namespace redis
