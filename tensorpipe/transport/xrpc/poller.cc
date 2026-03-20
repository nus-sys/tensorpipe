/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/xrpc/poller.h>

namespace tensorpipe {
namespace transport {
namespace xrpc {

Poller::Poller() {
  startThread("TP_XRPC_poller");
}

Poller::TToken Poller::add(diancie::QueueEntry* qe, TFunction fn) {
  std::unique_lock<std::mutex> lock(mutex_);
  TToken token;

  auto it = reusableTokens_.begin();
  if (it != reusableTokens_.end()) {
    token = *it;
    reusableTokens_.erase(it);
  } else {
    token = entries_.size();
  }

  if (entries_.size() <= token) {
    entries_.resize(token + 1);
  }

  entries_[token].qe = qe;
  entries_[token].fn = std::move(fn);
  // Capture the current flag state so we fire on the *next* toggle.
  entries_[token].lastFlag = qe->get_flag_f();

  entryCount_++;
  return token;
}

void Poller::remove(TToken token) {
  std::unique_lock<std::mutex> lock(mutex_);
  entries_[token].qe = nullptr;
  entries_[token].fn = nullptr;
  reusableTokens_.insert(token);
  entryCount_--;
}

void Poller::close() {
  if (!closed_.exchange(true)) {
    stopBusyPolling();
  }
}

void Poller::join() {
  close();

  if (!joined_.exchange(true)) {
    joinThread();
  }
}

Poller::~Poller() {
  join();
}

bool Poller::pollOnce() {
  bool didWork = false;

  // Take a snapshot under the lock to avoid holding it while calling callbacks.
  std::vector<std::pair<TToken, TFunction>> toCall;
  {
    std::unique_lock<std::mutex> lock(mutex_);
    for (TToken i = 0; i < entries_.size(); i++) {
      auto& entry = entries_[i];
      if (entry.qe == nullptr) {
        continue;
      }
      entry.qe->invalidate_entry();
      bool currentFlag = entry.qe->get_flag_f();
      if (currentFlag != entry.lastFlag) {
        entry.lastFlag = currentFlag;
        toCall.emplace_back(i, entry.fn);
      }
    }
  }

  for (auto& [token, fn] : toCall) {
    if (fn) {
      fn();
      didWork = true;
    }
  }

  return didWork;
}

bool Poller::readyToClose() {
  return entryCount_ == 0;
}

} // namespace xrpc
} // namespace transport
} // namespace tensorpipe
