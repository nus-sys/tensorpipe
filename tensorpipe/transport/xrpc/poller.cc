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

  // Collect ALL active callbacks.  We always invoke every callback because
  // inbox and outbox notifications share the same QueueEntry flag_f toggle.
  // Two rapid toggles (one from notifyPeerInbox, one from notifyPeerOutbox)
  // can cancel each other out before the poller observes a change, causing a
  // missed notification.  Unconditionally calling the callbacks is safe —
  // processRead/WriteOperationsFromLoop are cheap no-ops when the ring buffer
  // is empty or there are no pending operations.
  //
  // We still track flag changes so that didWork reflects actual data movement,
  // letting BusyPollingLoop yield when the system is truly idle.
  std::vector<TFunction> toCall;
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
        didWork = true;
      }
      toCall.push_back(entry.fn);
    }
  }

  for (auto& fn : toCall) {
    if (fn) {
      fn();
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
