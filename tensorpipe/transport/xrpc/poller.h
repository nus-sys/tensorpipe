/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <atomic>
#include <functional>
#include <mutex>
#include <set>
#include <vector>

#include <tensorpipe/common/busy_polling_loop.h>

#include <diancie_shm/rpc_session.hpp>

namespace tensorpipe {
namespace transport {
namespace xrpc {

// Poller replaces the Reactor for xrpc transport.
// Instead of polling a shared-memory token ring buffer, it polls
// Diancie QueueEntries for flag changes and invokes callbacks.
class Poller final : public BusyPollingLoop {
 public:
  using TFunction = std::function<void()>;
  using TToken = uint32_t;

  Poller();

  // Register a QueueEntry to poll. When its flag_f toggles to match
  // the expected value, the callback is invoked.
  TToken add(diancie::QueueEntry* qe, TFunction fn);

  // Unregister a previously registered entry.
  void remove(TToken token);

  void close();

  void join();

  ~Poller();

 protected:
  bool pollOnce() override;

  bool readyToClose() override;

 private:
  struct Entry {
    diancie::QueueEntry* qe = nullptr;
    TFunction fn;
    bool lastFlag = false;
  };

  std::mutex mutex_;
  std::atomic<bool> closed_{false};
  std::atomic<bool> joined_{false};

  std::set<TToken> reusableTokens_;
  std::vector<Entry> entries_;
  std::atomic<uint64_t> entryCount_{0};
};

} // namespace xrpc
} // namespace transport
} // namespace tensorpipe
