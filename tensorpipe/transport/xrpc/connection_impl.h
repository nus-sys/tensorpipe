/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <deque>
#include <memory>
#include <string>

#include <tensorpipe/common/nop.h>
#include <tensorpipe/common/optional.h>
#include <tensorpipe/common/ringbuffer.h>
#include <tensorpipe/common/ringbuffer_read_write_ops.h>
#include <tensorpipe/transport/connection_impl_boilerplate.h>
#include <tensorpipe/transport/xrpc/poller.h>

#include <diancie_shm/rpc_session.hpp>

namespace tensorpipe {
namespace transport {
namespace xrpc {

class ContextImpl;
class ListenerImpl;

class ConnectionImpl final : public ConnectionImplBoilerplate<
                                 ContextImpl,
                                 ListenerImpl,
                                 ConnectionImpl> {
  // Ring buffer data size: 512KB for inbox, 512KB for outbox.
  static constexpr size_t kRingBufferSize = 512 * 1024;

  static constexpr int kNumRingbufferRoles = 2;
  using Consumer = RingBufferRole<kNumRingbufferRoles, 0>;
  using Producer = RingBufferRole<kNumRingbufferRoles, 1>;

  enum State {
    INITIALIZING = 1,
    ESTABLISHED,
  };

 public:
  // Create a connection from an accepted client (server side).
  // channel_id is known from the NOTIFY_CLIENT_REQ.
  ConnectionImpl(
      ConstructorToken token,
      std::shared_ptr<ContextImpl> context,
      std::string id,
      diancie::channel_id_t channelId,
      bool isServer);

  // Create a connection that connects to the specified service (client side).
  ConnectionImpl(
      ConstructorToken token,
      std::shared_ptr<ContextImpl> context,
      std::string id,
      std::string addr);

 protected:
  // Implement the entry points called by ConnectionImplBoilerplate.
  void initImplFromLoop() override;
  void readImplFromLoop(read_callback_fn fn) override;
  void readImplFromLoop(AbstractNopHolder& object, read_nop_callback_fn fn)
      override;
  void readImplFromLoop(void* ptr, size_t length, read_callback_fn fn) override;
  void writeImplFromLoop(const void* ptr, size_t length, write_callback_fn fn)
      override;
  void writeImplFromLoop(const AbstractNopHolder& object, write_callback_fn fn)
      override;
  void handleErrorImpl() override;

 private:
  State state_{INITIALIZING};

  // Connection parameters.
  diancie::channel_id_t channelId_{0};
  bool isServer_{false};
  optional<std::string> addr_;  // For client-side connections.

  // Diancie metadata for this channel.
  std::unique_ptr<diancie::RPCConnectionMetaData> metadata_;

  // Ring buffers backed by Diancie shared memory.
  // Memory layout within the channel's payload area:
  //   [0 .. sizeof(RingBufferHeader<2>))                     -> inbox header
  //   [sizeof(header) .. sizeof(header) + kRingBufferSize)   -> inbox data
  //   [inbox_end .. inbox_end + sizeof(header))              -> outbox header
  //   [inbox_end + sizeof(header) .. end)                    -> outbox data
  optional<RingBuffer<kNumRingbufferRoles>> inboxRb_;
  optional<RingBuffer<kNumRingbufferRoles>> outboxRb_;

  // Poller tokens for inbox/outbox notifications.
  optional<Poller::TToken> inboxPollerToken_;
  optional<Poller::TToken> outboxPollerToken_;

  // Peer's queue entries for notification.
  diancie::QueueEntry* peerInboxQe_{nullptr};
  diancie::QueueEntry* peerOutboxQe_{nullptr};

  // Pending read/write operations.
  std::deque<RingbufferReadOperation> readOperations_;
  std::deque<RingbufferWriteOperation> writeOperations_;

  void initServerSide();
  void initClientSide();
  void setupRingBuffers(bool swapInboxOutbox);

  void processReadOperationsFromLoop();
  void processWriteOperationsFromLoop();

  // Notify peer that data is available in their inbox / space freed in outbox.
  void notifyPeerInbox();
  void notifyPeerOutbox();
};

} // namespace xrpc
} // namespace transport
} // namespace tensorpipe
