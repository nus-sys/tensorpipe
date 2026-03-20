/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/xrpc/connection_impl.h>

#include <cstring>
#include <deque>
#include <vector>

#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/common/error_macros.h>
#include <tensorpipe/common/ringbuffer_read_write_ops.h>
#include <tensorpipe/common/ringbuffer_role.h>
#include <tensorpipe/transport/error.h>
#include <tensorpipe/transport/xrpc/context_impl.h>
#include <tensorpipe/transport/xrpc/dax_util.h>

#include <diancie_shm/rpc_malloc_internal.hpp>

namespace tensorpipe {
namespace transport {
namespace xrpc {

ConnectionImpl::ConnectionImpl(
    ConstructorToken token,
    std::shared_ptr<ContextImpl> context,
    std::string id,
    diancie::channel_id_t channelId,
    bool isServer)
    : ConnectionImplBoilerplate<ContextImpl, ListenerImpl, ConnectionImpl>(
          token,
          std::move(context),
          std::move(id)),
      channelId_(channelId),
      isServer_(isServer) {}

ConnectionImpl::ConnectionImpl(
    ConstructorToken token,
    std::shared_ptr<ContextImpl> context,
    std::string id,
    std::string addr)
    : ConnectionImplBoilerplate<ContextImpl, ListenerImpl, ConnectionImpl>(
          token,
          std::move(context),
          std::move(id)),
      isServer_(false),
      addr_(std::move(addr)) {}

void ConnectionImpl::initImplFromLoop() {
  context_->enroll(*this);

  if (isServer_) {
    initServerSide();
  } else {
    initClientSide();
  }
}

void ConnectionImpl::initClientSide() {
  TP_DCHECK(addr_.has_value());
  auto& connector = context_->getConnector();

  // Request a channel from the RPC Manager.
  diancie::rpc_mgr_request_channel_req_t req;
  std::memset(&req, 0, sizeof(req));
  req.type = diancie::rpc_mgr_msg_type_t::REQUEST_CHANNEL_REQ;
  std::snprintf(
      req.service_name, sizeof(req.service_name), "%s", addr_.value().c_str());
  std::snprintf(
      req.instance_id,
      sizeof(req.instance_id),
      "tp_client_%s",
      id_.c_str());

  if (!connector.send_command(&req, sizeof(req))) {
    setError(TP_CREATE_ERROR(SystemError, "send REQUEST_CHANNEL_REQ", EIO));
    return;
  }

  diancie::rpc_mgr_request_channel_resp_t resp;
  connector.recv_response(&resp, sizeof(resp));
  if (resp.status != diancie::rpc_mgr_status_type_t::STATUS_OK) {
    setError(TP_CREATE_ERROR(SystemError, "REQUEST_CHANNEL_RESP failed", EIO));
    return;
  }

  channelId_ = resp.channel_id;

  // Map DAX/SHM region if not already mapped by context.
  if (context_->getDaxBase() == nullptr) {
    auto mapping = DaxMapping::map(resp.device_path, resp.size, resp.offset);
    if (mapping.base == nullptr) {
      setError(TP_CREATE_ERROR(SystemError, "map DAX/SHM region", errno));
      return;
    }
    context_->setDaxBase(mapping.base, mapping.size);
  }

  // Initialize client-side RPCSession thread.
  void* base = context_->getDaxBase();
  size_t size = context_->getDaxSize();

  // Create a temporary session for thread_init.
  diancie::RPCSession clientSession(base, size);
  int threadId = channelId_ * 2 - 1;  // Client thread ID.
  clientSession.thread_init(threadId);

  // Set up ring buffers. Client swaps inbox/outbox relative to server.
  setupRingBuffers(/*swapInboxOutbox=*/true);

  state_ = ESTABLISHED;
  processReadOperationsFromLoop();
  processWriteOperationsFromLoop();
}

void ConnectionImpl::initServerSide() {
  TP_DCHECK(channelId_ > 0);
  // DAX base should already be set by the listener.
  TP_DCHECK(context_->getDaxBase() != nullptr);

  // Server-side thread_init was already done in ListenerImpl.
  // Set up ring buffers. Server uses layout as-is.
  setupRingBuffers(/*swapInboxOutbox=*/false);

  state_ = ESTABLISHED;
  processReadOperationsFromLoop();
  processWriteOperationsFromLoop();
}

void ConnectionImpl::setupRingBuffers(bool swapInboxOutbox) {
  void* base = context_->getDaxBase();
  TP_DCHECK(base != nullptr);

  // Create RPCConnectionMetaData for this channel.
  metadata_ = std::make_unique<diancie::RPCConnectionMetaData>(
      base, channelId_ - 1);

  // Use the payload area for ring buffers.
  // Layout within payload area:
  //   [0] inbox header
  //   [sizeof(header)] inbox data (kRingBufferSize)
  //   [sizeof(header) + kRingBufferSize] outbox header
  //   [2*sizeof(header) + kRingBufferSize] outbox data (kRingBufferSize)
  using Header = RingBufferHeader<kNumRingbufferRoles>;
  constexpr size_t headerSize = sizeof(Header);

  uint8_t* payloadArea = static_cast<uint8_t*>(metadata_->get_payload_area());

  uint8_t* firstHeaderPtr = payloadArea;
  uint8_t* firstDataPtr = payloadArea + headerSize;
  uint8_t* secondHeaderPtr = payloadArea + headerSize + kRingBufferSize;
  uint8_t* secondDataPtr = payloadArea + 2 * headerSize + kRingBufferSize;

  Header* inboxHeader;
  uint8_t* inboxData;
  Header* outboxHeader;
  uint8_t* outboxData;

  if (!swapInboxOutbox) {
    // Server: first region is inbox, second is outbox.
    inboxHeader = reinterpret_cast<Header*>(firstHeaderPtr);
    inboxData = firstDataPtr;
    outboxHeader = reinterpret_cast<Header*>(secondHeaderPtr);
    outboxData = secondDataPtr;
  } else {
    // Client: first region is outbox, second is inbox (swap).
    outboxHeader = reinterpret_cast<Header*>(firstHeaderPtr);
    outboxData = firstDataPtr;
    inboxHeader = reinterpret_cast<Header*>(secondHeaderPtr);
    inboxData = secondDataPtr;
  }

  // Initialize headers using placement new (only one side should do this).
  // The server side initializes both headers since it maps first.
  if (!swapInboxOutbox) {
    new (inboxHeader) Header(kRingBufferSize);
    new (outboxHeader) Header(kRingBufferSize);
  }

  inboxRb_.emplace(inboxHeader, inboxData);
  outboxRb_.emplace(outboxHeader, outboxData);

  // Determine which QueueEntries to poll and which to toggle.
  // Server reads from client_queue (inbox notification) and
  // server_queue is used for outbox notification.
  // Client reads from server_queue (inbox notification) and
  // client_queue is used for outbox notification.
  diancie::QueueEntry* myInboxQe;
  diancie::QueueEntry* myOutboxQe;

  if (!swapInboxOutbox) {
    // Server: poll client_queue for inbox, server_queue for outbox.
    myInboxQe = metadata_->get_client_queue();
    myOutboxQe = metadata_->get_server_queue();
    peerInboxQe_ = metadata_->get_server_queue();
    peerOutboxQe_ = metadata_->get_client_queue();
  } else {
    // Client: poll server_queue for inbox, client_queue for outbox.
    myInboxQe = metadata_->get_server_queue();
    myOutboxQe = metadata_->get_client_queue();
    peerInboxQe_ = metadata_->get_client_queue();
    peerOutboxQe_ = metadata_->get_server_queue();
  }

  // Register with the poller.
  auto& poller = context_->getPoller();

  inboxPollerToken_ = poller.add(myInboxQe, [this]() {
    TP_VLOG(9) << "Connection " << id_
               << " is reacting to the peer writing to the inbox";
    processReadOperationsFromLoop();
  });

  outboxPollerToken_ = poller.add(myOutboxQe, [this]() {
    TP_VLOG(9) << "Connection " << id_
               << " is reacting to the peer reading from the outbox";
    processWriteOperationsFromLoop();
  });
}

void ConnectionImpl::readImplFromLoop(read_callback_fn fn) {
  readOperations_.emplace_back(std::move(fn));
  processReadOperationsFromLoop();
}

void ConnectionImpl::readImplFromLoop(
    AbstractNopHolder& object,
    read_nop_callback_fn fn) {
  readOperations_.emplace_back(
      &object,
      [fn{std::move(fn)}](
          const Error& error, const void* /* unused */, size_t /* unused */) {
        fn(error);
      });
  processReadOperationsFromLoop();
}

void ConnectionImpl::readImplFromLoop(
    void* ptr,
    size_t length,
    read_callback_fn fn) {
  readOperations_.emplace_back(ptr, length, std::move(fn));
  processReadOperationsFromLoop();
}

void ConnectionImpl::writeImplFromLoop(
    const void* ptr,
    size_t length,
    write_callback_fn fn) {
  writeOperations_.emplace_back(ptr, length, std::move(fn));
  processWriteOperationsFromLoop();
}

void ConnectionImpl::writeImplFromLoop(
    const AbstractNopHolder& object,
    write_callback_fn fn) {
  writeOperations_.emplace_back(&object, std::move(fn));
  processWriteOperationsFromLoop();
}

void ConnectionImpl::processReadOperationsFromLoop() {
  TP_DCHECK(context_->inLoop());

  if (state_ != ESTABLISHED) {
    return;
  }

  Consumer inboxConsumer(inboxRb_.value());
  while (!readOperations_.empty()) {
    RingbufferReadOperation& readOperation = readOperations_.front();
    if (readOperation.handleRead(inboxConsumer) > 0) {
      notifyPeerOutbox();
    }
    if (readOperation.completed()) {
      readOperations_.pop_front();
    } else {
      break;
    }
  }
}

void ConnectionImpl::processWriteOperationsFromLoop() {
  TP_DCHECK(context_->inLoop());

  if (state_ != ESTABLISHED) {
    return;
  }

  Producer outboxProducer(outboxRb_.value());
  while (!writeOperations_.empty()) {
    RingbufferWriteOperation& writeOperation = writeOperations_.front();
    if (writeOperation.handleWrite(outboxProducer) > 0) {
      notifyPeerInbox();
    }
    if (writeOperation.completed()) {
      writeOperations_.pop_front();
    } else {
      break;
    }
  }
}

void ConnectionImpl::notifyPeerInbox() {
  // Toggle the peer's inbox QueueEntry flag to signal data available.
  if (peerInboxQe_) {
    bool currentF = peerInboxQe_->get_flag_F();
    bool currentf = peerInboxQe_->get_flag_f();
    peerInboxQe_->set_entry(0, currentF, !currentf);
  }
}

void ConnectionImpl::notifyPeerOutbox() {
  // Toggle the peer's outbox QueueEntry flag to signal space freed.
  if (peerOutboxQe_) {
    bool currentF = peerOutboxQe_->get_flag_F();
    bool currentf = peerOutboxQe_->get_flag_f();
    peerOutboxQe_->set_entry(0, currentF, !currentf);
  }
}

void ConnectionImpl::handleErrorImpl() {
  for (auto& readOperation : readOperations_) {
    readOperation.handleError(error_);
  }
  readOperations_.clear();
  for (auto& writeOperation : writeOperations_) {
    writeOperation.handleError(error_);
  }
  writeOperations_.clear();

  auto& poller = context_->getPoller();
  if (inboxPollerToken_.has_value()) {
    poller.remove(inboxPollerToken_.value());
    inboxPollerToken_.reset();
  }
  if (outboxPollerToken_.has_value()) {
    poller.remove(outboxPollerToken_.value());
    outboxPollerToken_.reset();
  }

  context_->unenroll(*this);
}

} // namespace xrpc
} // namespace transport
} // namespace tensorpipe
