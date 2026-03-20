/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/xrpc/listener_impl.h>

#include <cstring>
#include <functional>

#include <tensorpipe/common/callback.h>
#include <tensorpipe/common/defs.h>
#include <tensorpipe/transport/error.h>
#include <tensorpipe/transport/xrpc/connection_impl.h>
#include <tensorpipe/transport/xrpc/context_impl.h>
#include <tensorpipe/transport/xrpc/dax_util.h>

namespace tensorpipe {
namespace transport {
namespace xrpc {

ListenerImpl::ListenerImpl(
    ConstructorToken token,
    std::shared_ptr<ContextImpl> context,
    std::string id,
    std::string addr)
    : ListenerImplBoilerplate<ContextImpl, ListenerImpl, ConnectionImpl>(
          token,
          std::move(context),
          std::move(id)),
      serviceName_(std::move(addr)) {}

void ListenerImpl::initImplFromLoop() {
  context_->enroll(*this);

  auto& connector = context_->getConnector();

  // Register this service with the RPC Manager.
  diancie::rpc_mgr_register_service_req_t req;
  std::memset(&req, 0, sizeof(req));
  req.type = diancie::rpc_mgr_msg_type_t::REGISTER_SERVICE_REQ;
  std::snprintf(
      req.service_name, sizeof(req.service_name), "%s", serviceName_.c_str());
  std::snprintf(
      req.instance_id,
      sizeof(req.instance_id),
      "tp_listener_%s",
      id_.c_str());

  if (!connector.send_command(&req, sizeof(req))) {
    setError(TP_CREATE_ERROR(SystemError, "send REGISTER_SERVICE_REQ", EIO));
    return;
  }

  diancie::rpc_mgr_register_service_resp_t resp;
  connector.recv_response(&resp, sizeof(resp));
  if (resp.status != diancie::rpc_mgr_status_type_t::STATUS_OK) {
    setError(TP_CREATE_ERROR(SystemError, "REGISTER_SERVICE_RESP failed", EIO));
    return;
  }

  // Map the DAX/SHM region (matches Diancie's map_dax_dev behavior).
  auto mapping = DaxMapping::map(resp.device_path, resp.size, resp.offset);
  if (mapping.base == nullptr) {
    setError(TP_CREATE_ERROR(SystemError, "map DAX/SHM region", errno));
    return;
  }
  void* base = mapping.base;

  context_->setDaxBase(base, resp.size);
  context_->setMaster(true);

  // Zero the device and initialize the heap.
  std::memset(base, 0, resp.size);
  session_ = std::make_unique<diancie::RPCSession>(base, resp.size);
  session_->master_fresh_init();
  context_->setSession(session_.get());
}

void ListenerImpl::acceptImplFromLoop(accept_callback_fn fn) {
  fns_.push_back(std::move(fn));

  // If this is the first pending accept, start polling for clients.
  if (fns_.size() == 1) {
    pollForClients();
  }
}

void ListenerImpl::pollForClients() {
  context_->deferToLoop([this]() {
    if (fns_.empty()) {
      return;
    }

    auto& connector = context_->getConnector();

    // Non-blocking check for incoming client notifications.
    auto msgType = connector.check_for_event(0);
    if (!msgType.has_value()) {
      // No event yet; re-schedule.
      pollForClients();
      return;
    }

    if (msgType.value() != diancie::rpc_mgr_msg_type_t::NOTIFY_CLIENT_REQ) {
      // Unexpected message type; re-schedule.
      pollForClients();
      return;
    }

    // Read the notification.
    diancie::rpc_mgr_new_client_notify_t notify;
    connector.recv_response(&notify, sizeof(notify));

    // Initialize server-side thread for this channel.
    diancie::RPCSession serverSession(
        context_->getDaxBase(), context_->getDaxSize());
    serverSession.thread_init(notify.channel_id * 2);

    // Send ACK back to RPC Manager.
    diancie::rpc_mgr_new_client_notify_ack_t ack;
    ack.status = diancie::rpc_mgr_status_type_t::STATUS_OK;
    connector.send_command(&ack, sizeof(ack));

    TP_DCHECK(!fns_.empty());
    auto fn = std::move(fns_.front());
    fns_.pop_front();

    // Create a server-side connection with the known channel_id.
    fn(Error::kSuccess,
       createAndInitConnection(notify.channel_id, /*isServer=*/true));

    // Continue polling if more accepts are pending.
    if (!fns_.empty()) {
      pollForClients();
    }
  });
}

std::string ListenerImpl::addrImplFromLoop() const {
  return serviceName_;
}

void ListenerImpl::handleErrorImpl() {
  for (auto& fn : fns_) {
    fn(error_, std::shared_ptr<Connection>());
  }
  fns_.clear();

  context_->unenroll(*this);
}

} // namespace xrpc
} // namespace transport
} // namespace tensorpipe
