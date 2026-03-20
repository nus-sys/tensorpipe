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

#include <tensorpipe/transport/listener_impl_boilerplate.h>

#include <dax_connector.hpp>
#include <diancie_shm/rpc_session.hpp>
#include <rpc_mgr_msg.hpp>

namespace tensorpipe {
namespace transport {
namespace xrpc {

class ConnectionImpl;
class ContextImpl;

class ListenerImpl final
    : public ListenerImplBoilerplate<ContextImpl, ListenerImpl, ConnectionImpl> {
 public:
  // Create a listener that registers the specified service name
  // with the RPC Manager.
  ListenerImpl(
      ConstructorToken token,
      std::shared_ptr<ContextImpl> context,
      std::string id,
      std::string addr);

 protected:
  // Implement the entry points called by ListenerImplBoilerplate.
  void initImplFromLoop() override;
  void acceptImplFromLoop(accept_callback_fn fn) override;
  std::string addrImplFromLoop() const override;
  void handleErrorImpl() override;

 private:
  std::string serviceName_;
  std::deque<accept_callback_fn> fns_;

  // Owned RPCSession for the master side.
  std::unique_ptr<diancie::RPCSession> session_;

  // Poll the connector for incoming NOTIFY_CLIENT_REQ messages.
  void pollForClients();
};

} // namespace xrpc
} // namespace transport
} // namespace tensorpipe
