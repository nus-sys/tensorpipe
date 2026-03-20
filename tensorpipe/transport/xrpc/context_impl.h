/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <functional>
#include <memory>
#include <string>

#include <tensorpipe/transport/context_impl_boilerplate.h>
#include <tensorpipe/transport/xrpc/poller.h>

#include <dax_connector.hpp>
#include <diancie_shm/rpc_session.hpp>

namespace tensorpipe {
namespace transport {
namespace xrpc {

class ConnectionImpl;
class ListenerImpl;

class ContextImpl final
    : public ContextImplBoilerplate<ContextImpl, ListenerImpl, ConnectionImpl> {
 public:
  static std::shared_ptr<ContextImpl> create(
      const std::string& rpcMgrHost = "localhost",
      int rpcMgrPort = 12345);

  ContextImpl(
      std::string domainDescriptor,
      std::unique_ptr<diancie::DAXCXLConnector> connector);

  // Implement the DeferredExecutor interface.
  bool inLoop() const override;
  void deferToLoop(std::function<void()> fn) override;

  // Expose Poller for ConnectionImpl/ListenerImpl.
  Poller& getPoller();

  // Expose DAXCXLConnector for ConnectionImpl/ListenerImpl.
  diancie::DAXCXLConnector& getConnector();

  // DAX memory base and session, set by ListenerImpl (server side)
  // or ConnectionImpl (client side) upon first mapping.
  void setDaxBase(void* base, size_t size);
  void* getDaxBase() const;
  size_t getDaxSize() const;

  void setSession(diancie::RPCSession* session);
  diancie::RPCSession* getSession() const;

  bool isMaster() const;
  void setMaster(bool master);

 protected:
  // Implement the entry points called by ContextImplBoilerplate.
  void handleErrorImpl() override;
  void joinImpl() override;

 private:
  Poller poller_;
  std::unique_ptr<diancie::DAXCXLConnector> connector_;

  void* daxBase_ = nullptr;
  size_t daxSize_ = 0;
  diancie::RPCSession* session_ = nullptr;
  bool isMaster_ = false;
};

} // namespace xrpc
} // namespace transport
} // namespace tensorpipe
