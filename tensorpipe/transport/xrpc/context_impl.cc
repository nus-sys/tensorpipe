/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/xrpc/context_impl.h>

#include <tensorpipe/common/system.h>
#include <tensorpipe/transport/xrpc/connection_impl.h>
#include <tensorpipe/transport/xrpc/listener_impl.h>

namespace tensorpipe {
namespace transport {
namespace xrpc {

namespace {

const std::string kDomainDescriptorPrefix{"xrpc:"};

} // namespace

std::shared_ptr<ContextImpl> ContextImpl::create(
    const std::string& rpcMgrHost,
    int rpcMgrPort) {
  std::ostringstream oss;
  oss << kDomainDescriptorPrefix;

  // This transport only works across processes on the same machine.
  auto bootID = getBootID();
  TP_THROW_ASSERT_IF(!bootID.has_value()) << "Unable to read boot_id";
  oss << bootID.value();

  std::string domainDescriptor = oss.str();
  TP_VLOG(8) << "The domain descriptor for XRPC is " << domainDescriptor;

  auto connector = std::make_unique<diancie::DAXCXLConnector>(
      rpcMgrHost, rpcMgrPort);

  return std::make_shared<ContextImpl>(
      std::move(domainDescriptor), std::move(connector));
}

ContextImpl::ContextImpl(
    std::string domainDescriptor,
    std::unique_ptr<diancie::DAXCXLConnector> connector)
    : ContextImplBoilerplate<ContextImpl, ListenerImpl, ConnectionImpl>(
          std::move(domainDescriptor)),
      connector_(std::move(connector)) {}

void ContextImpl::handleErrorImpl() {
  poller_.close();
}

void ContextImpl::joinImpl() {
  poller_.join();
}

bool ContextImpl::inLoop() const {
  return poller_.inLoop();
}

void ContextImpl::deferToLoop(std::function<void()> fn) {
  poller_.deferToLoop(std::move(fn));
}

Poller& ContextImpl::getPoller() {
  return poller_;
}

diancie::DAXCXLConnector& ContextImpl::getConnector() {
  return *connector_;
}

void ContextImpl::setDaxBase(void* base, size_t size) {
  daxBase_ = base;
  daxSize_ = size;
}

void* ContextImpl::getDaxBase() const {
  return daxBase_;
}

size_t ContextImpl::getDaxSize() const {
  return daxSize_;
}

void ContextImpl::setSession(diancie::RPCSession* session) {
  session_ = session;
}

diancie::RPCSession* ContextImpl::getSession() const {
  return session_;
}

bool ContextImpl::isMaster() const {
  return isMaster_;
}

void ContextImpl::setMaster(bool master) {
  isMaster_ = master;
}

} // namespace xrpc
} // namespace transport
} // namespace tensorpipe
