/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <tensorpipe/transport/xrpc/factory.h>

#include <tensorpipe/transport/context_boilerplate.h>
#include <tensorpipe/transport/xrpc/connection_impl.h>
#include <tensorpipe/transport/xrpc/context_impl.h>
#include <tensorpipe/transport/xrpc/listener_impl.h>

namespace tensorpipe {
namespace transport {
namespace xrpc {

std::shared_ptr<Context> create(
    const std::string& rpcMgrHost,
    int rpcMgrPort) {
  return std::make_shared<
      ContextBoilerplate<ContextImpl, ListenerImpl, ConnectionImpl>>(
      rpcMgrHost, rpcMgrPort);
}

} // namespace xrpc
} // namespace transport
} // namespace tensorpipe
