/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>
#include <string>

#include <tensorpipe/transport/context.h>

namespace tensorpipe {
namespace transport {
namespace xrpc {

std::shared_ptr<Context> create(
    const std::string& rpcMgrHost = "localhost",
    int rpcMgrPort = 12345);

} // namespace xrpc
} // namespace transport
} // namespace tensorpipe
