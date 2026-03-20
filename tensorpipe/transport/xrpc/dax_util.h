/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstddef>
#include <cstring>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace tensorpipe {
namespace transport {
namespace xrpc {

// Maps the DAX device region, matching Diancie's behavior.
struct DaxMapping {
  void* base = nullptr;
  size_t size = 0;

  static DaxMapping map(
      const char* devicePath,
      size_t devSize,
      size_t offset) {
    DaxMapping m;
    m.size = devSize;

    int fd = ::open(devicePath, O_RDWR);
    if (fd < 0) {
      return m;
    }
    m.base = ::mmap(
        nullptr, devSize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, offset);
    if (m.base == MAP_FAILED) {
      m.base = nullptr;
    }
    ::close(fd);

    return m;
  }
};

} // namespace xrpc
} // namespace transport
} // namespace tensorpipe
