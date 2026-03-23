// XRPC transport benchmark — server process
//
// Usage:
//   1. Start rpc_mgr:   ./diancie/rpc_mgr &
//   2. Start server:    ./apps/xrpc_bench_server
//   3. Start client:    ./apps/xrpc_bench_client [payload_size] [num_iterations]
//
// The server accepts a connection and echoes every message back until the
// client disconnects (readDescriptor returns an error).

#include <atomic>
#include <chrono>
#include <cstring>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

#include <tensorpipe/tensorpipe.h>
#include <tensorpipe/transport/xrpc/factory.h>

static void echoLoop(
    std::shared_ptr<tensorpipe::Pipe> pipe,
    std::atomic<bool>& done) {
  pipe->readDescriptor(
      [pipe, &done](
          const tensorpipe::Error& err, tensorpipe::Descriptor desc) {
        if (err) {
          // Client disconnected or error — exit gracefully.
          done = true;
          return;
        }

        // Allocate receive buffers based on descriptor.
        auto payloadBufs = std::make_shared<std::vector<std::vector<char>>>();
        auto tensorBufs = std::make_shared<std::vector<std::vector<char>>>();

        tensorpipe::Allocation alloc;
        alloc.payloads.resize(desc.payloads.size());
        payloadBufs->resize(desc.payloads.size());
        for (size_t i = 0; i < desc.payloads.size(); ++i) {
          (*payloadBufs)[i].resize(desc.payloads[i].length);
          alloc.payloads[i].data = (*payloadBufs)[i].data();
        }

        alloc.tensors.resize(desc.tensors.size());
        tensorBufs->resize(desc.tensors.size());
        for (size_t i = 0; i < desc.tensors.size(); ++i) {
          (*tensorBufs)[i].resize(desc.tensors[i].length);
          alloc.tensors[i].buffer =
              tensorpipe::CpuBuffer{(*tensorBufs)[i].data()};
        }

        pipe->read(
            std::move(alloc),
            [pipe, &done, payloadBufs, tensorBufs, desc](
                const tensorpipe::Error& err) {
              if (err) {
                done = true;
                return;
              }

              // Echo the received data back.
              tensorpipe::Message reply;
              reply.payloads.resize(payloadBufs->size());
              for (size_t i = 0; i < payloadBufs->size(); ++i) {
                reply.payloads[i].data = (*payloadBufs)[i].data();
                reply.payloads[i].length = (*payloadBufs)[i].size();
              }
              reply.tensors.resize(tensorBufs->size());
              for (size_t i = 0; i < tensorBufs->size(); ++i) {
                reply.tensors[i].buffer =
                    tensorpipe::CpuBuffer{(*tensorBufs)[i].data()};
                reply.tensors[i].length = (*tensorBufs)[i].size();
                reply.tensors[i].targetDevice =
                    desc.tensors[i].sourceDevice;
              }

              pipe->write(
                  std::move(reply),
                  [pipe, &done, payloadBufs, tensorBufs](
                      const tensorpipe::Error& err) {
                    if (err) {
                      done = true;
                      return;
                    }
                    echoLoop(pipe, done);
                  });
            });
      });
}

int main() {
  tensorpipe::Context ctx;
  ctx.registerTransport(
      0, "xrpc", tensorpipe::transport::xrpc::create("127.0.0.1", 12345));
  ctx.registerChannel(0, "basic", tensorpipe::channel::basic::create());

  const std::string addr = "xrpc://xrpc_bench_svc";
  std::atomic<bool> done{false};

  auto listener = ctx.listen({addr});
  std::cout << "[server] listening on " << addr << "\n";

  listener->accept([&](const tensorpipe::Error& err,
                        std::shared_ptr<tensorpipe::Pipe> pipe) {
    if (err) {
      std::cerr << "[server] accept error: " << err.what() << "\n";
      done = true;
      return;
    }
    std::cout << "[server] accepted connection\n";
    echoLoop(std::move(pipe), done);
  });

  while (!done.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  listener->close();
  ctx.close();
  ctx.join();

  std::cout << "[server] done\n";
  return 0;
}
