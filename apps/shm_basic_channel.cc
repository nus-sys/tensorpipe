// Minimal example: SHM transport + basic channel
//
// The SHM transport carries control messages between two pipes on the same
// machine.  The basic channel uses that transport to move tensor payloads.
// Both the listener ("server") and the connector ("client") run in the same
// process so the example is self-contained.

#include <atomic>
#include <chrono>
#include <cstring>
#include <iostream>
#include <thread>
#include <vector>

#include <tensorpipe/tensorpipe.h>

int main()
{
  // ── Context setup ────────────────────────────────────────────────────────
  // A single Context is shared between listener and connector here since both
  // run in the same process.  In a real deployment each process creates its
  // own Context with the same transport/channel registrations.

  tensorpipe::Context ctx;
  // Priority 0: SHM transport (same machine, fast ring-buffer IPC)
  ctx.registerTransport(0, "shm", tensorpipe::transport::shm::create());
  // Priority 0: basic channel (piggybacks on transport connections)
  ctx.registerChannel(0, "basic", tensorpipe::channel::basic::create());

  const std::string addr = "shm://shm_basic_example";

  // ── Server side ──────────────────────────────────────────────────────────
  std::atomic<bool> done{false};
  auto listener = ctx.listen({addr});

  listener->accept([&done](const tensorpipe::Error &err,
                           std::shared_ptr<tensorpipe::Pipe> pipe)
                   {
    if (err) {
      std::cerr << "[server] accept error: " << err.what() << "\n";
      done = true;
      return;
    }

    // Step 1: read the descriptor to learn payload/tensor sizes.
    pipe->readDescriptor([pipe, &done](const tensorpipe::Error& err,
                                       tensorpipe::Descriptor desc) {
      if (err) {
        std::cerr << "[server] readDescriptor error: " << err.what() << "\n";
        done = true;
        return;
      }

      // Allocate buffers based on the descriptor lengths.
      auto payloadBuf = std::make_shared<std::vector<char>>(
          desc.payloads[0].length);
      auto tensorBuf = std::make_shared<std::vector<float>>(
          desc.tensors[0].length / sizeof(float));

      tensorpipe::Allocation alloc;
      alloc.payloads.resize(1);
      alloc.payloads[0].data = payloadBuf->data();

      alloc.tensors.resize(1);
      alloc.tensors[0].buffer = tensorpipe::CpuBuffer{tensorBuf->data()};

      // Step 2: read the actual data into our buffers.
      pipe->read(std::move(alloc),
                 [pipe, payloadBuf, tensorBuf, &done](
                     const tensorpipe::Error& err) {
                   if (err) {
                     std::cerr << "[server] read error: " << err.what() << "\n";
                     done = true;
                     return;
                   }

                   std::string text(payloadBuf->begin(), payloadBuf->end());
                   std::cout << "[server] payload : \"" << text << "\"\n";

                   std::cout << "[server] tensor  : [";
                   for (std::size_t i = 0; i < tensorBuf->size(); ++i) {
                     if (i) std::cout << ", ";
                     std::cout << (*tensorBuf)[i];
                   }
                   std::cout << "]\n";

                   done = true;
                   pipe->close();
                 });
    }); });

  // ── Client side ──────────────────────────────────────────────────────────
  auto pipe = ctx.connect(addr);

  // Payload: a plain byte string.
  std::string text = "hello via SHM + basic channel";

  // Tensor: a small float array sent through the basic channel.
  std::vector<float> tensor = {1.0f, 2.0f, 3.0f, 4.0f};

  tensorpipe::Message msg;

  msg.payloads.resize(1);
  msg.payloads[0].data = text.data();
  msg.payloads[0].length = text.size();

  msg.tensors.resize(1);
  msg.tensors[0].buffer = tensorpipe::CpuBuffer{tensor.data()};
  msg.tensors[0].length = tensor.size() * sizeof(float);
  // Leave msg.tensors[0].targetDevice as default (CPU) so the basic channel
  // handles the transfer.

  pipe->write(std::move(msg), [](const tensorpipe::Error &err)
              {
    if (err) {
      std::cerr << "[client] write error: " << err.what() << "\n";
    } else {
      std::cout << "[client] message sent\n";
    } });

  // ── Wait and clean up ────────────────────────────────────────────────────
  while (!done.load())
  {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  listener->close();
  pipe->close();
  ctx.close();
  ctx.join();

  return 0;
}
