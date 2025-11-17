#include <iostream>
#include <atomic>
#include <chrono>
#include <thread>
#include <tensorpipe/tensorpipe.h>

int main()
{
    tensorpipe::Context ctx;
    ctx.registerTransport(0, "shm", tensorpipe::transport::shm::create());
    const std::string addr = "shm://hello";
    auto listener = ctx.listen({addr});
    std::atomic<bool> done{false};

    listener->accept(
        [&](const tensorpipe::Error &error,
            std::shared_ptr<tensorpipe::Pipe> pipe)
        {
            if (error)
            {
                std::cerr << "accept error: " << error.what() << "\n";
                done = true;
                return;
            }

            pipe->readDescriptor(
                [pipe, &done](const tensorpipe::Error &err,
                              tensorpipe::Descriptor desc)
                {
                    if (err)
                    {
                        std::cerr << "readDescriptor error: " << err.what() << "\n";
                        done = true;
                        return;
                    }

                    if (desc.payloads.empty())
                    {
                        std::cerr << "no payloads in message\n";
                        done = true;
                        return;
                    }

                    std::size_t len = desc.payloads[0].length;
                    auto buffer = std::make_shared<std::vector<char>>(len);
                    tensorpipe::Allocation alloc;
                    alloc.payloads.resize(desc.payloads.size());
                    alloc.payloads[0].data = buffer->data();

                    pipe->read(
                        std::move(alloc),
                        [pipe, buffer, &done](const tensorpipe::Error &err2)
                        {
                            if (err2)
                            {
                                std::cerr << "read error: " << err2.what() << "\n";
                                done = true;
                                return;
                            }

                            std::string received(buffer->begin(), buffer->end());
                            std::cout << "received: " << received << "\n";

                            done = true;
                            pipe->close();
                        });
                });
        });

    auto pipe = ctx.connect(addr);

    std::string text = "hello from tensorpipe";
    tensorpipe::Message msg;
    msg.payloads.resize(1);
    msg.payloads[0].data = const_cast<char *>(text.data());
    msg.payloads[0].length = text.size();

    pipe->write(
        std::move(msg),
        [](const tensorpipe::Error &errWrite)
        {
            if (errWrite)
            {
                std::cerr << "write error: " << errWrite.what() << "\n";
            }
        });

    // Wait for the server side to finish.
    while (!done.load())
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    listener->close();
    pipe->close();
    ctx.close();
    ctx.join();
}
