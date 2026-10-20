/* SPDX-License-Identifier: MIT */
#ifndef INCLUDED_GR_IQSTREAM_HTTP_LISTENER_H
#define INCLUDED_GR_IQSTREAM_HTTP_LISTENER_H
#include "session.h"
#include <gnuradio/iqstream/server.h>
#include <functional>
#include <memory>

namespace gr {
namespace iqstream {
/*! Multiplex HTTP/1 catalogues and HTTP/2 gRPC on one TCP/TLS listener. */
class http_listener
{
public:
    struct websocket_session {
        std::shared_ptr<session> state;
        session::resolver resolve;
        std::function<void()> release;
    };
    using websocket_factory = std::function<websocket_session()>;
    using handoff = std::function<void(int, const std::string&)>;
    http_listener(const std::string& address,
                  const tls_options& tls,
                  std::function<std::vector<stream_listing>()> list,
                  handoff grpc,
                  websocket_factory websocket);
    ~http_listener();
    std::string address() const;
    void shutdown();

private:
    class impl;
    std::unique_ptr<impl> d_impl;
};
} // namespace iqstream
} // namespace gr
#endif
