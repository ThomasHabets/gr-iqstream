/* SPDX-License-Identifier: MIT */
#ifndef INCLUDED_GR_IQSTREAM_SERVER_H
#define INCLUDED_GR_IQSTREAM_SERVER_H
#include <gnuradio/iqstream/options.h>
#include <memory>

namespace gr {
namespace iqstream {
class server_impl;
/*! Shared gRPC listener. Resources are registered by server_source/server_sink.
 * Construction starts the listener; shutdown cancels remaining sessions.
 */
class IQSTREAM_API server
{
public:
    using sptr = std::shared_ptr<server>;
    static sptr make(const std::string& address = "127.0.0.1:50051",
                     const tls_options& tls = {});
    ~server();
    std::string address() const;
    void shutdown();

private:
    friend class server_impl;
    explicit server(std::shared_ptr<server_impl> implementation);
    std::shared_ptr<server_impl> d_impl;
};
} // namespace iqstream
} // namespace gr

#endif /* INCLUDED_GR_IQSTREAM_SERVER_H */
