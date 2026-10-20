/* SPDX-License-Identifier: MIT */
#ifndef INCLUDED_GR_IQSTREAM_SERVER_H
#define INCLUDED_GR_IQSTREAM_SERVER_H
#include <gnuradio/iqstream/options.h>
#include <gnuradio/stream_observer.h>
#include <memory>

namespace gr {
namespace iqstream {
class server_impl;
struct IQSTREAM_API stream_listing {
    std::string name;
    sample_layout layout = sample_layout::REAL;
    std::optional<double> sample_rate_hz;
    bool ended = false;
    std::optional<gr::stream_info> origin;
};
/*! Shared gRPC and HTTP catalogue listener.
 * GET /iq/v1/streams returns a RustRadio-compatible JSON array with CORS headers.
 * Resources are registered by server_source/server_sink.
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
    /*! Enable process-wide, nonblocking float32/complex64 scalar downloads.
     * Clients must select ALLOW_GAPS. Observation starts at the current position.
     */
    void register_all_streams(metadata_profile profile = metadata_profile::NATIVE);
    /*! Fallback rate for observed outputs without an explicit rate.
     * Zero leaves unknown rates unspecified. Applies to new sessions and listings;
     * explicit output rates and manually registered resources take precedence.
     */
    void register_all_streams(metadata_profile profile, double default_sample_rate_hz);
    std::vector<stream_listing> streams() const;
    void shutdown();

private:
    friend class server_impl;
    explicit server(std::shared_ptr<server_impl> implementation);
    std::shared_ptr<server_impl> d_impl;
};
} // namespace iqstream
} // namespace gr

#endif /* INCLUDED_GR_IQSTREAM_SERVER_H */
