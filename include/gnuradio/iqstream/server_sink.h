/* SPDX-License-Identifier: GPL-3.0-or-later */
#ifndef INCLUDED_GR_IQSTREAM_SERVER_SINK_H
#define INCLUDED_GR_IQSTREAM_SERVER_SINK_H
#include <gnuradio/iqstream/server.h>
#include <gnuradio/sync_block.h>

namespace gr {
namespace iqstream {
/*! One server sink stream. Layout and encoding are immutable.
 * Sending blocks gracefully finish their accepted prefix on stop().
 * Explicit cancel() aborts instead. A new run starts a new session.
 */
class IQSTREAM_API server_sink : virtual public gr::sync_block
{
public:
    using sptr = std::shared_ptr<server_sink>;
    static sptr make(server::sptr listener,
                     const std::string& resource,
                     sample_layout layout = sample_layout::COMPLEX,
                     const stream_options& options = {});
    virtual session_status status() const = 0;
    virtual void cancel() = 0;
};
} // namespace iqstream
} // namespace gr

#endif /* INCLUDED_GR_IQSTREAM_SERVER_SINK_H */
