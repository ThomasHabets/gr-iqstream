/* SPDX-License-Identifier: GPL-3.0-or-later */
#ifndef INCLUDED_GR_IQSTREAM_SOURCE_H
#define INCLUDED_GR_IQSTREAM_SOURCE_H
#include <gnuradio/iqstream/server.h>
#include <gnuradio/sync_block.h>

namespace gr {
namespace iqstream {
/*! One source stream. Layout and encoding are immutable.
 * Sending blocks gracefully finish their accepted prefix on stop().
 * Explicit cancel() aborts instead. A new run starts a new session.
 */
class IQSTREAM_API source : virtual public gr::sync_block
{
public:
    using sptr = std::shared_ptr<source>;
    static sptr make(const std::string& address,
                     const std::string& resource,
                     sample_layout layout = sample_layout::COMPLEX,
                     const stream_options& options = {});
    virtual session_status status() const = 0;
    virtual void cancel() = 0;
};
} // namespace iqstream
} // namespace gr

#endif /* INCLUDED_GR_IQSTREAM_SOURCE_H */
