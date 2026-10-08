/* SPDX-License-Identifier: MIT */
#ifndef INCLUDED_GR_IQSTREAM_ALIGN_STREAMS_H
#define INCLUDED_GR_IQSTREAM_ALIGN_STREAMS_H
#include <gnuradio/block.h>
#include <gnuradio/iqstream/api.h>
#include <cstddef>

namespace gr {
namespace iqstream {
/*! Align two streams using UINT64 rustradio.iq.absolute_sample_index tags.
 * Port i's output has the same item size as its input. The ports can differ.
 * Drop samples before the first index anchor and samples with lower absolute
 * indices until both inputs agree. Indices advance once per item; gap_samples
 * tags advance the timeline before their associated retained sample. Reanchor
 * on later absolute index tags. Preserve retained tags on their own output and
 * emit absolute index anchors after alignment drops. No resampling is performed;
 * the inputs must count items on the same timeline and at the same rate.
 */
class IQSTREAM_API align_streams : virtual public gr::block
{
public:
    using sptr = std::shared_ptr<align_streams>;
    static sptr make(size_t itemsize0, size_t itemsize1);
};
} // namespace iqstream
} // namespace gr
#endif /* INCLUDED_GR_IQSTREAM_ALIGN_STREAMS_H */
