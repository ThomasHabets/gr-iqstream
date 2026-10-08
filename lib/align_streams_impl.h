/* SPDX-License-Identifier: MIT */
#ifndef INCLUDED_GR_IQSTREAM_ALIGN_STREAMS_IMPL_H
#define INCLUDED_GR_IQSTREAM_ALIGN_STREAMS_IMPL_H
#include <gnuradio/iqstream/align_streams.h>
#include <array>
#include <optional>

namespace gr {
namespace iqstream {
class align_streams_impl : public align_streams
{
public:
    align_streams_impl(size_t itemsize0, size_t itemsize1);
    bool start() override;
    void forecast(int noutput_items, gr_vector_int& required) override;
    int general_work(int noutput_items,
                     gr_vector_int& ninput_items,
                     gr_vector_const_void_star& input_items,
                     gr_vector_void_star& output_items) override;

private:
    struct cursor {
        std::optional<uint64_t> index;
        std::optional<uint64_t> applied_offset;
        bool exhausted = false;
    };
    void apply_markers(int port, const std::vector<gr::tag_t>& tags);
    int segment_size(int port, int available, const std::vector<gr::tag_t>& tags);
    void advance_cursor(int port, int count);
    const std::array<size_t, 2> d_itemsize;
    std::array<cursor, 2> d_cursor;
    std::optional<uint64_t> d_output_next;
    pmt::pmt_t d_index_key;
    pmt::pmt_t d_gap_key;
};
} // namespace iqstream
} // namespace gr
#endif /* INCLUDED_GR_IQSTREAM_ALIGN_STREAMS_IMPL_H */
