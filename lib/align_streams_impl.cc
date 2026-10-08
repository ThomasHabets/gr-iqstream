/* SPDX-License-Identifier: MIT */
#include "align_streams_impl.h"
#include <gnuradio/io_signature.h>
#include <gnuradio/sptr_magic.h>
#include <algorithm>
#include <cstring>
#include <limits>
#include <stdexcept>

namespace gr {
namespace iqstream {
namespace {
int checked_itemsize(size_t size)
{
    if (!size || size > static_cast<size_t>(std::numeric_limits<int>::max()))
        throw std::invalid_argument("align_streams item sizes must be positive ints");
    return static_cast<int>(size);
}
pmt::pmt_t checked_tag_key(const std::string& key)
{
    if (key.empty() || key == "rustradio.iq.gap_samples")
        throw std::invalid_argument("align_streams index tag keys must be nonempty "
                                    "and distinct from rustradio.iq.gap_samples");
    return pmt::intern(key);
}
uint64_t index_value(const gr::tag_t& tag)
{
    if (!pmt::is_uint64(tag.value))
        throw std::runtime_error("align_streams timing tags must contain PMT uint64");
    return pmt::to_uint64(tag.value);
}
uint64_t add_index(uint64_t index, uint64_t count)
{
    if (count > UINT64_MAX - index)
        throw std::runtime_error("align_streams absolute sample index overflow");
    return index + count;
}
} // namespace
align_streams::sptr align_streams::make(size_t itemsize0,
                                        size_t itemsize1,
                                        const std::string& tag_key0,
                                        const std::string& tag_key1)
{
    return gnuradio::make_block_sptr<align_streams_impl>(
        itemsize0, itemsize1, tag_key0, tag_key1);
}
align_streams_impl::align_streams_impl(size_t itemsize0,
                                       size_t itemsize1,
                                       const std::string& tag_key0,
                                       const std::string& tag_key1)
    : gr::block("align_streams",
                gr::io_signature::makev(
                    2, 2, { checked_itemsize(itemsize0), checked_itemsize(itemsize1) }),
                gr::io_signature::makev(
                    2, 2, { checked_itemsize(itemsize0), checked_itemsize(itemsize1) })),
      d_itemsize{ itemsize0, itemsize1 },
      d_index_keys{ checked_tag_key(tag_key0), checked_tag_key(tag_key1) },
      d_gap_key(pmt::intern("rustradio.iq.gap_samples"))
{
    set_tag_propagation_policy(TPP_DONT);
}
bool align_streams_impl::start()
{
    d_cursor = {};
    d_output_next.reset();
    return true;
}
void align_streams_impl::forecast(int, gr_vector_int& required)
{
    // Alignment may consume only one input, regardless of requested output size.
    required[0] = required[1] = 1;
}
void align_streams_impl::apply_markers(int port, const std::vector<gr::tag_t>& tags)
{
    auto& c = d_cursor[port];
    const auto offset = nitems_read(port);
    if (c.applied_offset == offset)
        return;
    if (c.exhausted)
        throw std::runtime_error("align_streams samples follow UINT64_MAX index");
    std::optional<uint64_t> anchor;
    uint64_t gap = 0;
    for (const auto& tag : tags) {
        if (tag.offset != offset)
            continue;
        if (tag.key == d_index_keys[port]) {
            const auto value = index_value(tag);
            if (anchor && *anchor != value)
                throw std::runtime_error("align_streams conflicting index anchors");
            anchor = value;
        } else if (tag.key == d_gap_key)
            gap = add_index(gap, index_value(tag));
    }
    if (anchor) {
        if (c.index && *anchor < *c.index)
            throw std::runtime_error("align_streams index anchor moved backwards");
        // An explicit anchor includes any gap at this same offset.
        c.index = anchor;
    } else if (c.index)
        c.index = add_index(*c.index, gap);
    c.applied_offset = offset;
}
int align_streams_impl::segment_size(int port,
                                     int available,
                                     const std::vector<gr::tag_t>& tags)
{
    const auto offset = nitems_read(port);
    for (const auto& tag : tags)
        if (tag.offset > offset &&
            (tag.key == d_index_keys[port] || tag.key == d_gap_key))
            return static_cast<int>(tag.offset - offset);
    return available;
}
void align_streams_impl::advance_cursor(int port, int count)
{
    auto& c = d_cursor[port];
    if (static_cast<uint64_t>(count - 1) > UINT64_MAX - *c.index)
        throw std::runtime_error("align_streams absolute sample index overflow");
    if (static_cast<uint64_t>(count - 1) == UINT64_MAX - *c.index) {
        c.index.reset();
        c.exhausted = true;
    } else
        c.index = add_index(*c.index, count);
    consume(port, count);
}
int align_streams_impl::general_work(int noutput_items,
                                     gr_vector_int& available,
                                     gr_vector_const_void_star& input,
                                     gr_vector_void_star& output)
{
    std::array<std::vector<gr::tag_t>, 2> tags;
    std::array<int, 2> segments;
    bool searching = false;
    for (int port = 0; port < 2; ++port) {
        if (!available[port])
            return 0;
        const auto first = nitems_read(port);
        get_tags_in_range(tags[port], port, first, add_index(first, available[port]));
        std::stable_sort(
            tags[port].begin(), tags[port].end(), [](const auto& a, const auto& b) {
                return a.offset < b.offset;
            });
        apply_markers(port, tags[port]);
        if (!d_cursor[port].index) {
            int discard = available[port];
            for (const auto& tag : tags[port]) {
                if (tag.key == d_index_keys[port]) {
                    discard = static_cast<int>(tag.offset - first);
                    break;
                }
            }
            consume(port, discard);
            searching = true;
        }
        segments[port] = segment_size(port, available[port], tags[port]);
    }
    if (searching)
        return 0;
    const auto index0 = *d_cursor[0].index;
    const auto index1 = *d_cursor[1].index;
    if (index0 != index1) {
        const int port = index0 < index1 ? 0 : 1;
        const auto distance = port == 0 ? index1 - index0 : index0 - index1;
        const int discard =
            static_cast<int>(std::min<uint64_t>(distance, segments[port]));
        advance_cursor(port, discard);
        return 0;
    }
    int count = std::min({ noutput_items, segments[0], segments[1] });
    if (!count)
        return 0;
    const auto room = UINT64_MAX - index0;
    if (static_cast<uint64_t>(count - 1) > room)
        count = static_cast<int>(room + 1);
    for (int port = 0; port < 2; ++port) {
        std::memcpy(output[port], input[port], count * d_itemsize[port]);
        const auto first = nitems_read(port);
        const auto end = add_index(first, count);
        const auto written = nitems_written(port);
        bool has_anchor = false;
        for (auto tag : tags[port]) {
            if (tag.offset >= end)
                break;
            if (tag.key == d_index_keys[port])
                has_anchor = true;
            tag.offset = add_index(written, tag.offset - first);
            add_item_tag(port, tag);
        }
        if ((!d_output_next || *d_output_next != index0) && !has_anchor)
            add_item_tag(port, written, d_index_keys[port], pmt::from_uint64(index0));
        advance_cursor(port, count);
    }
    d_output_next = d_cursor[0].index;
    return count;
}
} // namespace iqstream
} // namespace gr
