/* SPDX-License-Identifier: MIT */
#include "test.h"
#include <gnuradio/blocks/vector_sink.h>
#include <gnuradio/blocks/vector_source.h>
#include <gnuradio/iqstream/align_streams.h>
#include <gnuradio/top_block.h>
#include <array>
#include <numeric>
#include <optional>

using namespace gr::iqstream;
namespace {
gr::tag_t timing_tag(uint64_t offset, const char* key, uint64_t value)
{
    gr::tag_t tag;
    tag.offset = offset;
    tag.key = pmt::intern(key);
    tag.value = pmt::from_uint64(value);
    tag.srcid = pmt::PMT_F;
    return tag;
}
constexpr const char* ABSOLUTE = "rustradio.iq.absolute_sample_index";
void check_alignment(uint64_t origin,
                     bool discontinuities,
                     const char* key0 = ABSOLUTE,
                     const char* key1 = ABSOLUTE)
{
    std::vector<float> first(12);
    std::iota(first.begin(), first.end(), 0.f);
    std::vector<gr_complex> second;
    for (int i = 0; i < 20; ++i)
        second.emplace_back(i, -i);
    std::vector<gr::tag_t> tags0{ timing_tag(0, key0, origin) };
    const auto start1 = discontinuities ? origin : origin + 2;
    std::vector<gr::tag_t> tags1{ timing_tag(0, key1, start1) };
    std::vector<int> expected;
    if (discontinuities) {
        tags0.push_back(timing_tag(3, GAP_SAMPLES, 2));
        tags0.push_back(timing_tag(6, key0, origin + 11));
        expected = { 0, 1, 2, 5, 6, 7, 11, 12, 13, 14, 15, 16 };
    } else {
        // Exercise the last legal UINT64 index without wrapping.
        first.resize(5);
        second.resize(3);
        expected = { 2, 3, 4 };
    }
    auto graph = gr::make_top_block("alignment QA");
    auto source0 = gr::blocks::vector_source_f::make(first, false, 1, tags0);
    auto source1 = gr::blocks::vector_source_c::make(second, false, 1, tags1);
    auto aligned = align_streams::make(sizeof(float), sizeof(gr_complex), key0, key1);
    auto out0 = gr::blocks::vector_sink_f::make();
    auto out1 = gr::blocks::vector_sink_c::make();
    graph->connect(source0, 0, aligned, 0);
    graph->connect(source1, 0, aligned, 1);
    graph->connect(aligned, 0, out0, 0);
    graph->connect(aligned, 1, out1, 0);
    graph->run(3);
    const auto data0 = out0->data();
    const auto data1 = out1->data();
    CHECK(data0.size() == expected.size() && data1.size() == expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        CHECK(data0[i] == (discontinuities ? i : i + 2));
        const int index = expected[i] - (discontinuities ? 0 : 2);
        CHECK(data1[i] == gr_complex(index, -index));
    }
    const std::array<std::vector<gr::tag_t>, 2> port_tags{ out0->tags(), out1->tags() };
    const std::array<const char*, 2> keys{ key0, key1 };
    for (int port = 0; port < 2; ++port) {
        const auto& tags = port_tags[port];
        std::optional<uint64_t> index;
        for (size_t offset = 0; offset < expected.size(); ++offset) {
            for (const auto& tag : tags)
                if (tag.offset == offset && tag.key == pmt::intern(keys[port]))
                    index = pmt::to_uint64(tag.value);
            CHECK(index && *index == origin + expected[offset]);
            if (offset + 1 < expected.size())
                ++*index;
        }
    }
}
} // namespace
int main()
{
    try {
        check_alignment(100, true);
        check_alignment(100, true, "left.position", "right.position");
        check_alignment(UINT64_MAX - 4, false);
        std::cout << "alignment checks passed\n";
    } catch (const std::exception& e) {
        std::cerr << e.what() << '\n';
        return 1;
    }
}
