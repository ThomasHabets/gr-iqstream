/* SPDX-License-Identifier: GPL-3.0-or-later */
#ifndef INCLUDED_GR_IQSTREAM_RESOURCE_H
#define INCLUDED_GR_IQSTREAM_RESOURCE_H
#include "session.h"
namespace gr {
namespace iqstream {
struct resource {
    mutable std::mutex mutex;
    std::condition_variable cv;
    bool sending;
    sample_layout layout;
    stream_options options;
    uint64_t graph_position = 0;
    bool claimed = false;
    bool stopped = false;
    std::shared_ptr<session> connection;
};
} // namespace iqstream
} // namespace gr

#endif /* INCLUDED_GR_IQSTREAM_RESOURCE_H */
