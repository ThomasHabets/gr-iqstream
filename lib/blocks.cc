/* SPDX-License-Identifier: GPL-3.0-or-later */
#include "client_connection.h"
#include "server_impl.h"
#include <gnuradio/gr_complex.h>
#include <gnuradio/io_signature.h>
#include <gnuradio/iqstream/server_sink.h>
#include <gnuradio/iqstream/server_source.h>
#include <gnuradio/iqstream/sink.h>
#include <gnuradio/iqstream/source.h>
#include <gnuradio/sptr_magic.h>
#include <algorithm>

namespace gr {
namespace iqstream {
namespace {
size_t stride(sample_layout layout)
{
    (void)encoding(layout);
    return layout == sample_layout::REAL ? sizeof(float) : sizeof(gr_complex);
}
template <class Interface, bool Sending, bool Serving>
class stream_impl : public Interface
{
public:
    stream_impl(const std::string& address,
                const std::string& id,
                sample_layout layout,
                stream_options options,
                server::sptr listener = {})
        : gr::sync_block(
              Sending ? "iqstream_sink" : "iqstream_source",
              gr::io_signature::make(
                  Sending ? 1 : 0, Sending ? 1 : 0, Sending ? stride(layout) : 0),
              gr::io_signature::make(
                  Sending ? 0 : 1, Sending ? 0 : 1, Sending ? 0 : stride(layout))),
          d_address(address),
          d_id(id),
          d_layout(layout),
          d_options(std::move(options)),
          d_listener(std::move(listener))
    {
        validate_options(d_options, Sending || Serving);
        require(!id.empty() && valid_utf8(id), "invalid resource identifier");
        if constexpr (Sending)
            (void)description(layout, d_options);
        if constexpr (Serving) {
            require(static_cast<bool>(d_listener), "null server");
            d_entry = server_impl::get(d_listener)
                          ->add_resource(id, Sending, layout, d_options);
        }
        this->set_tag_propagation_policy(gr::block::TPP_DONT);
    }
    ~stream_impl() override
    {
        stop();
        if constexpr (Serving)
            server_impl::get(d_listener)->remove_resource(d_id, d_entry);
    }
    bool start() override
    {
        std::lock_guard<std::mutex> guard(d_mutex);
        if (d_running)
            return true;
        if constexpr (Serving) {
            if (d_has_run)
                server_impl::get(d_listener)->reset_resource(d_entry);
        } else {
            d_connection.reset();
            d_session =
                std::make_shared<session>(true, Sending, d_layout, d_options, d_id);
            d_connection =
                std::make_unique<client_connection>(d_address, d_session, d_options);
        }
        d_running = true;
        d_has_run = true;
        return true;
    }
    bool stop() override
    {
        std::shared_ptr<session> state;
        {
            std::lock_guard<std::mutex> guard(d_mutex);
            if (!d_running)
                return true;
            d_running = false;
        }
        if constexpr (Serving) {
            std::unique_lock<std::mutex> guard(d_entry->mutex);
            if constexpr (Sending) {
                d_entry->cv.wait_for(
                    guard, std::chrono::milliseconds(d_options.shutdown_timeout_ms), [&] {
                        return d_entry->connection || d_entry->stopped;
                    });
            }
            d_entry->stopped = true;
            state = d_entry->connection;
            d_entry->cv.notify_all();
        } else
            state = d_session;
        if (!state)
            return true;
        if constexpr (Sending)
            state->finish_sending();
        else if (!state->done())
            state->cancel();
        if (!state->wait_done(
                std::chrono::milliseconds(d_options.shutdown_timeout_ms + 100))) {
            state->cancel();
            state->wait_done(std::chrono::seconds(5));
        }
        return state->status().state == session_state::COMPLETE;
    }
    int work(int noutput_items,
             gr_vector_const_void_star& input_items,
             gr_vector_void_star& output_items) override
    {
        auto state = connection();
        if constexpr (Serving && Sending) {
            if (!state && !d_options.blocking) {
                std::lock_guard<std::mutex> guard(d_entry->mutex);
                // Recheck under the registration lock: acquisition after the
                // session origin must contribute to that session's loss policy.
                state = d_entry->connection;
                if (!state) {
                    d_entry->graph_position =
                        advance(d_entry->graph_position, noutput_items);
                    return noutput_items;
                }
            }
        }
        if (!state)
            return gr::block::WORK_DONE;
        try {
            if constexpr (Sending) {
                std::vector<gr::tag_t> tags;
                this->get_tags_in_range(tags,
                                        0,
                                        this->nitems_read(0),
                                        advance(this->nitems_read(0), noutput_items));
                auto n = state->push(
                    input_items[0], noutput_items, tags, this->nitems_read(0));
                if constexpr (Serving) {
                    std::lock_guard<std::mutex> guard(d_entry->mutex);
                    d_entry->graph_position = advance(d_entry->graph_position, n);
                }
                return n ? static_cast<int>(n) : gr::block::WORK_DONE;
            } else {
                std::vector<gr::tag_t> tags;
                auto n = state->pull(
                    output_items[0], noutput_items, this->nitems_written(0), tags);
                for (const auto& tag : tags)
                    this->add_item_tag(0, tag);
                return n;
            }
        } catch (const protocol_error& e) {
            state->local_failure(e.code(), e.what());
        } catch (const std::exception& e) {
            state->local_failure(grpc::StatusCode::INTERNAL, e.what());
        }
        return gr::block::WORK_DONE;
    }
    session_status status() const override
    {
        std::shared_ptr<session> state;
        if constexpr (Serving) {
            std::lock_guard<std::mutex> guard(d_entry->mutex);
            state = d_entry->connection;
        } else {
            std::lock_guard<std::mutex> guard(d_mutex);
            state = d_session;
        }
        return state ? state->status() : session_status{};
    }
    void cancel() override
    {
        std::shared_ptr<session> state;
        if constexpr (Serving) {
            std::lock_guard<std::mutex> guard(d_entry->mutex);
            d_entry->stopped = true;
            state = d_entry->connection;
            d_entry->cv.notify_all();
        } else {
            std::lock_guard<std::mutex> guard(d_mutex);
            state = d_session;
        }
        if (state)
            state->cancel();
    }

private:
    std::shared_ptr<session> connection()
    {
        if constexpr (Serving) {
            std::unique_lock<std::mutex> lock(d_entry->mutex);
            while (!d_entry->connection && !d_entry->stopped &&
                   (d_options.blocking || !Sending)) {
                d_entry->cv.wait_for(lock, std::chrono::milliseconds(10));
                boost::this_thread::interruption_point();
            }
            return d_entry->connection;
        } else
            return d_session;
    }
    mutable std::mutex d_mutex;
    std::string d_address;
    std::string d_id;
    sample_layout d_layout;
    stream_options d_options;
    server::sptr d_listener;
    std::shared_ptr<resource> d_entry;
    std::shared_ptr<session> d_session;
    std::unique_ptr<client_connection> d_connection;
    bool d_running = false;
    bool d_has_run = false;
};
} // namespace
source::sptr source::make(const std::string& address,
                          const std::string& resource,
                          sample_layout layout,
                          const stream_options& options)
{
    return gnuradio::make_block_sptr<stream_impl<source, false, false>>(
        address, resource, layout, options);
}
sink::sptr sink::make(const std::string& address,
                      const std::string& resource,
                      sample_layout layout,
                      const stream_options& options)
{
    return gnuradio::make_block_sptr<stream_impl<sink, true, false>>(
        address, resource, layout, options);
}
server_source::sptr server_source::make(server::sptr listener,
                                        const std::string& resource,
                                        sample_layout layout,
                                        const stream_options& options)
{
    return gnuradio::make_block_sptr<stream_impl<server_source, false, true>>(
        "", resource, layout, options, std::move(listener));
}
server_sink::sptr server_sink::make(server::sptr listener,
                                    const std::string& resource,
                                    sample_layout layout,
                                    const stream_options& options)
{
    return gnuradio::make_block_sptr<stream_impl<server_sink, true, true>>(
        "", resource, layout, options, std::move(listener));
}
} // namespace iqstream
} // namespace gr
