/* SPDX-License-Identifier: GPL-3.0-or-later */
#ifndef INCLUDED_GR_IQSTREAM_SESSION_H
#define INCLUDED_GR_IQSTREAM_SESSION_H
#include "protocol.h"
#include <condition_variable>
#include <chrono>
#include <deque>
#include <functional>
#include <mutex>

namespace gr {
namespace iqstream {
class session;
struct resource_binding {
    bool sending;
    sample_layout layout;
    stream_options options;
    uint64_t graph_origin = 0;
};

/*! Transport-independent state machine. Transport hooks run under d_mutex;
 * reactions must not reenter synchronously. One write is outstanding at a time.
 */
class session : public std::enable_shared_from_this<session>
{
public:
    session(bool client,
            bool sending,
            sample_layout layout,
            stream_options options,
            std::string resource = {});
    using resolver = std::function<resource_binding(const wire::Open&,
                                                    const std::shared_ptr<session>&)>;
    void bind(std::function<void(const std::string&)> write,
              std::function<void()> half_close,
              std::function<void(grpc::Status)> finish,
              std::function<void()> abort,
              resolver resolve = {});
    void begin_client();
    void received(const std::string& bytes);
    void write_done(bool ok);
    void read_closed();
    void transport_done(const grpc::Status& status);
    void tick();
    size_t push(const void* data,
                size_t count,
                const std::vector<gr::tag_t>& tags,
                uint64_t local_offset);
    int
    pull(void* output, int capacity, uint64_t local_offset, std::vector<gr::tag_t>& tags);
    void finish_sending();
    void cancel();
    void local_failure(grpc::StatusCode code, const std::string& message);
    bool wait_done(std::chrono::milliseconds timeout);
    session_status status() const;
    bool sending() const;
    bool done() const;

private:
    enum class action { NONE, OPEN, STARTED, CREDIT, FRAME, END, COMPLETE, FAILURE };
    struct incoming {
        wire::Frame frame;
        std::vector<gr::tag_t> tags;
        size_t position = 0;
        uint64_t gap_before = 0;
    };
    mutable std::mutex d_mutex;
    std::condition_variable d_cv;
    const bool d_client;
    bool d_sending;
    sample_layout d_layout;
    stream_options d_options;
    std::string d_resource;
    wire::StreamDescription d_description;
    wire::Limits d_limits;
    wire::LossPolicy d_policy;
    session_status d_status;
    bool d_started = false;
    bool d_busy = false;
    bool d_read_closed = false;
    bool d_done = false;
    bool d_failed = false;
    bool d_remote_failure = false;
    bool d_finish_requested = false;
    bool d_end_sent = false;
    bool d_end_received = false;
    bool d_complete_queued = false;
    bool d_complete_sent = false;
    bool d_peer_complete = false;
    bool d_finishing_transport = false;
    bool d_half_closed = false;
    action d_write_action = action::NONE;
    uint64_t d_sequence = 0;
    uint64_t d_cursor = 0;
    uint64_t d_acquisition_cursor = 0;
    uint64_t d_send_limit = 0;
    uint64_t d_grant = 0;
    uint64_t d_retired = 0;
    uint64_t d_pending_gap = 0;
    uint64_t d_pending_gap_first = 0;
    uint64_t d_receive_gap = 0;
    std::optional<uint64_t> d_credit;
    std::deque<wire::Frame> d_outgoing;
    std::deque<incoming> d_incoming;
    std::deque<std::pair<std::string, action>> d_controls;
    std::function<void(const std::string&)> d_write;
    std::function<void()> d_half_close;
    std::function<void(grpc::Status)> d_finish;
    std::function<void()> d_abort;
    resolver d_resolve;
    const std::chrono::steady_clock::time_point d_open_time;
    std::chrono::steady_clock::time_point d_finish_time;
    std::chrono::steady_clock::time_point d_credit_time;
    void drive_locked();
    void send_locked(const std::string& bytes, action kind);
    void
    fail_locked(grpc::StatusCode code, const std::string& message, bool notify = true);
    void remote_failure_locked(const wire::Failure& failure);
    void open_locked(const wire::Open& open);
    void started_locked(const wire::Started& started);
    void frame_locked(wire::Frame frame, size_t encoded_bytes);
    void credit_locked(const wire::FlowControl& credit);
    void end_locked(const wire::End& end);
    void complete_locked(const wire::Complete& complete);
    void retire_locked();
    void receiver_completion_locked();
    void queue_end_locked();
    wire::Complete completion_locked() const;
    void update_description_locked();
    std::string client_control(const wire::ClientMessage& message) const;
    std::string server_control(const wire::ServerMessage& message) const;
};
} // namespace iqstream
} // namespace gr

#endif /* INCLUDED_GR_IQSTREAM_SESSION_H */
