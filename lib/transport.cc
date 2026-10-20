/* SPDX-License-Identifier: MIT */
#include "client_connection.h"
#include "server_impl.h"
#include <grpcpp/create_channel.h>
#include <grpcpp/generic/generic_stub.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <algorithm>
#include <cmath>
#include <set>

namespace gr {
namespace iqstream {
namespace {
grpc::ByteBuffer buffer(const std::string& bytes)
{
    grpc::Slice slice(bytes);
    return grpc::ByteBuffer(&slice, 1);
}
std::string bytes(const grpc::ByteBuffer& input)
{
    require(input.Length() <= MAX_ENVELOPE_BYTES,
            "gRPC envelope byte limit",
            grpc::StatusCode::RESOURCE_EXHAUSTED);
    std::vector<grpc::Slice> slices;
    require(input.Dump(&slices).ok(), "invalid gRPC ByteBuffer");
    std::string result;
    result.reserve(input.Length());
    for (const auto& slice : slices)
        result.append(reinterpret_cast<const char*>(slice.begin()), slice.size());
    return result;
}
std::shared_ptr<grpc::ChannelCredentials> client_credentials(const tls_options& tls)
{
    if (!tls.enabled)
        return grpc::InsecureChannelCredentials();
    grpc::SslCredentialsOptions options;
    options.pem_root_certs = tls.root_certificates;
    options.pem_cert_chain = tls.certificate_chain;
    options.pem_private_key = tls.private_key;
    require(tls.certificate_chain.empty() == tls.private_key.empty(),
            "both client certificate and key required");
    return grpc::SslCredentials(options);
}
std::string target(const std::string& address)
{
    require(!address.empty(), "empty server address");
    // gRPC takes authority/port targets, not RustRadio's HTTP URL spelling.
    auto value = address;
    for (const auto& prefix : { std::string("http://"), std::string("https://") }) {
        if (value.compare(0, prefix.size(), prefix) == 0) {
            value.erase(0, prefix.size());
            break;
        }
    }
    require(value.find('/') == std::string::npos ||
                value.find("://") != std::string::npos,
            "address must not include an HTTP path");
    return value;
}
} // namespace
class client_connection::reactor
    : public grpc::ClientBidiReactor<grpc::ByteBuffer, grpc::ByteBuffer>
{
public:
    reactor(const std::string& address,
            const std::shared_ptr<session>& state,
            const stream_options& options)
        : d_session(state)
    {
        auto tls = options.tls;
        if (address.compare(0, 8, "https://") == 0)
            tls.enabled = true;
        require(address.compare(0, 7, "http://") != 0 || !tls.enabled,
                "http:// cannot request TLS");
        grpc::ChannelArguments args;
        args.SetMaxReceiveMessageSize(MAX_ENVELOPE_BYTES);
        args.SetMaxSendMessageSize(MAX_ENVELOPE_BYTES);
        args.SetCompressionAlgorithm(GRPC_COMPRESS_NONE);
        d_channel =
            grpc::CreateCustomChannel(target(address), client_credentials(tls), args);
        for (const auto& pair : options.grpc_metadata)
            d_context.AddMetadata(pair.first, pair.second);
        d_context.set_compression_algorithm(GRPC_COMPRESS_NONE);
        if (options.transfer_timeout_ms)
            d_context.set_deadline(
                std::chrono::system_clock::now() +
                std::chrono::milliseconds(options.transfer_timeout_ms));
        grpc::GenericStub stub(d_channel);
        stub.PrepareBidiStreamingCall(&d_context, METHOD, {}, this);
        // Scheduler/watchdog threads may issue operations until the read side closes.
        AddHold();
        d_session->bind(
            [this](const std::string& data) {
                d_write_buffer = buffer(data);
                StartWrite(&d_write_buffer);
            },
            [this] { StartWritesDone(); },
            [](grpc::Status) {},
            [this] { d_context.TryCancel(); });
        StartRead(&d_read_buffer);
        try {
            d_session->begin_client();
        } catch (const std::exception& e) {
            d_session->local_failure(grpc::StatusCode::INVALID_ARGUMENT, e.what());
        }
        StartCall();
    }
    void OnReadDone(bool ok) override
    {
        if (!ok) {
            d_session->read_closed();
            RemoveHold();
            return;
        }
        try {
            d_session->received(bytes(d_read_buffer));
        } catch (const protocol_error& e) {
            d_session->local_failure(e.code(), e.what());
        }
        d_read_buffer.Clear();
        StartRead(&d_read_buffer);
    }
    void OnWriteDone(bool ok) override
    {
        d_write_buffer.Clear();
        d_session->write_done(ok);
    }
    void OnDone(const grpc::Status& result) override
    {
        d_session->transport_done(result);
    }

private:
    std::shared_ptr<session> d_session;
    std::shared_ptr<grpc::Channel> d_channel;
    grpc::ClientContext d_context;
    grpc::ByteBuffer d_read_buffer;
    grpc::ByteBuffer d_write_buffer;
};
client_connection::client_connection(const std::string& address,
                                     const std::shared_ptr<session>& state,
                                     const stream_options& options)
    : d_session(state), d_reactor(std::make_unique<reactor>(address, state, options))
{
    d_watchdog = std::thread([state] {
        while (!state->done()) {
            state->tick();
            state->wait_done(std::chrono::milliseconds(10));
        }
    });
}
client_connection::~client_connection()
{
    if (!d_session->done())
        d_session->cancel();
    // The gRPC reactor must stay alive until OnDone; cancellation releases pending I/O.
    if (d_watchdog.joinable())
        d_watchdog.join();
}
namespace {
class server_reactor : public grpc::ServerGenericBidiReactor
{
public:
    server_reactor(grpc::GenericCallbackServerContext* context,
                   std::shared_ptr<session> state,
                   session::resolver resolve,
                   std::function<void()> release)
        : d_release(std::move(release)), d_context(context), d_session(std::move(state))
    {
        d_session->bind(
            [this](const std::string& data) {
                d_write_buffer = buffer(data);
                d_write_busy = true;
                StartWrite(&d_write_buffer);
            },
            [] {},
            [this](grpc::Status result) { finish(result); },
            [this] {
                d_abort_requested = true;
                d_context->TryCancel();
                if (!d_write_busy)
                    finish(grpc::Status(grpc::StatusCode::CANCELLED, "session aborted"));
            },
            std::move(resolve));
        StartRead(&d_read_buffer);
    }
    void OnReadDone(bool ok) override
    {
        if (!ok) {
            d_session->read_closed();
            return;
        }
        try {
            d_session->received(bytes(d_read_buffer));
        } catch (const protocol_error& e) {
            d_session->local_failure(e.code(), e.what());
        }
        d_read_buffer.Clear();
        StartRead(&d_read_buffer);
    }
    void OnWriteDone(bool ok) override
    {
        d_write_busy = false;
        d_write_buffer.Clear();
        d_session->write_done(ok);
        if (d_abort_requested)
            finish(grpc::Status(grpc::StatusCode::CANCELLED, "session aborted"));
    }
    void OnCancel() override { d_session->cancel(); }
    void OnDone() override
    {
        d_session->transport_done(d_result);
        d_release();
        delete this;
    }

private:
    void finish(const grpc::Status& result)
    {
        bool expected = false;
        if (d_finished.compare_exchange_strong(expected, true)) {
            d_result = result;
            Finish(result);
        }
    }
    std::function<void()> d_release;
    grpc::GenericCallbackServerContext* d_context;
    std::shared_ptr<session> d_session;
    grpc::ByteBuffer d_read_buffer;
    grpc::ByteBuffer d_write_buffer;
    std::atomic<bool> d_write_busy{ false };
    std::atomic<bool> d_abort_requested{ false };
    std::atomic<bool> d_finished{ false };
    grpc::Status d_result{ grpc::StatusCode::CANCELLED, "session cancelled" };
};
class catalogue_reactor : public grpc::ServerGenericBidiReactor
{
public:
    catalogue_reactor(std::function<std::vector<stream_listing>()> list,
                      std::function<void()> release)
        : d_list(std::move(list)), d_release(std::move(release))
    {
        StartRead(&d_input);
    }
    void OnReadDone(bool ok) override
    {
        if (!ok) {
            if (!d_received)
                Finish(grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                                    "missing catalogue request"));
            else
                respond();
            return;
        }
        if (d_received) {
            Finish(grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                                "multiple catalogue requests"));
            return;
        }
        try {
            const auto input = bytes(d_input);
            require(input.size() <= MAX_CONTROL_BYTES,
                    "catalogue request byte limit",
                    grpc::StatusCode::RESOURCE_EXHAUSTED);
            wire::ListStreamsRequest request;
            require(request.ParseFromString(input), "invalid catalogue request");
            d_received = true;
            d_input.Clear();
            StartRead(&d_input);
        } catch (const protocol_error& e) {
            Finish(grpc::Status(e.code(), e.what()));
        }
    }
    void OnDone() override
    {
        d_release();
        delete this;
    }

private:
    void respond()
    {
        try {
            auto entries = d_list();
            require(entries.size() <= 4096,
                    "catalogue entry limit",
                    grpc::StatusCode::RESOURCE_EXHAUSTED);
            wire::ListStreamsResponse response;
            for (const auto& entry : entries) {
                auto* info = response.add_streams();
                info->set_name(entry.name);
                *info->mutable_encoding() = encoding(entry.layout);
                if (entry.sample_rate_hz)
                    info->set_sample_rate_hz(*entry.sample_rate_hz);
                info->set_ended(entry.ended);
                require(response.ByteSizeLong() <= MAX_ENVELOPE_BYTES,
                        "catalogue byte limit",
                        grpc::StatusCode::RESOURCE_EXHAUSTED);
            }
            d_output = buffer(response.SerializeAsString());
            StartWriteAndFinish(&d_output, grpc::WriteOptions{}, grpc::Status::OK);
        } catch (const protocol_error& e) {
            Finish(grpc::Status(e.code(), e.what()));
        } catch (const std::exception& e) {
            Finish(grpc::Status(grpc::StatusCode::INTERNAL, e.what()));
        }
    }
    std::function<std::vector<stream_listing>()> d_list;
    std::function<void()> d_release;
    grpc::ByteBuffer d_input, d_output;
    bool d_received = false;
};
class rejected_reactor : public grpc::ServerGenericBidiReactor
{
public:
    explicit rejected_reactor(grpc::StatusCode code)
    {
        Finish(grpc::Status(code, "RPC unavailable"));
    }
    void OnDone() override { delete this; }
};
} // namespace
server_impl::server_impl(const std::string& address, const tls_options& tls)
{
    grpc::ServerBuilder builder;
    d_acceptor = builder.experimental().AddExternalConnectionAcceptor(
        grpc::ServerBuilder::experimental_type::ExternalConnectionType::FROM_FD,
        grpc::InsecureServerCredentials());
    builder.RegisterCallbackGenericService(this);
    builder.SetMaxReceiveMessageSize(MAX_ENVELOPE_BYTES);
    builder.SetMaxSendMessageSize(MAX_ENVELOPE_BYTES);
    builder.SetDefaultCompressionAlgorithm(GRPC_COMPRESS_NONE);
    d_server = builder.BuildAndStart();
    require(static_cast<bool>(d_server),
            "cannot start gRPC server",
            grpc::StatusCode::UNAVAILABLE);
    d_listener = std::make_unique<http_listener>(
        target(address),
        tls,
        [this] { return streams(); },
        [this](int fd, const std::string& preface) {
            grpc::experimental::ExternalConnectionAcceptor::NewConnectionParameters
                params;
            params.fd = fd;
            params.read_buffer = buffer(preface);
            d_acceptor->HandleNewConnection(&params);
        },
        [this] {
            std::lock_guard<std::mutex> guard(d_mutex);
            require(!d_stopping && d_active_sessions < 64,
                    "server stopped/session limit",
                    grpc::StatusCode::RESOURCE_EXHAUSTED);
            auto state = std::make_shared<session>(
                false, false, sample_layout::REAL, stream_options{});
            ++d_active_sessions;
            d_sessions.push_back(state);
            return http_listener::websocket_session{ state,
                                                     [this](const auto& open,
                                                            const auto& connection) {
                                                         return resolve(open, connection);
                                                     },
                                                     [this] { --d_active_sessions; } };
        });
    d_address = d_listener->address();
    d_watchdog = std::thread([this] {
        while (!d_stopping) {
            std::vector<std::shared_ptr<session>> states;
            {
                std::lock_guard<std::mutex> guard(d_mutex);
                d_sessions.erase(std::remove_if(d_sessions.begin(),
                                                d_sessions.end(),
                                                [&](const auto& weak) {
                                                    if (auto s = weak.lock()) {
                                                        states.push_back(std::move(s));
                                                        return false;
                                                    }
                                                    return true;
                                                }),
                                 d_sessions.end());
            }
            for (auto& state : states)
                state->tick();
            poll_observations();
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    });
}
server_impl::~server_impl() { shutdown(); }
std::shared_ptr<resource> server_impl::add_resource(const std::string& id,
                                                    bool sending,
                                                    sample_layout layout,
                                                    const stream_options& options)
{
    require(!id.empty() && valid_utf8(id), "invalid resource identifier");
    validate_options(options, sending);
    // Validate fixed resource format/rate and metadata before admitting sessions.
    (void)description(layout, options);
    auto entry = std::make_shared<resource>();
    entry->sending = sending;
    entry->layout = layout;
    entry->options = options;
    std::lock_guard<std::mutex> guard(d_mutex);
    require(!d_stopping && d_resources.emplace(id, entry).second,
            "duplicate resource/server stopped");
    return entry;
}
void server_impl::remove_resource(const std::string& id,
                                  const std::shared_ptr<resource>& entry)
{
    std::lock_guard<std::mutex> guard(d_mutex);
    auto found = d_resources.find(id);
    if (found != d_resources.end() && found->second == entry)
        d_resources.erase(found);
}
void server_impl::reset_resource(const std::shared_ptr<resource>& entry)
{
    std::lock_guard<std::mutex> guard(entry->mutex);
    require(!d_stopping, "server is shut down", grpc::StatusCode::FAILED_PRECONDITION);
    require(!entry->connection || entry->connection->done(),
            "previous resource session still active",
            grpc::StatusCode::FAILED_PRECONDITION);
    entry->connection.reset();
    entry->claimed = false;
    entry->stopped = false;
    entry->graph_position = 0;
    entry->cv.notify_all();
}
resource_binding server_impl::resolve(const wire::Open& open,
                                      const std::shared_ptr<session>& state)
{
    const auto id =
        open.has_upload() ? open.upload().destination() : open.download().source();
    std::shared_ptr<resource> entry;
    {
        std::lock_guard<std::mutex> guard(d_mutex);
        auto found = d_resources.find(id);
        if (found != d_resources.end())
            entry = found->second;
    }
    if (!entry)
        return resolve_observed(open, state);
    std::lock_guard<std::mutex> guard(entry->mutex);
    require(!entry->stopped, "resource is closed", grpc::StatusCode::FAILED_PRECONDITION);
    require(!entry->claimed,
            "resource already claimed",
            entry->connection && !entry->connection->done()
                ? grpc::StatusCode::ALREADY_EXISTS
                : grpc::StatusCode::FAILED_PRECONDITION);
    require(entry->sending == open.has_download(),
            "resource direction mismatch",
            grpc::StatusCode::FAILED_PRECONDITION);
    entry->claimed = true;
    entry->connection = state;
    entry->cv.notify_all();
    return { entry->sending, entry->layout, entry->options, entry->graph_position };
}
grpc::ServerGenericBidiReactor*
server_impl::CreateReactor(grpc::GenericCallbackServerContext* context)
{
    if (context->method() == "/rustradio.iq.v1.IqStreaming/ListStreams") {
        std::lock_guard<std::mutex> guard(d_mutex);
        if (d_stopping || d_active_sessions >= 64)
            return new rejected_reactor(grpc::StatusCode::RESOURCE_EXHAUSTED);
        ++d_active_sessions;
        return new catalogue_reactor([this] { return streams(); },
                                     [this] { --d_active_sessions; });
    }
    if (context->method() != METHOD)
        return new rejected_reactor(grpc::StatusCode::UNIMPLEMENTED);
    auto state =
        std::make_shared<session>(false, false, sample_layout::REAL, stream_options{});
    {
        std::lock_guard<std::mutex> guard(d_mutex);
        if (d_stopping || d_active_sessions >= 64)
            return new rejected_reactor(grpc::StatusCode::RESOURCE_EXHAUSTED);
        ++d_active_sessions;
        auto* reactor = new server_reactor(
            context,
            state,
            [this](const auto& open, const auto& connection) {
                return resolve(open, connection);
            },
            [this] { --d_active_sessions; });
        d_sessions.push_back(state);
        return reactor;
    }
}
std::string server_impl::address() const { return d_address; }
void server_impl::shutdown()
{
    std::lock_guard<std::mutex> shutdown_guard(d_shutdown_mutex);
    if (d_stopping.exchange(true))
        return;
    d_listener->shutdown();
    if (d_watchdog.joinable())
        d_watchdog.join();
    std::vector<std::shared_ptr<session>> sessions;
    {
        std::lock_guard<std::mutex> guard(d_mutex);
        d_observations.clear();
        for (auto& weak : d_sessions)
            if (auto s = weak.lock())
                sessions.push_back(s);
        for (auto& pair : d_resources) {
            std::lock_guard<std::mutex> resource_guard(pair.second->mutex);
            pair.second->stopped = true;
            pair.second->cv.notify_all();
        }
    }
    for (auto& state : sessions)
        state->cancel();
    d_server->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(5));
    d_server->Wait();
}
void server_impl::register_all_streams(metadata_profile profile,
                                       double default_sample_rate_hz)
{
    require(!d_stopping, "server is shut down", grpc::StatusCode::FAILED_PRECONDITION);
    require(profile == metadata_profile::NATIVE || profile == metadata_profile::RUSTRADIO,
            "invalid observation metadata profile");
    require(std::isfinite(default_sample_rate_hz) && default_sample_rate_hz >= 0,
            "default sample rate must be finite and nonnegative");
    d_observe_default_sample_rate_hz = default_sample_rate_hz;
    d_observe_profile = profile;
    d_observe = true;
}
std::vector<stream_listing> server_impl::streams() const
{
    const auto process = d_observe ? gr::streams() : std::vector<gr::stream_info>{};
    const auto default_rate = d_observe_default_sample_rate_hz.load();
    std::vector<std::pair<std::string, std::shared_ptr<resource>>> manual;
    {
        std::lock_guard<std::mutex> guard(d_mutex);
        if (d_stopping)
            return {};
        manual.assign(d_resources.begin(), d_resources.end());
    }
    std::set<std::string> used;
    for (const auto& entry : manual)
        used.insert(entry.first);
    for (const auto& info : process)
        used.insert(info.name);
    std::vector<stream_listing> result;
    for (const auto& info : process) {
        if (!info.observable || info.item_type.vlen != 1 ||
            (info.item_type.type != gr::stream_item_type::float32 &&
             info.item_type.type != gr::stream_item_type::complex64))
            continue;
        std::string name = info.name;
        if (std::any_of(manual.begin(),
                        manual.end(),
                        [&](const auto& e) { return e.first == name; }) ||
            std::any_of(result.begin(), result.end(), [&](const auto& e) {
                return e.name == name;
            })) {
            auto base = name + "-gr-" + std::to_string(info.id);
            name = base;
            unsigned suffix = 0;
            while (!used.insert(name).second)
                name = base + "-" + std::to_string(++suffix);
        }
        result.push_back({ name,
                           info.item_type.type == gr::stream_item_type::float32
                               ? sample_layout::REAL
                               : sample_layout::COMPLEX,
                           info.sample_rate_hz
                               ? info.sample_rate_hz
                               : (default_rate > 0 ? std::optional<double>(default_rate)
                                                   : std::nullopt),
                           info.ended,
                           info });
    }
    for (const auto& pair : manual) {
        const auto& entry = pair.second;
        std::lock_guard<std::mutex> guard(entry->mutex);
        if (!entry->sending)
            continue;
        result.push_back({ pair.first,
                           entry->layout,
                           entry->options.sample_rate_hz > 0
                               ? std::optional<double>(entry->options.sample_rate_hz)
                               : std::nullopt,
                           entry->stopped,
                           {} });
    }
    std::sort(result.begin(), result.end(), [](const auto& a, const auto& b) {
        return a.name < b.name;
    });
    return result;
}
resource_binding server_impl::resolve_observed(const wire::Open& open,
                                               const std::shared_ptr<session>& state)
{
    require(d_observe && open.has_download(),
            "unknown resource",
            grpc::StatusCode::NOT_FOUND);
    auto entries = streams();
    auto it = std::find_if(entries.begin(), entries.end(), [&](const auto& entry) {
        return entry.origin && entry.name == open.download().source();
    });
    require(it != entries.end(), "unknown resource", grpc::StatusCode::NOT_FOUND);
    require(open.loss_policy() == wire::LOSS_POLICY_ALLOW_GAPS,
            "automatic streams require ALLOW_GAPS",
            grpc::StatusCode::FAILED_PRECONDITION);
    const auto& info = *it->origin;
    std::lock_guard<std::mutex> guard(d_mutex);
    require(!d_stopping, "server is shut down", grpc::StatusCode::UNAVAILABLE);
    auto old = d_observations.find(info.id);
    require(old == d_observations.end() || old->second->connection->done(),
            "stream already observed",
            grpc::StatusCode::ALREADY_EXISTS);
    gr::stream_subscription::sptr subscription;
    try {
        subscription = gr::observe_stream(info.id);
    } catch (const std::out_of_range&) {
        throw protocol_error(grpc::StatusCode::NOT_FOUND, "stream expired");
    }
    auto observation = std::make_shared<server_impl::observation>();
    observation->subscription = subscription;
    observation->connection = state;
    observation->origin = subscription->start_offset();
    d_observations[info.id] = observation;
    stream_options options;
    options.profile = d_observe_profile.load();
    options.loss = loss_policy::ALLOW_GAPS;
    options.blocking = false;
    options.sample_rate_hz = it->sample_rate_hz.value_or(0.0);
    options.source_id = it->name;
    options.properties = { { "gnuradio.block_id", pmt::from_long(info.block_id) },
                           { "gnuradio.block_name", pmt::intern(info.block_name) },
                           { "gnuradio.block_alias", pmt::intern(info.block_alias) },
                           { "gnuradio.output_port", pmt::from_uint64(info.output_port) },
                           { "gnuradio.stream_id", pmt::from_uint64(info.id) },
                           { "gnuradio.item_type", pmt::intern(info.item_type.name) },
                           { "gnuradio.item_size",
                             pmt::from_uint64(info.item_type.item_size) },
                           { "gnuradio.vlen", pmt::from_uint64(info.item_type.vlen) } };
    return { true, it->layout, options, observation->origin };
}
void server_impl::poll_observations()
{
    std::vector<std::pair<uint64_t, std::shared_ptr<observation>>> observations;
    {
        std::lock_guard<std::mutex> guard(d_mutex);
        observations.assign(d_observations.begin(), d_observations.end());
    }
    for (const auto& pair : observations) {
        auto& observer = *pair.second;
        if (!observer.connection->done() && observer.subscription) {
            try {
                for (unsigned i = 0; i < 9; ++i) {
                    auto event = observer.subscription->poll();
                    if (!event)
                        break;
                    if (event->end == gr::stream_end_reason::error)
                        observer.connection->local_failure(grpc::StatusCode::INTERNAL,
                                                           "stream producer failed");
                    else
                        observer.connection->offer_observation(*event, observer.origin);
                    if (event->end != gr::stream_end_reason::none) {
                        observer.subscription.reset();
                        break;
                    }
                }
            } catch (const protocol_error& e) {
                observer.connection->local_failure(e.code(), e.what());
            } catch (const std::exception& e) {
                observer.connection->local_failure(grpc::StatusCode::INTERNAL, e.what());
            }
        }
        if (observer.connection->done()) {
            observer.subscription.reset();
            std::lock_guard<std::mutex> guard(d_mutex);
            auto it = d_observations.find(pair.first);
            if (it != d_observations.end() && it->second == pair.second)
                d_observations.erase(it);
        }
    }
}
server::server(std::shared_ptr<server_impl> impl) : d_impl(std::move(impl)) {}
server::sptr server::make(const std::string& address, const tls_options& tls)
{
    return sptr(new server(std::make_shared<server_impl>(address, tls)));
}
server::~server() { shutdown(); }
std::string server::address() const { return d_impl->address(); }
void server::shutdown() { d_impl->shutdown(); }
void server::register_all_streams(metadata_profile profile)
{
    register_all_streams(profile, 0.0);
}
void server::register_all_streams(metadata_profile profile, double default_sample_rate_hz)
{
    d_impl->register_all_streams(profile, default_sample_rate_hz);
}
std::vector<stream_listing> server::streams() const { return d_impl->streams(); }
} // namespace iqstream
} // namespace gr
