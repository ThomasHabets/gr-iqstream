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
std::shared_ptr<grpc::ServerCredentials> server_credentials(const tls_options& tls)
{
    if (!tls.enabled)
        return grpc::InsecureServerCredentials();
    require(!tls.certificate_chain.empty() && !tls.private_key.empty(),
            "TLS server certificate/key required");
    grpc::SslServerCredentialsOptions options;
    options.pem_root_certs = tls.root_certificates;
    options.pem_key_cert_pairs.push_back({ tls.private_key, tls.certificate_chain });
    options.client_certificate_request =
        tls.require_client_certificate
            ? GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY
            : GRPC_SSL_DONT_REQUEST_CLIENT_CERTIFICATE;
    return grpc::SslServerCredentials(options);
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
    int port = 0;
    auto bind_address = target(address);
    builder.AddListeningPort(bind_address, server_credentials(tls), &port);
    builder.RegisterCallbackGenericService(this);
    builder.SetMaxReceiveMessageSize(MAX_ENVELOPE_BYTES);
    builder.SetMaxSendMessageSize(MAX_ENVELOPE_BYTES);
    builder.SetDefaultCompressionAlgorithm(GRPC_COMPRESS_NONE);
    d_server = builder.BuildAndStart();
    require(d_server && port, "cannot bind gRPC listener", grpc::StatusCode::UNAVAILABLE);
    const auto colon = bind_address.rfind(':');
    d_address = colon == std::string::npos
                    ? bind_address
                    : bind_address.substr(0, colon + 1) + std::to_string(port);
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
        require(
            found != d_resources.end(), "unknown resource", grpc::StatusCode::NOT_FOUND);
        entry = found->second;
    }
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
    if (d_watchdog.joinable())
        d_watchdog.join();
    std::vector<std::shared_ptr<session>> sessions;
    {
        std::lock_guard<std::mutex> guard(d_mutex);
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
server::server(std::shared_ptr<server_impl> impl) : d_impl(std::move(impl)) {}
server::sptr server::make(const std::string& address, const tls_options& tls)
{
    return sptr(new server(std::make_shared<server_impl>(address, tls)));
}
server::~server() { shutdown(); }
std::string server::address() const { return d_impl->address(); }
void server::shutdown() { d_impl->shutdown(); }
} // namespace iqstream
} // namespace gr
