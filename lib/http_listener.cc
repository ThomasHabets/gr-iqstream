/* SPDX-License-Identifier: MIT */
#include "http_listener.h"
#include "protocol.h"
#include <google/protobuf/util/json_util.h>
#include <openssl/ssl.h>
#include <sys/socket.h>
#include <type_traits>
#include <unistd.h>
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast/core/buffers_to_string.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <array>
#include <set>
#include <thread>

namespace gr {
namespace iqstream {
namespace asio = boost::asio;
namespace http = boost::beast::http;
namespace websocket = boost::beast::websocket;
using tcp = asio::ip::tcp;
using error_code = boost::system::error_code;
namespace {
constexpr size_t HEADER_LIMIT = 16 * 1024;
constexpr size_t CONNECTION_LIMIT = 64;
std::string catalogue_json(const std::vector<stream_listing>& entries)
{
    require(entries.size() <= 4096, "catalogue entry limit");
    google::protobuf::util::JsonPrintOptions options;
    options.always_print_primitive_fields = true;
    options.always_print_enums_as_ints = true;
    options.preserve_proto_field_names = true;
    std::string result = "[";
    for (const auto& entry : entries) {
        wire::StreamInfo info;
        info.set_name(entry.name);
        *info.mutable_encoding() = encoding(entry.layout);
        info.set_ended(entry.ended);
        if (entry.sample_rate_hz)
            info.set_sample_rate_hz(*entry.sample_rate_hz);
        std::string json;
        require(google::protobuf::util::MessageToJsonString(info, &json, options).ok(),
                "cannot serialize catalogue");
        // RustRadio's serde representation includes null for an unknown rate.
        if (!entry.sample_rate_hz)
            json.insert(1, "\"sample_rate_hz\":null,");
        if (result.size() != 1)
            result += ',';
        result += json;
        require(result.size() < MAX_ENVELOPE_BYTES, "catalogue byte limit");
    }
    return result + ']';
}
int select_alpn(SSL*,
                const unsigned char** out,
                unsigned char* outlen,
                const unsigned char* in,
                unsigned int inlen,
                void*)
{
    static const unsigned char protocols[] = "\x08http/1.1\x02h2";
    // Prefer HTTP/1.1 when offered (browsers), while gRPC offers h2 alone.
    unsigned char* selected = nullptr;
    const auto result = SSL_select_next_proto(
        &selected, outlen, protocols, sizeof(protocols) - 1, in, inlen);
    *out = selected;
    return result == OPENSSL_NPN_NEGOTIATED ? SSL_TLSEXT_ERR_OK : SSL_TLSEXT_ERR_NOACK;
}
} // namespace

class http_listener::impl
{
    struct connection {
        virtual ~connection() = default;
        virtual void close() = 0;
    };
    template <class Stream>
    class client : public connection, public std::enable_shared_from_this<client<Stream>>
    {
    public:
        template <class... Args>
        client(impl& owner, Args&&... args)
            : d_owner(owner),
              d_stream(std::forward<Args>(args)...),
              d_timer(owner.d_io),
              d_bridge(owner.d_io)
        {
        }
        void start()
        {
            d_timer.expires_after(std::chrono::seconds(5));
            auto self = this->shared_from_this();
            d_timer.async_wait([self](error_code ec) {
                if (!ec)
                    self->close();
            });
            if constexpr (std::is_same_v<Stream, tcp::socket>) {
                read_header();
            } else {
                d_stream.async_handshake(asio::ssl::stream_base::server,
                                         [self](error_code ec) {
                                             if (ec)
                                                 self->close();
                                             else
                                                 self->read_header();
                                         });
            }
        }
        void close() override
        {
            if (d_closed)
                return;
            d_closed = true;
            error_code ignored;
            d_timer.cancel();
            d_stream.lowest_layer().close(ignored);
            d_bridge.close(ignored);
            if (d_session.state) {
                d_session.state->transport_done(d_result);
                d_session.release();
                d_session.release = [] {};
            }
            d_owner.d_clients.erase(this->shared_from_this());
        }

    private:
        void read_header()
        {
            if (d_closed)
                return;
            auto self = this->shared_from_this();
            asio::async_read_until(
                d_stream,
                asio::dynamic_buffer(d_header, HEADER_LIMIT),
                "\r\n\r\n",
                [self](error_code ec, size_t) {
                    if (ec) {
                        if (ec == asio::error::not_found)
                            self->respond(431, "Headers too large\n");
                        else
                            self->close();
                    } else if (self->d_header.compare(0, 18, "PRI * HTTP/2.0\r\n\r\n") ==
                               0) {
                        self->start_grpc();
                    } else {
                        self->handle_http();
                    }
                });
        }
        void start_grpc()
        {
            if (d_closed)
                return;
            d_timer.cancel();
            if constexpr (std::is_same_v<Stream, tcp::socket>) {
                // Hand the socket and already-read preface directly to gRPC:
                // ordinary plaintext IQ traffic does not pass through a proxy.
                error_code ec;
                int fd = d_stream.release(ec);
                if (ec)
                    return close();
                d_owner.d_grpc(fd, d_header);
                close();
            } else {
                // gRPC accepts plaintext descriptors. Bridge decrypted TLS over
                // a local socket pair, with one bounded buffer per direction.
                int fds[2];
                if (::socketpair(
                        AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0, fds))
                    return close();
                error_code ec;
                d_bridge.assign(asio::local::stream_protocol(), fds[0], ec);
                if (ec) {
                    ::close(fds[0]);
                    ::close(fds[1]);
                    return close();
                }
                d_owner.d_grpc(fds[1], d_header);
                d_header.clear();
                read_front();
                read_back();
            }
        }
        void read_front()
        {
            if (d_closed)
                return;
            auto self = this->shared_from_this();
            d_stream.async_read_some(
                asio::buffer(d_front), [self](error_code ec, size_t n) {
                    if (ec)
                        return self->close();
                    asio::async_write(self->d_bridge,
                                      asio::buffer(self->d_front.data(), n),
                                      [self](error_code ec, size_t) {
                                          if (ec)
                                              self->close();
                                          else
                                              self->read_front();
                                      });
                });
        }
        void read_back()
        {
            if (d_closed)
                return;
            auto self = this->shared_from_this();
            d_bridge.async_read_some(
                asio::buffer(d_back), [self](error_code ec, size_t n) {
                    if (ec)
                        return self->close();
                    asio::async_write(self->d_stream,
                                      asio::buffer(self->d_back.data(), n),
                                      [self](error_code ec, size_t) {
                                          if (ec)
                                              self->close();
                                          else
                                              self->read_back();
                                      });
                });
        }
        void handle_http()
        {
            if (d_closed)
                return;
            http::request_parser<http::empty_body> parser;
            parser.header_limit(HEADER_LIMIT);
            parser.body_limit(0);
            error_code ec;
            parser.put(asio::buffer(d_header), ec);
            if (ec || !parser.is_done())
                return respond(400, "Invalid request\n");
            const auto& request = parser.get();
            const auto path = request.target().substr(0, request.target().find('?'));
            if (path == "/iq/v1/stream") {
                if (request.method() == http::verb::options)
                    return respond(204, "");
                if (!websocket::is_upgrade(request))
                    return respond(400, "WebSocket upgrade required\n");
                bool protocol = false;
                for (auto value :
                     http::token_list(request[http::field::sec_websocket_protocol]))
                    protocol |= value == "rustradio.iq.v1";
                if (!protocol)
                    return respond(400, "required subprotocol: rustradio.iq.v1\n");
                return upgrade_websocket();
            }
            if (path != "/iq/v1/streams")
                return respond(404, "Not found\n");
            if (request.method() == http::verb::options)
                return respond(204, "");
            if (request.method() != http::verb::get &&
                request.method() != http::verb::head)
                return respond(405, "Method not allowed\n");
            try {
                respond(200,
                        catalogue_json(d_owner.d_list()),
                        request.method() == http::verb::head);
            } catch (const std::exception&) {
                respond(503, "Catalogue unavailable\n");
            }
        }
        void upgrade_websocket()
        {
            try {
                d_session = d_owner.d_websocket();
            } catch (const std::exception&) {
                return respond(503, "Session unavailable\n");
            }
            d_websocket = std::make_unique<websocket::stream<Stream&>>(d_stream);
            d_websocket->read_message_max(MAX_ENVELOPE_BYTES);
            d_websocket->binary(true);
            d_websocket->auto_fragment(true);
            d_websocket->write_buffer_bytes(64 * 1024);
            d_websocket->set_option(
                websocket::stream_base::decorator([](websocket::response_type& response) {
                    response.set(http::field::sec_websocket_protocol, "rustradio.iq.v1");
                    response.set(http::field::access_control_allow_origin, "*");
                }));
            std::weak_ptr<client> weak = this->shared_from_this();
            // Session hooks can run on scheduler/watchdog threads under the session
            // mutex. Always post reactions; never reenter the session from a hook.
            d_session.state->bind(
                [weak](const std::string& bytes) {
                    if (auto self = weak.lock())
                        asio::post(self->d_owner.d_io,
                                   [self, bytes] { self->write_websocket(bytes); });
                },
                [] {},
                [weak](grpc::Status result) {
                    if (auto self = weak.lock())
                        asio::post(self->d_owner.d_io,
                                   [self, result] { self->finish_websocket(result); });
                },
                [weak] {
                    if (auto self = weak.lock())
                        asio::post(self->d_owner.d_io, [self] { self->close(); });
                },
                d_session.resolve);
            auto self = this->shared_from_this();
            // The buffered overload preserves any first frame coalesced with the
            // upgrade request. One binary WS message is one protobuf envelope.
            d_websocket->async_accept(asio::buffer(d_header), [self](error_code ec) {
                if (ec)
                    return self->close();
                self->d_timer.cancel();
                self->d_header.clear();
                self->read_websocket();
            });
        }
        void read_websocket()
        {
            if (d_closed || d_ws_finishing)
                return;
            auto self = this->shared_from_this();
            d_websocket->async_read(d_ws_input, [self](error_code ec, size_t) {
                if (self->d_closed)
                    return;
                if (ec)
                    return self->close();
                if (!self->d_websocket->got_binary()) {
                    self->d_session.state->local_failure(
                        grpc::StatusCode::INVALID_ARGUMENT,
                        "WebSocket messages must be binary");
                    return;
                }
                try {
                    self->d_session.state->received(
                        boost::beast::buffers_to_string(self->d_ws_input.data()));
                } catch (const protocol_error& error) {
                    self->d_session.state->local_failure(error.code(), error.what());
                    return;
                }
                self->d_ws_input.consume(self->d_ws_input.size());
                self->read_websocket();
            });
        }
        void write_websocket(const std::string& bytes)
        {
            if (d_closed || d_ws_finishing)
                return;
            d_ws_output = bytes;
            auto self = this->shared_from_this();
            d_websocket->async_write(asio::buffer(d_ws_output),
                                     [self](error_code ec, size_t) {
                                         if (self->d_closed)
                                             return;
                                         self->d_ws_output.clear();
                                         self->d_session.state->write_done(!ec);
                                         if (ec)
                                             self->close();
                                     });
        }
        void finish_websocket(const grpc::Status& result)
        {
            if (d_closed || d_ws_finishing)
                return;
            d_ws_finishing = true;
            d_result = result;
            auto code = websocket::close_code::normal;
            if (!result.ok())
                code = result.error_code() == grpc::StatusCode::INVALID_ARGUMENT ||
                               result.error_code() == grpc::StatusCode::DATA_LOSS ||
                               result.error_code() == grpc::StatusCode::RESOURCE_EXHAUSTED
                           ? websocket::close_code::protocol_error
                           : websocket::close_code::internal_error;
            // Do not let a peer that ignores the closing handshake retain state.
            d_timer.expires_after(std::chrono::seconds(5));
            auto self = this->shared_from_this();
            d_timer.async_wait([self](error_code ec) {
                if (!ec)
                    self->close();
            });
            d_websocket->async_close(code, [self](error_code) { self->close(); });
        }
        void respond(unsigned status, std::string body, bool head = false)
        {
            if (d_closed)
                return;
            d_response.version(11);
            d_response.result(status);
            d_response.set(http::field::content_type,
                           status == 200 ? "application/json" : "text/plain");
            d_response.set(http::field::access_control_allow_origin, "*");
            d_response.set(http::field::access_control_allow_methods,
                           "GET, HEAD, OPTIONS");
            d_response.set(http::field::access_control_allow_headers, "Content-Type");
            if (status == 405)
                d_response.set(http::field::allow, "GET, HEAD, OPTIONS");
            d_response.keep_alive(false);
            d_response.body() = std::move(body);
            d_response.prepare_payload();
            if (head)
                d_response.body().clear();
            auto self = this->shared_from_this();
            http::async_write(d_stream, d_response, [self](error_code, size_t) {
                if constexpr (std::is_same_v<Stream, tcp::socket>) {
                    self->close();
                } else {
                    // Send close_notify so HTTPS readers see a clean EOF.
                    self->d_stream.async_shutdown([self](error_code) { self->close(); });
                }
            });
        }
        impl& d_owner;
        Stream d_stream;
        asio::steady_timer d_timer;
        asio::local::stream_protocol::socket d_bridge;
        std::string d_header;
        http::response<http::string_body> d_response;
        std::array<char, 32768> d_front, d_back;
        bool d_closed = false;
        bool d_ws_finishing = false;
        websocket_session d_session;
        std::unique_ptr<websocket::stream<Stream&>> d_websocket;
        boost::beast::flat_buffer d_ws_input{ MAX_ENVELOPE_BYTES };
        std::string d_ws_output;
        grpc::Status d_result{ grpc::StatusCode::CANCELLED, "WebSocket disconnected" };
    };

public:
    impl(const std::string& address,
         const tls_options& tls,
         std::function<std::vector<stream_listing>()> list,
         handoff grpc,
         websocket_factory websocket)
        : d_acceptor(d_io),
          d_tls(asio::ssl::context::tls_server),
          d_secure(tls.enabled),
          d_list(std::move(list)),
          d_grpc(std::move(grpc)),
          d_websocket(std::move(websocket))
    {
        if (d_secure) {
            require(!tls.certificate_chain.empty() && !tls.private_key.empty(),
                    "TLS server certificate/key required");
            d_tls.set_options(
                asio::ssl::context::default_workarounds | asio::ssl::context::no_sslv2 |
                asio::ssl::context::no_sslv3 | asio::ssl::context::no_tlsv1 |
                asio::ssl::context::no_tlsv1_1);
            d_tls.use_certificate_chain(asio::buffer(tls.certificate_chain));
            d_tls.use_private_key(asio::buffer(tls.private_key), asio::ssl::context::pem);
            require(SSL_CTX_check_private_key(d_tls.native_handle()) == 1,
                    "TLS certificate and key do not match");
            if (tls.root_certificates.empty())
                d_tls.set_default_verify_paths();
            else
                d_tls.add_certificate_authority(asio::buffer(tls.root_certificates));
            if (tls.require_client_certificate)
                d_tls.set_verify_mode(asio::ssl::verify_peer |
                                      asio::ssl::verify_fail_if_no_peer_cert);
            SSL_CTX_set_alpn_select_cb(d_tls.native_handle(), select_alpn, nullptr);
        }
        const auto colon = address.rfind(':');
        require(colon != std::string::npos, "listener requires host:port");
        auto host = address.substr(0, colon);
        if (host.size() >= 2 && host.front() == '[' && host.back() == ']')
            host = host.substr(1, host.size() - 2);
        tcp::resolver resolver(d_io);
        const auto endpoints =
            resolver.resolve(host, address.substr(colon + 1), tcp::resolver::passive);
        error_code ec = asio::error::host_not_found;
        for (const auto& endpoint : endpoints) {
            d_acceptor.open(endpoint.endpoint().protocol(), ec);
            if (!ec)
                d_acceptor.set_option(tcp::acceptor::reuse_address(true), ec);
            if (!ec)
                d_acceptor.bind(endpoint, ec);
            if (!ec)
                d_acceptor.listen(asio::socket_base::max_listen_connections, ec);
            if (!ec)
                break;
            error_code ignored;
            d_acceptor.close(ignored);
        }
        require(!ec,
                "cannot bind IQ listener: " + ec.message(),
                grpc::StatusCode::UNAVAILABLE);
        d_address = address.substr(0, colon + 1) +
                    std::to_string(d_acceptor.local_endpoint().port());
        accept();
        d_thread = std::thread([this] { d_io.run(); });
    }
    ~impl() { shutdown(); }
    void shutdown()
    {
        if (!d_thread.joinable())
            return;
        asio::post(d_io, [this] {
            d_stopped = true;
            error_code ignored;
            d_acceptor.close(ignored);
            while (!d_clients.empty())
                (*d_clients.begin())->close();
        });
        d_thread.join();
    }
    std::string address() const { return d_address; }

private:
    void accept()
    {
        d_acceptor.async_accept([this](error_code ec, tcp::socket socket) {
            if (ec || d_stopped)
                return;
            if (d_clients.size() < CONNECTION_LIMIT) {
                if (d_secure) {
                    auto entry = std::make_shared<client<asio::ssl::stream<tcp::socket>>>(
                        *this, std::move(socket), d_tls);
                    d_clients.insert(entry);
                    entry->start();
                } else {
                    auto entry =
                        std::make_shared<client<tcp::socket>>(*this, std::move(socket));
                    d_clients.insert(entry);
                    entry->start();
                }
            }
            accept();
        });
    }
    asio::io_context d_io;
    tcp::acceptor d_acceptor;
    asio::ssl::context d_tls;
    bool d_secure;
    bool d_stopped = false;
    std::function<std::vector<stream_listing>()> d_list;
    handoff d_grpc;
    websocket_factory d_websocket;
    std::set<std::shared_ptr<connection>> d_clients;
    std::string d_address;
    std::thread d_thread;
};
http_listener::http_listener(const std::string& address,
                             const tls_options& tls,
                             std::function<std::vector<stream_listing>()> list,
                             handoff grpc,
                             websocket_factory websocket)
    : d_impl(std::make_unique<impl>(
          address, tls, std::move(list), std::move(grpc), std::move(websocket)))
{
}
http_listener::~http_listener() = default;
std::string http_listener::address() const { return d_impl->address(); }
void http_listener::shutdown() { d_impl->shutdown(); }
} // namespace iqstream
} // namespace gr
