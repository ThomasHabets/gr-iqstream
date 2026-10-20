/* SPDX-License-Identifier: MIT */
#include "client_connection.h"
#include "server_impl.h"
#include "test.h"
#include <grpcpp/create_channel.h>
#include <grpcpp/generic/generic_stub.h>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <cstring>
#include <future>

using namespace gr::iqstream;
std::shared_ptr<session> accepted(const std::shared_ptr<resource>& resource)
{
    std::unique_lock<std::mutex> lock(resource->mutex);
    CHECK(resource->cv.wait_for(lock, std::chrono::seconds(5), [&] {
        return static_cast<bool>(resource->connection);
    }));
    return resource->connection;
}
void roundtrip(bool upload, sample_layout layout, size_t samples)
{
    stream_options options;
    options.sample_rate_hz = 48000;
    options.max_frame_bytes = 512;
    options.max_in_flight_frames = 1;
    auto listener = server::make("127.0.0.1:0");
    auto resource =
        server_impl::get(listener)->add_resource("iq", !upload, layout, options);
    auto client = std::make_shared<session>(true, upload, layout, options, "iq");
    client_connection transport(listener->address(), client, options);
    auto remote = accepted(resource);
    auto sender = upload ? client : remote;
    auto receiver = upload ? remote : client;
    const size_t stride = layout == sample_layout::REAL ? 4 : 8;
    std::string payload(samples * stride, '\0');
    for (size_t i = 0; i < payload.size(); ++i)
        payload[i] = static_cast<char>(i * 37);
    gr::tag_t tag;
    tag.offset = 3;
    tag.key = pmt::intern("u64");
    tag.value = pmt::from_uint64(UINT64_MAX);
    tag.srcid = pmt::intern("");
    auto producer = std::async(std::launch::async, [&] {
        size_t offset = 0;
        while (offset < samples) {
            auto n = sender->push(
                payload.data() + offset * stride, samples - offset, { tag, tag }, offset);
            CHECK(n);
            offset += n;
        }
        sender->finish_sending();
    });
    std::string result;
    size_t tag_count = 0;
    for (;;) {
        char out[24];
        std::vector<gr::tag_t> tags;
        int n = receiver->pull(out, 3, result.size() / stride, tags);
        if (n < 0)
            break;
        result.append(out, n * stride);
        for (const auto& t : tags) {
            CHECK(t.offset == 3);
            CHECK(pmt::to_uint64(t.value) == UINT64_MAX);
            CHECK(pmt::is_symbol(t.srcid) && pmt::symbol_to_string(t.srcid).empty());
            ++tag_count;
        }
    }
    producer.get();
    CHECK(sender->wait_done(std::chrono::seconds(5)));
    CHECK(receiver->wait_done(std::chrono::seconds(5)));
    CHECK(sender->status().state == session_state::COMPLETE);
    CHECK(receiver->status().state == session_state::COMPLETE);
    CHECK(result == payload);
    CHECK(tag_count == (samples > 3 ? 2 : 0));
    listener->shutdown();
}
void gaps(bool upload, bool retain_after_gap)
{
    stream_options options;
    options.loss = loss_policy::ALLOW_GAPS;
    options.blocking = false;
    options.max_frame_bytes = 512;
    options.max_in_flight_frames = 1;
    auto listener = server::make("127.0.0.1:0");
    auto entry = server_impl::get(listener)->add_resource(
        "iq", !upload, sample_layout::REAL, options);
    auto client =
        std::make_shared<session>(true, upload, sample_layout::REAL, options, "iq");
    client_connection transport(listener->address(), client, options);
    auto remote = accepted(entry);
    auto sender = upload ? client : remote;
    auto receiver = upload ? remote : client;
    std::vector<float> samples(1000, 0.25f);
    size_t consumed = 0;
    while (consumed < samples.size()) {
        auto n = sender->push(
            samples.data() + consumed, samples.size() - consumed, {}, consumed);
        CHECK(n);
        consumed += n;
    }
    auto lost = sender->status().lost_samples;
    CHECK(lost > 0 && lost < samples.size());
    auto ending = std::async(std::launch::async, [&] {
        if (retain_after_gap) {
            const auto deadline =
                std::chrono::steady_clock::now() + std::chrono::seconds(5);
            while (sender->status().next_sample != samples.size()) {
                CHECK(std::chrono::steady_clock::now() < deadline);
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            float last = 0.5f;
            CHECK(sender->push(&last, 1, {}, samples.size()) == 1);
        }
        sender->finish_sending();
    });
    size_t retained = 0, markers = 0;
    for (;;) {
        float data[3];
        std::vector<gr::tag_t> tags;
        auto n = receiver->pull(data, 3, retained, tags);
        if (n < 0)
            break;
        for (const auto& t : tags) {
            CHECK(retain_after_gap);
            CHECK(t.offset == samples.size() - lost);
            if (pmt::symbol_to_string(t.key) == GAP_SAMPLES)
                CHECK(pmt::to_uint64(t.value) == lost);
            else {
                CHECK(pmt::symbol_to_string(t.key) == SAMPLE_INDEX);
                CHECK(pmt::to_uint64(t.value) == samples.size());
            }
            ++markers;
        }
        retained += n;
    }
    ending.get();
    CHECK(sender->wait_done(std::chrono::seconds(5)));
    CHECK(receiver->status().state == session_state::COMPLETE);
    CHECK(sender->status().state == session_state::COMPLETE);
    CHECK(retained == samples.size() - lost + (retain_after_gap ? 1 : 0));
    CHECK(receiver->status().lost_samples == lost);
    CHECK(receiver->status().trailing_gap == (retain_after_gap ? 0 : lost));
    CHECK(markers == (retain_after_gap ? 2 : 0));
    CHECK(receiver->status().next_sample == samples.size() + (retain_after_gap ? 1 : 0));
    listener->shutdown();
}
void completion_timeout()
{
    stream_options options;
    options.max_in_flight_frames = 1;
    options.shutdown_timeout_ms = 100;
    auto receiver_options = options;
    // Only the sender should expire in this test. With equal deadlines, the
    // receiver watchdog can abort first and the sender legitimately sees CANCELLED.
    receiver_options.shutdown_timeout_ms = 5000;
    auto listener = server::make("127.0.0.1:0");
    auto entry = server_impl::get(listener)->add_resource(
        "iq", false, sample_layout::REAL, receiver_options);
    auto sender =
        std::make_shared<session>(true, true, sample_layout::REAL, options, "iq");
    client_connection transport(listener->address(), sender, options);
    auto receiver = accepted(entry);
    float sample = 1;
    CHECK(sender->push(&sample, 1, {}, 0) == 1);
    sender->finish_sending();
    // Receiver never accepts the frame into graph buffers, so it cannot Complete.
    CHECK(sender->wait_done(std::chrono::seconds(3)));
    CHECK(sender->status().state == session_state::FAILED);
    CHECK(sender->status().grpc_status_code == grpc::StatusCode::DEADLINE_EXCEEDED);
    CHECK(sender->status().completion_uncertain);
    listener->shutdown();
}
void failures()
{
    stream_options options;
    options.handshake_timeout_ms = 100;
    options.shutdown_timeout_ms = 100;
    auto listener = server::make("127.0.0.1:0");
    auto client =
        std::make_shared<session>(true, false, sample_layout::REAL, options, "missing");
    client_connection transport(listener->address(), client, options);
    CHECK(client->wait_done(std::chrono::seconds(3)));
    CHECK(client->status().state == session_state::FAILED);
    CHECK(client->status().grpc_status_code == grpc::StatusCode::NOT_FOUND);
    listener->shutdown();
    auto stalled =
        std::make_shared<session>(true, false, sample_layout::REAL, options, "iq");
    client_connection unavailable("127.0.0.1:1", stalled, options);
    CHECK(stalled->wait_done(std::chrono::seconds(3)));
    CHECK(stalled->status().state != session_state::COMPLETE);
}
wire::ListStreamsResponse query_catalogue(const std::string& address)
{
    grpc::GenericStub stub(
        grpc::CreateChannel(address, grpc::InsecureChannelCredentials()));
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
    grpc::CompletionQueue queue;
    grpc::Slice slice("");
    grpc::ByteBuffer request(&slice, 1), response;
    auto call = stub.PrepareUnaryCall(
        &context, "/rustradio.iq.v1.IqStreaming/ListStreams", request, &queue);
    CHECK(call);
    grpc::Status status;
    call->StartCall();
    call->Finish(&response, &status, reinterpret_cast<void*>(1));
    void* tag;
    bool ok;
    CHECK(queue.Next(&tag, &ok) && ok && tag == reinterpret_cast<void*>(1));
    CHECK(status.ok());
    std::vector<grpc::Slice> slices;
    CHECK(response.Dump(&slices).ok());
    std::string data;
    for (const auto& part : slices)
        data.append(reinterpret_cast<const char*>(part.begin()), part.size());
    wire::ListStreamsResponse result;
    CHECK(result.ParseFromString(data));
    queue.Shutdown();
    return result;
}
std::string http_request(const std::string& address, const std::string& request)
{
    boost::asio::ip::tcp::iostream socket;
    socket.expires_after(std::chrono::seconds(5));
    const auto colon = address.rfind(':');
    socket.connect(address.substr(0, colon), address.substr(colon + 1));
    CHECK(socket);
    socket << request << std::flush;
    CHECK(socket);
    std::string response((std::istreambuf_iterator<char>(socket)), {});
    CHECK(!response.empty());
    return response;
}
void catalogue()
{
    auto listener = server::make("127.0.0.1:0");
    listener->register_all_streams(metadata_profile::NATIVE, 96000);
    stream_options options;
    options.sample_rate_hz = 0;
    auto download = server_impl::get(listener)->add_resource(
        "download", true, sample_layout::COMPLEX, options);
    auto upload = server_impl::get(listener)->add_resource(
        "upload", false, sample_layout::REAL, options);
    auto local = listener->streams();
    CHECK(local.size() == 1 && local[0].name == "download");
    CHECK(!local[0].sample_rate_hz);
    const auto response =
        http_request(listener->address(),
                     "GET /iq/v1/streams HTTP/1.1\r\nHost: localhost\r\n"
                     "Origin: http://localhost:8080\r\n\r\n");
    CHECK(response.find("HTTP/1.1 200") == 0);
    CHECK(response.find("Access-Control-Allow-Origin: *\r\n") != std::string::npos);
    const auto body = response.substr(response.find("\r\n\r\n") + 4);
    CHECK(body.find("\"name\":\"download\"") != std::string::npos);
    CHECK(body.find("upload") == std::string::npos);
    CHECK(body.find("\"sample_rate_hz\":null") != std::string::npos);
    CHECK(http_request(listener->address(),
                       "OPTIONS /iq/v1/streams HTTP/1.1\r\nHost: localhost\r\n\r\n")
              .find("HTTP/1.1 204") == 0);
    CHECK(http_request(listener->address(),
                       "GET /iq/v1/streams HTTP/1.1\r\nX-Large: " +
                           std::string(17000, 'x') + "\r\n\r\n")
              .find("HTTP/1.1 431") == 0);
    auto result = query_catalogue(listener->address());
    CHECK(result.streams_size() == 1);
    CHECK(result.streams(0).name() == "download");
    CHECK(result.streams(0).encoding().layout() == wire::SAMPLE_LAYOUT_COMPLEX);
    CHECK(!result.streams(0).has_sample_rate_hz());
    listener->shutdown();
}
void websocket_roundtrip()
{
    namespace asio = boost::asio;
    namespace beast = boost::beast;
    auto listener = server::make("127.0.0.1:0");
    stream_options options;
    options.sample_rate_hz = 48000;
    options.max_in_flight_frames = 1;
    auto resource = server_impl::get(listener)->add_resource(
        "iq", true, sample_layout::REAL, options);
    asio::io_context io;
    beast::websocket::stream<beast::tcp_stream> socket(io);
    const auto address = listener->address();
    const auto colon = address.rfind(':');
    asio::ip::tcp::resolver resolver(io);
    socket.next_layer().connect(
        resolver.resolve(address.substr(0, colon), address.substr(colon + 1)));
    socket.set_option(beast::websocket::stream_base::decorator(
        [](beast::websocket::request_type& request) {
            request.set(beast::http::field::sec_websocket_protocol, "rustradio.iq.v1");
        }));
    socket.handshake(address, "/iq/v1/stream");
    socket.binary(true);
    auto send = [&](const wire::ClientMessage& message) {
        auto data = message.SerializeAsString();
        socket.write(asio::buffer(data));
    };
    auto receive = [&] {
        beast::flat_buffer input;
        socket.read(input);
        CHECK(socket.got_binary());
        wire::ServerMessage message;
        CHECK(message.ParseFromString(beast::buffers_to_string(input.data())));
        return message;
    };
    std::string open;
    session client(true, false, sample_layout::REAL, options, "iq");
    client.bind(
        [&](const auto& bytes) { open = bytes; }, [] {}, [](grpc::Status) {}, [] {});
    client.begin_client();
    socket.write(asio::buffer(open));
    CHECK(receive().has_started());
    auto remote = accepted(resource);
    wire::ClientMessage credit;
    credit.mutable_flow_control()->set_send_limit(1);
    send(credit);
    auto producer = std::async(std::launch::async, [&] {
        float data[] = { 1, 2, 3, 4 };
        CHECK(remote->push(data, 4, {}, 0) == 4);
        remote->finish_sending();
    });
    auto frame = receive();
    if (!frame.has_frame())
        throw std::runtime_error(frame.DebugString());
    CHECK(frame.frame().chunk().sample_count() == 4);
    auto end = receive();
    CHECK(end.has_end() && end.end().next_sample() == 4);
    wire::ClientMessage complete;
    complete.mutable_complete()->set_next_sample(4);
    complete.mutable_complete()->set_next_sequence(1);
    complete.mutable_complete()->set_completion_mode(wire::COMPLETION_MODE_ACCEPTED);
    send(complete);
    CHECK(receive().has_complete());
    beast::flat_buffer input;
    boost::system::error_code ec;
    socket.read(input, ec);
    CHECK(ec == beast::websocket::error::closed);
    CHECK(socket.reason().code == beast::websocket::close_code::normal);
    producer.get();
    CHECK(remote->wait_done(std::chrono::seconds(5)));
    CHECK(remote->status().state == session_state::COMPLETE);
    listener->shutdown();
}
int main(int argc, char** argv)
{
    try {
        if (argc == 3 && std::string(argv[1]) == "--catalogue") {
            auto result = query_catalogue(argv[2]);
            CHECK(result.streams_size() == 1 && result.streams(0).name() == "iq");
            CHECK(result.streams(0).sample_rate_hz() == 48000);
            std::cout << "Remote catalogue passed\n";
            return 0;
        }
        for (bool upload : { false, true })
            for (auto layout : { sample_layout::REAL, sample_layout::COMPLEX }) {
                roundtrip(upload, layout, 0);
                roundtrip(upload, layout, 1000);
            }
        for (bool upload : { false, true })
            for (bool retained : { false, true })
                gaps(upload, retained);
        catalogue();
        websocket_roundtrip();
        completion_timeout();
        failures();
        std::cout << "gRPC transport acceptance passed\n";
    } catch (const std::exception& e) {
        std::cerr << e.what() << '\n';
        return 1;
    }
}
