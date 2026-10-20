/* SPDX-License-Identifier: MIT */
#ifndef INCLUDED_GR_IQSTREAM_SERVER_IMPL_H
#define INCLUDED_GR_IQSTREAM_SERVER_IMPL_H
#include "http_listener.h"
#include "resource.h"
#include <gnuradio/iqstream/server.h>
#include <grpcpp/generic/async_generic_service.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <atomic>
#include <map>
#include <thread>
namespace gr {
namespace iqstream {
class server_impl : public grpc::CallbackGenericService
{
public:
    static std::shared_ptr<server_impl> get(const server::sptr& listener)
    {
        return listener->d_impl;
    }
    server_impl(const std::string& address, const tls_options& tls);
    ~server_impl() override;
    std::shared_ptr<resource> add_resource(const std::string& id,
                                           bool sending,
                                           sample_layout layout,
                                           const stream_options& options);
    void remove_resource(const std::string& id, const std::shared_ptr<resource>& entry);
    void reset_resource(const std::shared_ptr<resource>& entry);
    resource_binding resolve(const wire::Open& open,
                             const std::shared_ptr<session>& state);
    grpc::ServerGenericBidiReactor*
    CreateReactor(grpc::GenericCallbackServerContext* context) override;
    std::string address() const;
    void register_all_streams(metadata_profile profile, double default_sample_rate_hz);
    std::vector<stream_listing> streams() const;
    void shutdown();

private:
    mutable std::mutex d_mutex;
    std::mutex d_shutdown_mutex;
    std::map<std::string, std::shared_ptr<resource>> d_resources;
    std::vector<std::weak_ptr<session>> d_sessions;
    std::unique_ptr<grpc::experimental::ExternalConnectionAcceptor> d_acceptor;
    std::unique_ptr<grpc::Server> d_server;
    std::unique_ptr<http_listener> d_listener;
    std::string d_address;
    std::atomic<size_t> d_active_sessions{ 0 };
    std::atomic<bool> d_stopping{ false };
    std::thread d_watchdog;
    std::atomic<bool> d_observe{ false };
    std::atomic<double> d_observe_default_sample_rate_hz{ 0.0 };
    std::atomic<metadata_profile> d_observe_profile{ metadata_profile::NATIVE };
    struct observation {
        gr::stream_subscription::sptr subscription;
        std::shared_ptr<session> connection;
        uint64_t origin;
    };
    std::map<uint64_t, std::shared_ptr<observation>> d_observations;
    resource_binding resolve_observed(const wire::Open& open,
                                      const std::shared_ptr<session>& state);
    void poll_observations();
};
} // namespace iqstream
} // namespace gr

#endif /* INCLUDED_GR_IQSTREAM_SERVER_IMPL_H */
