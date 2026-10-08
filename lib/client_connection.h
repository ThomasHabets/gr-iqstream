/* SPDX-License-Identifier: MIT */
#ifndef INCLUDED_GR_IQSTREAM_CLIENT_CONNECTION_H
#define INCLUDED_GR_IQSTREAM_CLIENT_CONNECTION_H
#include "session.h"
#include <thread>
namespace gr {
namespace iqstream {
class client_connection
{
public:
    client_connection(const std::string& address,
                      const std::shared_ptr<session>& state,
                      const stream_options& options);
    ~client_connection();

private:
    class reactor;
    std::shared_ptr<session> d_session;
    std::unique_ptr<reactor> d_reactor;
    std::thread d_watchdog;
};
} // namespace iqstream
} // namespace gr

#endif /* INCLUDED_GR_IQSTREAM_CLIENT_CONNECTION_H */
