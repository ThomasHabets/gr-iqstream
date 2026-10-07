/* SPDX-License-Identifier: GPL-3.0-or-later */
#ifndef INCLUDED_GR_IQSTREAM_TEST_H
#define INCLUDED_GR_IQSTREAM_TEST_H
#include "protocol.h"
#include <iostream>
#include <stdexcept>
#define CHECK(EXPR)                                                          \
    do {                                                                     \
        if (!(EXPR))                                                         \
            throw std::runtime_error(std::string("check failed: ") + #EXPR); \
    } while (false)
template <class F>
void rejects(F&& f, grpc::StatusCode code = grpc::StatusCode::INVALID_ARGUMENT)
{
    try {
        f();
    } catch (const gr::iqstream::protocol_error& e) {
        CHECK(e.code() == code);
        return;
    }
    throw std::runtime_error("expected rejection");
}

#endif /* INCLUDED_GR_IQSTREAM_TEST_H */
