/* SPDX-License-Identifier: MIT */
#include <gnuradio/iqstream/align_streams.h>
#include <gnuradio/iqstream/server_sink.h>
#include <gnuradio/iqstream/server_source.h>
#include <gnuradio/iqstream/sink.h>
#include <gnuradio/iqstream/source.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

namespace py = pybind11;
using namespace gr::iqstream;
namespace {
template <class Block>
void bind_block(py::module_& m, const char* name, bool serving)
{
    auto cls = py::
        class_<Block, gr::sync_block, gr::block, gr::basic_block, std::shared_ptr<Block>>(
            m, name);
    if (serving) {
        // Argument names are selected at compile time below, because make() types differ.
        cls.def(py::init(&Block::make),
                py::arg("listener"),
                py::arg("resource"),
                py::arg("layout") = sample_layout::COMPLEX,
                py::arg("options") = stream_options{});
    } else {
        cls.def(py::init(&Block::make),
                py::arg("address"),
                py::arg("resource"),
                py::arg("layout") = sample_layout::COMPLEX,
                py::arg("options") = stream_options{});
    }
    cls.def("status", &Block::status).def("cancel", &Block::cancel);
}
} // namespace
PYBIND11_MODULE(iqstream_python, m)
{
    py::module_::import("gnuradio.gr");
    py::module_::import("pmt");
    py::enum_<sample_layout>(m, "sample_layout")
        .value("REAL", sample_layout::REAL)
        .value("COMPLEX", sample_layout::COMPLEX);
    py::enum_<loss_policy>(m, "loss_policy")
        .value("LOSSLESS", loss_policy::LOSSLESS)
        .value("ALLOW_GAPS", loss_policy::ALLOW_GAPS);
    py::enum_<metadata_profile>(m, "metadata_profile")
        .value("NATIVE", metadata_profile::NATIVE)
        .value("RUSTRADIO", metadata_profile::RUSTRADIO);
    py::enum_<session_state>(m, "session_state")
        .value("IDLE", session_state::IDLE)
        .value("OPENING", session_state::OPENING)
        .value("STREAMING", session_state::STREAMING)
        .value("FINISHING", session_state::FINISHING)
        .value("COMPLETE", session_state::COMPLETE)
        .value("FAILED", session_state::FAILED)
        .value("CANCELLED", session_state::CANCELLED);
    py::class_<tls_options>(m, "tls_options")
        .def(py::init<>())
        .def_readwrite("enabled", &tls_options::enabled)
        .def_readwrite("require_client_certificate",
                       &tls_options::require_client_certificate)
        .def_readwrite("root_certificates", &tls_options::root_certificates)
        .def_readwrite("certificate_chain", &tls_options::certificate_chain)
        .def_readwrite("private_key", &tls_options::private_key);
    py::class_<stream_options>(m, "stream_options")
        .def(py::init<>())
#define OPTION(NAME) .def_readwrite(#NAME, &stream_options::NAME)
            OPTION(sample_rate_hz) OPTION(loss) OPTION(profile) OPTION(blocking)
                OPTION(max_frame_bytes) OPTION(max_in_flight_frames)
                    OPTION(handshake_timeout_ms) OPTION(shutdown_timeout_ms)
                        OPTION(transfer_timeout_ms) OPTION(source_id)
                            OPTION(source_sample_offset) OPTION(sample_zero_seconds)
                                OPTION(sample_zero_nanos) OPTION(properties) OPTION(tls)
                                    OPTION(grpc_metadata);
#undef OPTION
    py::class_<session_status>(m, "session_status")
#define STATUS(NAME) .def_readonly(#NAME, &session_status::NAME)
        STATUS(state) STATUS(next_sequence) STATUS(next_sample) STATUS(lost_samples)
            STATUS(trailing_gap) STATUS(completion_uncertain) STATUS(grpc_status_code)
                STATUS(message) STATUS(sample_rate_hz) STATUS(source_sample_offset)
#undef STATUS
                    .def_property_readonly("description_bytes",
                                           [](const session_status& s) {
                                               return py::bytes(s.description_bytes);
                                           });
    py::class_<server, server::sptr>(m, "server")
        .def(py::init(&server::make),
             py::arg("address") = "127.0.0.1:50051",
             py::arg("tls") = tls_options{})
        .def("address", &server::address)
        .def("shutdown", &server::shutdown, py::call_guard<py::gil_scoped_release>());
    bind_block<source>(m, "source", false);
    bind_block<sink>(m, "sink", false);
    bind_block<server_source>(m, "server_source", true);
    bind_block<server_sink>(m, "server_sink", true);
    py::class_<align_streams, gr::block, gr::basic_block, align_streams::sptr>(
        m, "align_streams")
        .def(py::init(&align_streams::make),
             py::arg("itemsize0"),
             py::arg("itemsize1"),
             py::arg("tag_key0") = "rustradio.iq.absolute_sample_index",
             py::arg("tag_key1") = "rustradio.iq.absolute_sample_index");
    m.attr("ABSOLUTE_SAMPLE_INDEX") = "rustradio.iq.absolute_sample_index";
    m.attr("GAP_SAMPLES") = "rustradio.iq.gap_samples";
    m.attr("SAMPLE_INDEX") = "rustradio.iq.sample_index";
}
