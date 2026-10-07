"""Native gRPC real/complex sample streaming for GNU Radio 3.10.12."""
from .iqstream_python import *  # noqa: F403


def configured_options(options, *, sample_rate_hz=None, profile=None, loss=None):
    """Copy options for a GRC block without modifying a shared configuration."""
    result = stream_options()  # noqa: F405
    for name in (
        "sample_rate_hz", "loss", "profile", "string_to_symbol", "blocking",
        "max_frame_bytes", "max_in_flight_frames", "handshake_timeout_ms",
        "shutdown_timeout_ms", "transfer_timeout_ms", "source_id",
        "source_sample_offset", "sample_zero_seconds", "sample_zero_nanos",
        "properties", "tls", "grpc_metadata",
    ):
        setattr(result, name, getattr(options, name))
    if sample_rate_hz is not None:
        result.sample_rate_hz = sample_rate_hz
    if profile is not None:
        result.profile = profile
    if loss is not None:
        result.loss = loss
    return result


def endpoint_address(value):
    """Resolve a shared listener or a gRPC address for Companion client blocks."""
    return value.address() if isinstance(value, server) else value  # noqa: F405
