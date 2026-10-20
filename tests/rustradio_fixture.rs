// SPDX-License-Identifier: MIT
//! Independent interop fixture. It depends on upstream APIs without patching them.
use futures_util::{SinkExt, StreamExt};
use rustradio::blocks::{IqStreamSink, IqStreamSource, VectorSink, VectorSource};
use rustradio::graph::{Graph, GraphRunner};
use rustradio::iq_stream::{IqSample, IqServer, SourceStatus, StreamOptions};
use rustradio::stream::{Tag, TagValue};
use rustradio::{Complex, Float};
use std::io::{self, Write};
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};

fn tags() -> Vec<Tag> {
    vec![
        Tag::new(123, "u64", TagValue::U64(u64::MAX)),
        Tag::new(123, "false", TagValue::Bool(false)),
        Tag::new(123, "empty", TagValue::String(String::new())),
        Tag::new(124, "i64", TagValue::I64(i64::MIN)),
        Tag::new(124, "float", TagValue::Float(0.25)),
    ]
}
async fn serve<T: IqSample>(values: Vec<T>) -> Result<(), Box<dyn std::error::Error>> {
    let server = IqServer::new();
    let (source, input) = VectorSource::builder(values).tags(&tags()).build()?;
    let sink = IqStreamSink::builder(input, &server, "iq", 48000.0)
        .blocking(true)
        .build()?;
    let mut graph = Graph::new();
    graph.add(Box::new(source));
    graph.add(Box::new(sink));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let serving = tokio::spawn(async move {
        server
            .serve(listener, async {
                let _ = stopped.await;
            })
            .await
    });
    println!("{address}");
    io::stdout().flush()?;
    tokio::task::spawn_blocking(move || graph.run()).await??;
    let _ = stop.send(());
    serving.await??;
    Ok(())
}
// Exercise the same SourceTransport API used by the browser's WASM adapter.
async fn connect_websocket<T: IqSample>(
    address: &str,
    resource: &str,
    options: &StreamOptions,
) -> Result<
    (
        IqStreamSource<T>,
        rustradio::stream::ReadStream<T>,
        rustradio::iq_stream::SourceHandle,
    ),
    Box<dyn std::error::Error>,
> {
    let mut request = format!("ws://{address}/iq/v1/stream").into_client_request()?;
    request
        .headers_mut()
        .insert("Sec-WebSocket-Protocol", "rustradio.iq.v1".parse()?);
    let (mut socket, response) = tokio_tungstenite::connect_async(request).await?;
    assert_eq!(
        response.headers()["sec-websocket-protocol"],
        "rustradio.iq.v1"
    );
    socket
        .send(Message::Binary(
            rustradio::iq_stream::encode_client(&options.open::<T>(resource)?)?.into(),
        ))
        .await?;
    let message = socket.next().await.ok_or("missing Started")??;
    let Message::Binary(bytes) = message else {
        return Err("nonbinary Started".into());
    };
    let decoded = rustradio::iq_stream::decode_server(&bytes)?;
    let Some(rustradio::iq_stream::proto::server_message::Body::Started(started)) =
        decoded.message.body
    else {
        return Err("expected Started".into());
    };
    let (source, input, handle, transport) = IqStreamSource::<T>::from_started(started, options)?;
    tokio::spawn(async move {
        let result: Result<(), Box<dyn std::error::Error + Send + Sync>> = async {
            loop {
                tokio::select! {
                    control = transport.next_control() => {
                        let Some(control) = control else { return Err("control channel ended before close".into()); };
                        socket.send(Message::Binary(rustradio::iq_stream::encode_client(&control)?.into())).await?;
                    }
                    message = socket.next() => match message.ok_or("WebSocket disconnected")?? {
                        Message::Binary(bytes) => transport.accept(rustradio::iq_stream::decode_server(&bytes)?)?,
                        Message::Close(Some(frame)) if u16::from(frame.code) == 1000 => {
                            let _ = socket.flush().await;
                            transport.finish()?;
                            return Ok(());
                        }
                        Message::Ping(_) | Message::Pong(_) => {},
                        _ => return Err("unexpected WebSocket message".into()),
                    }
                }
            }
        }.await;
        if let Err(error) = result {
            transport.fail(error.to_string());
        }
    });
    Ok((source, input, handle))
}
async fn receive<T: IqSample + PartialEq>(
    address: String,
    expected: Vec<T>,
    resource: &str,
    observe: bool,
    websocket: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut options = StreamOptions::default();
    if observe {
        options.loss_policy = rustradio::iq_stream::proto::LossPolicy::AllowGaps;
    }
    let (source, input, status) = if websocket {
        connect_websocket::<T>(&address, resource, &options).await?
    } else {
        IqStreamSource::<T>::connect_async(format!("http://{address}"), resource, options).await?
    };
    if observe {
        println!("connected");
        io::stdout().flush()?;
    }
    assert_eq!(status.description().sample_rate_hz, 48000.0);
    let sink = VectorSink::new(input, 2000);
    let collected = sink.hook();
    let mut graph = Graph::new();
    graph.add(Box::new(source));
    graph.add(Box::new(sink));
    tokio::task::spawn_blocking(move || graph.run()).await??;
    assert_eq!(status.status(), SourceStatus::Complete);
    assert_eq!(collected.data().samples(), expected);
    for expected_tag in tags() {
        let data = collected.data();
        let found: Vec<_> = data
            .tags()
            .iter()
            .filter(|t| t.key() == expected_tag.key())
            .collect();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].pos(), expected_tag.pos());
        assert_eq!(found[0].val(), expected_tag.val());
    }
    Ok(())
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    let real: Vec<Float> = (0..1000).map(|i| i as f32 / 32.0).collect();
    let complex: Vec<Complex> = (0..1000)
        .map(|i| Complex::new(i as f32 / 32.0, -(i as f32) / 64.0))
        .collect();
    match (args[1].as_str(), args[2].as_str()) {
        ("serve", "real") => serve(real).await?,
        ("serve", "complex") => serve(complex).await?,
        ("receive", "real") => receive(args[3].clone(), real, "iq", false, false).await?,
        ("receive", "complex") => receive(args[3].clone(), complex, "iq", false, false).await?,
        ("observe", "real") => receive(args[3].clone(), real, &args[4], true, false).await?,
        ("observe", "complex") => receive(args[3].clone(), complex, &args[4], true, false).await?,
        ("observe-ws", "real") => receive(args[3].clone(), real, &args[4], true, true).await?,
        ("observe-ws", "complex") => {
            receive(args[3].clone(), complex, &args[4], true, true).await?
        }
        _ => panic!("expected serve|receive|observe real|complex [address] [resource]"),
    }
    Ok(())
}
