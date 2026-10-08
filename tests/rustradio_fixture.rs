// SPDX-License-Identifier: MIT
//! Independent interop fixture. It depends on upstream APIs without patching them.
use rustradio::blocks::{IqStreamSink, IqStreamSource, VectorSink, VectorSource};
use rustradio::graph::{Graph, GraphRunner};
use rustradio::iq_stream::{IqSample, IqServer, SourceStatus, StreamOptions};
use rustradio::stream::{Tag, TagValue};
use rustradio::{Complex, Float};
use std::io::{self, Write};

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
async fn receive<T: IqSample + PartialEq>(
    address: String,
    expected: Vec<T>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (source, input, status) =
        IqStreamSource::<T>::connect(format!("http://{address}"), "iq", StreamOptions::default())
            .await?;
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
        ("receive", "real") => receive(args[3].clone(), real).await?,
        ("receive", "complex") => receive(args[3].clone(), complex).await?,
        _ => panic!("expected serve|receive real|complex [address]"),
    }
    Ok(())
}
