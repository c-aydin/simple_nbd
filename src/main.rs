use std::io;
use std::net::SocketAddrV6;

use simple_nbd::backend::{MemoryBackend, NbdBackend};
use simple_nbd::constants::*;
use simple_nbd::export::{MpscExport, NbdExportRegistry, NbdExportRegistryExt, NbdExportSpec};
use simple_nbd::server;

#[tokio::main]
async fn main() -> io::Result<()> {
    tracing_subscriber::fmt::init();

    let registry = NbdExportRegistry::new_locked();
    prepare_exports(registry.clone()).await;

    let addr = SocketAddrV6::new(std::net::Ipv6Addr::UNSPECIFIED, 10809, 0, 0);
    server::listen(addr, registry).await
}

async fn prepare_exports(registry: NbdExportRegistry) {
    let export = MpscExport::new(
        NbdExportSpec {
            name: "example_export".to_string(),
            description: "An example NBD export".to_string(),
            size: 64 * 1024 * 1024, // 64 MiB
            block_size: 4096,
            flags: NBD_FLAG_HAS_FLAGS
                | NBD_FLAG_SEND_FLUSH
                | NBD_FLAG_SEND_FUA
                | NBD_FLAG_SEND_TRIM,
        },
        128,
    );
    let mut backend = MemoryBackend::new(export.spec.size as usize);
    registry.add_export(export.clone()).await;

    tokio::spawn(async move {
        let mut req_rx = export.request_receiver.lock().await;
        let rep_tx = export.reply_sender.clone();
        loop {
            match req_rx.recv().await {
                Some(req) => {
                    tracing::trace!(
                        "Received operation: handle={}, type={:?}, offset={}, length={}",
                        req.cookie,
                        req.request_type,
                        req.offset,
                        req.length
                    );
                    let reply = backend.handle_request(&req).await;
                    rep_tx.send(reply).await.unwrap();
                }
                None => {
                    tracing::info!("Request channel closed");
                    break;
                }
            };
        }
    });

    let export = MpscExport::new(
        NbdExportSpec {
            name: "example_export2".to_string(),
            description: "Another example NBD export".to_string(),
            size: 64 * 1024 * 1024, // 64 MiB
            block_size: 4096,
            flags: NBD_FLAG_HAS_FLAGS
                | NBD_FLAG_SEND_FLUSH
                | NBD_FLAG_SEND_FUA
                | NBD_FLAG_SEND_TRIM,
        },
        128,
    );
    let mut backend = MemoryBackend::new(export.spec.size as usize);
    registry.add_export(export.clone()).await;

    tokio::spawn(async move {
        let mut req_rx = export.request_receiver.lock().await;
        let rep_tx = export.reply_sender.clone();
        loop {
            match req_rx.recv().await {
                Some(req) => {
                    tracing::trace!(
                        "Received operation: cookie={}, type={:?}, offset={}, length={}",
                        req.cookie,
                        req.request_type,
                        req.offset,
                        req.length
                    );
                    let reply = backend.handle_request(&req).await;
                    rep_tx.send(reply).await.unwrap();
                }
                None => {
                    tracing::info!("Request channel closed");
                    break;
                }
            };
        }
    });
}
