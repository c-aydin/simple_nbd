use std::collections::VecDeque;
use std::io::{self, Error};
use std::net::SocketAddrV6;

use bytes::BytesMut;
use bytes::{Buf, Bytes};
use futures::sink::SinkExt;
use tokio::net::{TcpListener, TcpStream};
use tokio_stream::StreamExt;
use tokio_util::codec::Framed;
use tracing::{debug, error, info, instrument, trace, warn};

use super::codec::*;
use super::constants::*;
use super::export::{NbdExportRegistry, NbdExportRegistryExt, NbdReply, NbdRequest, RequestType};

#[instrument(skip(addr, registry))]
pub async fn listen(addr: SocketAddrV6, registry: NbdExportRegistry) -> Result<(), Error> {
    info!(address = %addr, "Starting NBD server");

    let listener = TcpListener::bind(addr).await?;
    info!(address = %addr, "NBD server listening");

    loop {
        // Accept a new connection
        let (stream, socket_addr) = listener.accept().await?;

        info!(peer_addr = %socket_addr, "Accepted new connection");

        // Spawn a new task to handle the connection
        let registry = registry.clone();
        tokio::spawn(async move {
            match nbd_session(stream, registry).await {
                Err(e) => error!(error = %e, "Session terminated with error"),
                Ok(_) => info!("Session ended successfully"),
            }
        });
    }
}

/// Handle a single NBD client connection
pub async fn nbd_session(stream: TcpStream, registry: NbdExportRegistry) -> io::Result<()> {
    // Framed doesn't share encoder/decoder so use shared state to ensure coherence
    let (client_rx, client_tx) = tokio::io::split(stream);
    let decoder = NbdCodec::new_server();
    let mut encoder = NbdCodec::new_server();
    encoder.set_shared_state(decoder.shareable_state());
    let mut client_receiver = Framed::new(client_rx, decoder);
    let mut client_sender = Framed::new(client_tx, encoder);

    // --- 1. Handshake Phase ---
    // The codec handles the first part (ExpectClientFlags)
    // We start by sending our greeting
    debug!("Sending ServerGreeting");
    client_sender
        .send(NbdMessage::ServerGreeting {
            flags: NBD_FLAG_FIXED_NEWSTYLE | NBD_FLAG_NO_ZEROES,
        })
        .await?;

    // Now, we wait for the client's flags
    let client_flags_msg = match client_receiver.next().await {
        Some(Ok(msg)) => msg,
        Some(Err(e)) => return Err(e),
        None => {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "Client disconnected during handshake",
            ));
        }
    };

    if let NbdMessage::ClientFlags { flags } = client_flags_msg {
        if flags & NBD_FLAG_C_FIXED_NEWSTYLE == 0 {
            warn!(?flags, "Client does not support fixed newstyle. Closing.");
            return Ok(());
        }
        debug!("Client supports fixed newstyle");
        if flags & NBD_FLAG_C_NO_ZEROES == 0 {
            warn!(
                ?flags,
                "Client did not set NO_ZEROES flag, this could get weird."
            );
        }
    } else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Expected ClientFlags",
        ));
    }

    let mut export_name = String::new();

    // --- 2. Option Haggling Loop ---
    while client_receiver.codec().state() == SessionState::Haggling
        && let Some(result) = client_receiver.next().await
    {
        match result? {
            NbdMessage::OptionClanger { option_id, data } => {
                debug!(?option_id, ?data, "Handling OptionClanger");
                match option_id {
                    NBD_OPT_ABORT => {
                        info!("Client requested abort");
                        client_sender
                            .send(NbdMessage::SimpleReply {
                                option_id: NBD_OPT_LIST,
                                reply_type: NBD_REP_ACK,
                                data: Bytes::new(),
                            })
                            .await?;
                    }
                    NBD_OPT_LIST => {
                        debug!("Client requested export list");
                        let exports = registry.list_exports().await;
                        info!(
                            export_count = exports.len(),
                            "Sending export list to client"
                        );
                        for (name, spec) in exports.iter() {
                            let mut response_data = BytesMut::new();
                            response_data.extend_from_slice(&(name.len() as u32).to_be_bytes());
                            response_data.extend_from_slice(name.as_bytes());
                            response_data.extend_from_slice(spec.description.as_bytes());
                            client_sender
                                .send(NbdMessage::SimpleReply {
                                    option_id: NBD_OPT_LIST,
                                    reply_type: NBD_REP_SERVER,
                                    data: Bytes::from(response_data),
                                })
                                .await?;
                        }
                        client_sender
                            .send(NbdMessage::SimpleReply {
                                option_id: NBD_OPT_LIST,
                                reply_type: NBD_REP_ACK,
                                data: Bytes::new(),
                            })
                            .await?;
                    }
                    NBD_OPT_EXPORT_NAME => {
                        debug!("Client requested export name");
                        export_name = String::from_utf8_lossy(&data).to_string();
                        let export = registry.get_export(&export_name).await;
                        let export = match export {
                            Some(exp) => exp,
                            None => {
                                warn!(?export_name, "Client requested unknown export name");
                                // Send error reply
                                client_sender
                                    .send(NbdMessage::SimpleReply {
                                        option_id: NBD_OPT_GO,
                                        reply_type: NBD_REP_ERR_UNKNOWN,
                                        data: Bytes::new(),
                                    })
                                    .await?;
                                continue;
                            }
                        };
                        tracing::info!(?export_name, "Client export OK");
                        // All good
                        // client_sender
                        //     .send(NbdMessage::SimpleReply {
                        //         option_id: NBD_OPT_EXPORT_NAME,
                        //         reply_type: NBD_REP_ACK,
                        //         data: Bytes::new(),
                        //     })
                        //     .await?;

                        client_sender
                            .send(NbdMessage::LegacyExportInfo {
                                size: export.spec.size,
                                flags: export.spec.flags,
                            })
                            .await?;
                    }
                    NBD_OPT_INFO => {
                        debug!("Client requested export info");
                    }
                    NBD_OPT_GO => {
                        debug!("Client sent NBD_OPT_GO, ending negotiation");

                        let mut reader = &data[..];
                        let name_len = reader.get_u32();
                        if name_len == 0 {
                            export_name = "default".to_string();
                        } else {
                            let mut name_buf = vec![0u8; name_len as usize];
                            reader.copy_to_slice(&mut name_buf);
                            export_name = String::from_utf8_lossy(&name_buf).to_string();
                        }
                        let export = registry.get_export(&export_name).await;
                        let export = match export {
                            Some(exp) => exp,
                            None => {
                                warn!(?export_name, "Client requested unknown export name");
                                // Send error reply
                                client_sender
                                    .send(NbdMessage::SimpleReply {
                                        option_id: NBD_OPT_GO,
                                        reply_type: NBD_REP_ERR_UNKNOWN,
                                        data: Bytes::new(),
                                    })
                                    .await?;
                                continue;
                            }
                        };

                        tracing::debug!(?export_name, "Client export determined");

                        let request_count = reader.get_u16();
                        tracing::debug!(?export_name, ?request_count, "Client GO request details");
                        let mut requests = Vec::<u16>::new();
                        for _ in 0..request_count {
                            let info_type = reader.get_u16();
                            requests.push(info_type);
                        }
                        tracing::debug!(?requests, "Client GO request info types");
                        let mut sent_info = false;
                        for req in requests {
                            match req {
                                NBD_INFO_EXPORT => {
                                    client_sender
                                        .send(NbdMessage::ExportInfo {
                                            option_id: NBD_OPT_GO,
                                            request_type: NBD_INFO_EXPORT,
                                            size: export.spec.size,
                                            flags: export.spec.flags,
                                        })
                                        .await?;
                                    sent_info = true;
                                }
                                // I guess in theory we're supposed to answer the rest
                                // but even the actual nbd-server.c doesn't ...
                                _ => {
                                    warn!(?req, "Client requested unsupported info type");
                                }
                            }
                        }

                        if !sent_info {
                            // Send export info by default
                            client_sender
                                .send(NbdMessage::ExportInfo {
                                    option_id: NBD_OPT_GO,
                                    request_type: NBD_INFO_EXPORT,
                                    size: export.spec.size,
                                    flags: export.spec.flags,
                                })
                                .await?;
                        }

                        // Acknowledge GO
                        client_sender
                            .send(NbdMessage::SimpleReply {
                                option_id: NBD_OPT_GO,
                                reply_type: NBD_REP_ACK,
                                data: Bytes::new(),
                            })
                            .await?;
                    }
                    _ => {
                        // TODO: Send NBD_REP_ERR_UNSUP?
                        warn!(?option_id, "Unsupported option received");
                    }
                }
            }
            // The codec shouldn't produce these messages on the server side
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Unexpected message during haggling",
                ));
            }
        }
    }

    if client_receiver.codec().state() != SessionState::Transmission {
        warn!("Client disconnected before entering Transmission phase");
        return Ok(());
    }

    let export = match registry.get_export(&export_name).await {
        Some(exp) => exp,
        None => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "No export selected for transmission",
            ));
        }
    };
    let backend_sender = export.request_sender.clone();
    let reply_receiver_lock = export.reply_receiver.clone();
    let mut backend_receiver = match reply_receiver_lock.try_lock() {
        Ok(receiver) => receiver,
        Err(_) => {
            return Err(io::Error::other(
                "Failed to lock export reply receiver, another session may be active?",
            ));
        }
    };

    // --- 3. Transmission Phase ---
    info!("Entering Transmission Phase");
    let mut to_backend = VecDeque::new();
    let mut to_client = VecDeque::new();
    loop {
        tokio::select! {
            // handle messages from client
            Some(result) = client_receiver.next() => {
                match result {
                    Ok(m) => {to_backend.push_back(m);},
                    Err(e) => {
                        warn!(error = %e, "Error reading message from client, terminating session");
                        return Err(e);
                    }
                };
            }

            // handle replies from backend
            result = backend_receiver.recv() => {
                match result {
                    Some(r) => {
                        trace!("Received reply from backend: {}", r.cookie);
                        to_client.push_back(r);
                    },
                    None => {
                        info!("Backend reply channel closed, terminating session");
                        // theoretically do this gracefully?
                        break;
                    }
                };
            }

        }
        while let Some(msg) = to_backend.pop_front() {
            let req = match message_to_request(&msg) {
                Ok(r) => r,
                Err(ref e) if e.kind() == io::ErrorKind::ConnectionAborted => {
                    info!("Client requested disconnect");
                    break;
                }
                Err(e) => {
                    warn!(error = %e, "Failed to convert message to request, skipping");
                    continue;
                }
            };
            trace!("Sending request to backend: {}", req.cookie);
            backend_sender.send(req).await.unwrap();
        }
        while let Some(reply) = to_client.pop_front() {
            trace!("Sending reply to client: {}", reply.cookie);
            let reply_msg = reply_to_message(&reply)?;
            client_sender.send(reply_msg).await.unwrap();
        }
    }

    info!("Client disconnected");
    Ok(())
}

fn message_to_request(msg: &NbdMessage) -> io::Result<NbdRequest> {
    match msg {
        NbdMessage::CmdRead {
            cookie,
            offset,
            length,
        } => Ok(NbdRequest {
            cookie: *cookie,
            request_type: RequestType::Read,
            offset: *offset,
            length: *length,
            data: None,
        }),
        NbdMessage::CmdWrite {
            cookie,
            offset,
            data,
        } => Ok(NbdRequest {
            cookie: *cookie,
            request_type: RequestType::Write,
            offset: *offset,
            length: data.len() as u32,
            data: Some(data.clone()),
        }),
        NbdMessage::CmdFlush { cookie } => Ok(NbdRequest {
            cookie: *cookie,
            request_type: RequestType::Flush,
            offset: 0,
            length: 0,
            data: None,
        }),
        NbdMessage::CmdTrim {
            cookie,
            offset,
            length,
        } => Ok(NbdRequest {
            cookie: *cookie,
            request_type: RequestType::Trim,
            offset: *offset,
            length: *length,
            data: None,
        }),
        NbdMessage::CmdDisc { cookie: _ } => Err(io::Error::new(
            io::ErrorKind::ConnectionAborted,
            "Client requested disconnect",
        )),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Cannot convert message to request",
        )),
    }
}

fn reply_to_message(reply: &NbdReply) -> io::Result<NbdMessage> {
    Ok(NbdMessage::CmdReply {
        error: reply.error_code,
        cookie: reply.cookie,
        data: reply.data.clone(),
    })
}
