use std::{net::SocketAddr, sync::Arc, time::Duration};

use anyhow::Result;
use futures::{SinkExt, StreamExt};
use ironfish_rust::{IncomingViewKey, MerkleNote, OutgoingViewKey, ViewKey};
use log::{debug, error, info};
use rayon::prelude::*;
use rayon::{iter::IntoParallelIterator, ThreadPool};
use tokio::{
    io::split,
    net::TcpStream,
    sync::{mpsc, oneshot, Mutex},
    time::sleep,
};
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::codec::{DMessage, DMessageCodec, DRequest, DResponse, RegisterWorker, SingleResponse};

#[derive(Debug, Clone)]
pub struct WorkerPool {
    pub pool: Arc<Mutex<ThreadPool>>,
}

impl WorkerPool {
    pub async fn decryption(&self, request: DRequest) -> DResponse {
        let pool = self.pool.lock().await;
        let decrypted_notes = pool.install(move || {
            let decrypted: Vec<Option<SingleResponse>> = request
                .data
                .into_par_iter()
                .map(|data| {
                    let serialized_note = data.serialized_note.clone();
                    let raw = hex::decode(data.serialized_note);
                    match raw {
                        Ok(raw) => {
                            let note_enc = MerkleNote::read(&raw[..]);
                            let in_vk = IncomingViewKey::from_hex(&data.incoming_view_key);
                            let vk = ViewKey::from_hex(&data.view_key);
                            let out_vk = OutgoingViewKey::from_hex(&data.outgoing_view_key);
                            if note_enc.is_ok() && in_vk.is_ok() && vk.is_ok() && out_vk.is_ok() {
                                let note_enc = note_enc.unwrap();
                                let in_vk = in_vk.unwrap();
                                let out_vk = out_vk.unwrap();
                                let vk = vk.unwrap();
                                if let Ok(received_note) = note_enc.decrypt_note_for_owner(&in_vk) {
                                    if received_note.value() != 0 {
                                        let mut vec: Vec<u8> = Vec::with_capacity(32);
                                        if note_enc.merkle_hash().write(&mut vec).is_ok() {
                                            let hash = hex::encode(vec);
                                            let nullifier = match data.current_note_index {
                                                Some(current_indx) => Some(hex::encode(
                                                    received_note
                                                        .nullifier(&vk, current_indx)
                                                        .to_vec(),
                                                )),
                                                None => None,
                                            };
                                            let res = SingleResponse {
                                                index: data.current_note_index,
                                                for_spender: false,
                                                hash,
                                                nullifier,
                                                serialized_note,
                                            };
                                            return Some(res);
                                        }
                                    }
                                }

                                if data.decrypt_for_spender {
                                    if let Ok(spend_note) =
                                        note_enc.decrypt_note_for_spender(&out_vk)
                                    {
                                        if spend_note.value() != 0 {
                                            let mut vec: Vec<u8> = Vec::with_capacity(32);
                                            if note_enc.merkle_hash().write(&mut vec).is_ok() {
                                                let hash = hex::encode(vec);
                                                let res = SingleResponse {
                                                    index: data.current_note_index,
                                                    for_spender: true,
                                                    hash,
                                                    nullifier: None,
                                                    serialized_note,
                                                };
                                                return Some(res);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Err(_) => {}
                    }
                    return None;
                })
                .collect();
            decrypted.into_iter().flatten().collect()
        });
        DResponse {
            id: request.id,
            data: decrypted_notes,
        }
    }
}

pub async fn handle_connection(
    worker_pool: Arc<WorkerPool>,
    stream: TcpStream,
    name: Option<String>,
) -> Result<()> {
    info!("connected to scheduler");
    let (r, w) = split(stream);
    let mut socket_w_handler = FramedWrite::new(w, DMessageCodec::default());
    let mut socket_r_handler = FramedRead::new(r, DMessageCodec::default());
    let (tx, mut rx) = mpsc::channel::<DMessage>(1024);

    let worker_name = name.unwrap_or(format!("{:?}", gethostname::gethostname()));

    // send to scheduler loop
    let (router, handler) = oneshot::channel();
    let send_task_handler = tokio::spawn(async move {
        let _ = router.send(());
        while let Some(message) = rx.recv().await {
            debug!("write message to scheduler {:?}", message);
            match message {
                DMessage::DResponse(response) => {
                    if let Err(e) = socket_w_handler.send(DMessage::DResponse(response)).await {
                        error!("failed to send DResponse message, {:?}", e);
                        return;
                    }
                }
                DMessage::RegisterWorker(register) => {
                    if let Err(e) = socket_w_handler
                        .send(DMessage::RegisterWorker(register))
                        .await
                    {
                        error!("failed to send RegisterWorker message, {:?}", e);
                        return;
                    }
                }
                _ => error!("invalid message to send"),
            }
        }
    });
    let _ = handler.await;

    // receive task handler loop
    let task_tx = tx.clone();
    let (router, handler) = oneshot::channel();
    let receive_task_handler = tokio::spawn(async move {
        let _ = router.send(());
        while let Some(Ok(message)) = socket_r_handler.next().await {
            match message {
                DMessage::DRequest(request) => {
                    info!("new task from scheduler: {}", request.id.clone());
                    let response = worker_pool.decryption(request).await;
                    if let Err(e) = task_tx.send(DMessage::DResponse(response)).await {
                        error!("failed to send response to write channel, {}", e);
                    }
                }
                _ => {
                    error!("invalid message");
                    break;
                }
            }
        }
    });
    let _ = handler.await;

    let heart_beat_tx = tx.clone();
    let (router, handler) = oneshot::channel();
    let heart_beat_handler = tokio::spawn(async move {
        let _ = router.send(());
        loop {
            let _ = heart_beat_tx
                .send(DMessage::RegisterWorker(RegisterWorker {
                    name: worker_name.clone(),
                }))
                .await
                .unwrap();
            sleep(Duration::from_secs(30)).await;
        }
    });
    let _ = handler.await;
    let _ = tokio::join!(send_task_handler, receive_task_handler, heart_beat_handler);
    Ok(())
}

pub async fn start_worker(addr: SocketAddr, name: Option<String>) -> Result<()> {
    let thread_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(num_cpus::get())
        .build()
        .unwrap();
    let worker_pool = WorkerPool {
        pool: Arc::new(Mutex::new(thread_pool)),
    };
    let worker = Arc::new(worker_pool);
    let (router, handler) = oneshot::channel();
    tokio::spawn(async move {
        let _ = router.send(());
        loop {
            match TcpStream::connect(&addr).await {
                Ok(stream) => {
                    if let Err(e) = handle_connection(worker.clone(), stream, name.clone()).await {
                        error!("connection to scheduler interrupted: {:?}", e);
                    }
                    error!("handle_connection exited");
                    sleep(Duration::from_secs(10)).await;
                }
                Err(e) => {
                    error!("failed to connect to scheduler, try again, {:?}", e);
                    sleep(Duration::from_secs(10)).await;
                }
            }
        }
    });
    let _ = handler.await;
    std::future::pending::<()>().await;
    Ok(())
}
