use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

use anyhow::Result;
use axum::{
    error_handling::HandleErrorLayer,
    extract::{self, State},
    http::StatusCode,
    routing::{get, post},
    BoxError, Json, Router,
};
use futures::{SinkExt, StreamExt};
use log::{debug, error, info, warn};
use tokio::{
    io::split,
    net::{TcpListener, TcpStream},
    sync::{
        mpsc::{self, Sender},
        oneshot, RwLock,
    },
    time::{sleep, timeout},
};
use tokio_util::codec::{FramedRead, FramedWrite};
use tower::{timeout::TimeoutLayer, ServiceBuilder};
use tower_http::cors::{Any, CorsLayer};

use crate::{
    cache::RedisClient,
    codec::{DMessage, DMessageCodec, DRequest, DResponse},
    DResult, GetResultReq,
};

const EXPIRATION: usize = 5 * 60;

#[derive(Debug, Clone)]
pub struct ServerWorker {
    pub router: Sender<DRequest>,
    // 1: Idle; 2: Busy
    pub status: u8,
}

impl ServerWorker {
    pub fn new(router: Sender<DRequest>) -> Self {
        Self { router, status: 1 }
    }
}

#[derive(Debug, Clone)]
pub struct Scheduler {
    pub workers: Arc<RwLock<HashMap<String, ServerWorker>>>,
    pub queue: Arc<RwLock<Vec<DRequest>>>,
    pub db: Arc<RwLock<RedisClient>>,
    pub seen: Arc<RwLock<Vec<String>>>,
}

impl Scheduler {
    pub fn new(redis: &str) -> Arc<Self> {
        Arc::new(Self {
            workers: Arc::new(RwLock::new(HashMap::new())),
            queue: Arc::new(RwLock::new(vec![])),
            db: Arc::new(RwLock::new(RedisClient::connect(redis).unwrap())),
            seen: Arc::new(RwLock::new(vec![])),
        })
    }

    pub async fn handle_stream(stream: TcpStream, server: Arc<Self>) -> Result<()> {
        let peer = stream.peer_addr().unwrap();
        let (tx, mut rx) = mpsc::channel::<DRequest>(1024);
        let mut worker_name = stream.peer_addr().unwrap().clone().to_string();
        let (r, w) = split(stream);
        let mut outbound_w = FramedWrite::new(w, DMessageCodec::default());
        let mut outbound_r = FramedRead::new(r, DMessageCodec::default());
        let (router, handler) = oneshot::channel();
        let mut timer = tokio::time::interval(Duration::from_secs(300));
        let _ = timer.tick().await;

        let worker_server = server.clone();
        tokio::spawn(async move {
            let _ = router.send(());
            loop {
                tokio::select! {
                    _ = timer.tick() => {
                        debug!("no message from worker {} for 5 mins, exit", peer);
                        let _ = worker_server.workers.write().await.remove(&worker_name).unwrap();
                        break;
                    },
                    Some(request) = rx.recv() => {
                        debug!("new message from rx for worker {}", worker_name);
                        let _ = worker_server.workers.write().await.get_mut(&worker_name).unwrap().status = 2;
                        let send_future = outbound_w.send(DMessage::DRequest(request));
                        if let Err(error) = timeout(Duration::from_millis(200), send_future).await {
                            debug!("send message to worker timeout: {}", error);
                        }

                    },
                    result = outbound_r.next() => {
                        debug!("new message from outboud_reader {:?} of worker {}", result, worker_name);
                        match result {
                            Some(Ok(message)) => {
                                timer.reset();
                                match message {
                                    DMessage::RegisterWorker(register) => {
                                        debug!("heart beat info {:?}", register);
                                        match worker_name == register.name {
                                            true => {},
                                            false => {
                                                let worker = ServerWorker::new(tx.clone());
                                                worker_name = register.name;
                                                info!("new worker: {}", worker_name.clone());
                                                let _ = worker_server.workers.write().await.insert(worker_name.clone(), worker);
                                            }
                                        }
                                    },
                                    DMessage::DRequest(_) => log::error!("invalid message from worker, should never happen"),
                                    DMessage::DResponse(response) => {
                                        let task_id = response.id.clone();
                                        info!("task {} response from worker {}", task_id, worker_name);
                                        let _ = worker_server.db.write().await.set_str(&task_id, &serde_json::to_string(&response).unwrap(), EXPIRATION);
                                        match worker_server.queue.write().await.pop() {
                                            Some(task) => {
                                                let _ = tx.clone().send(task).await.unwrap();
                                            },
                                            None => worker_server.workers.write().await.get_mut(&worker_name).unwrap().status = 1,
                                        }
                                    },
                                }
                            },
                            _ => {
                                warn!("unknown message");
                                let _ = worker_server.workers.write().await.remove(&worker_name).unwrap();
                                break;
                            },
                        }
                    }
                }
            }
            error!("worker {} main loop exit", worker_name);
        });
        let _ = handler.await;
        Ok(())
    }
}

pub async fn start_rest(shared: Arc<Scheduler>, rest: SocketAddr) -> Result<()> {
    let router = Router::new()
        .route("/health", get(health_handler))
        .route("/submitTask", post(submit_task_handler))
        .route("/getResult", post(task_result_handler))
        .with_state(shared)
        .layer(
            ServiceBuilder::new()
                .layer(HandleErrorLayer::new(|_: BoxError| async {
                    StatusCode::REQUEST_TIMEOUT
                }))
                .layer(TimeoutLayer::new(Duration::from_secs(60))),
        )
        .layer(
            CorsLayer::new()
                .allow_methods(Any)
                .allow_origin(Any)
                .allow_headers(Any),
        );
    let listener = TcpListener::bind(&rest).await?;
    axum::serve(listener, router).await?;
    Ok(())
}

pub async fn health_handler() -> Json<bool> {
    Json(true)
}

pub async fn submit_task_handler(
    State(shared): State<Arc<Scheduler>>,
    extract::Json(request): extract::Json<DRequest>,
) {
    let task_id = request.clone().id;
    if shared.seen.read().await.contains(&task_id) {
        warn!("duplicated task, ignore");
        return;
    }
    info!("new task submitted {}", task_id);
    shared.seen.write().await.push(task_id.clone());
    for (_k, v) in shared.workers.read().await.iter() {
        if v.status == 1 {
            if let Err(e) = v.router.send(request).await {
                log::error!("failed to send task to manager {}", e);
            }
            return;
        }
    }
    let _ = shared.queue.write().await.push(request);
    info!("new task added to queue {}", task_id);
}

pub async fn task_result_handler(
    State(shared): State<Arc<Scheduler>>,
    extract::Json(request): extract::Json<GetResultReq>,
) -> Json<DResult> {
    let task_id = request.id;
    let completed_result = shared.db.read().await.get_str(&task_id);
    if !shared.seen.read().await.contains(&task_id) {
        warn!("task not submitted yet");
        return Json(DResult::missed());
    }
    match completed_result {
        Ok(data) => {
            let data: DResponse = serde_json::from_str(&data).unwrap();
            shared.seen.write().await.retain(|x| *x == task_id);
            Json(DResult::completed(data))
        }
        Err(_) => Json(DResult::running()),
    }
}

pub async fn start_server(tcp: SocketAddr, rest: SocketAddr, redis: &str) -> Result<()> {
    let server = Scheduler::new(redis);
    let listener = TcpListener::bind(&tcp).await.unwrap();
    let (router, handler) = oneshot::channel();
    let server_clone = server.clone();
    let proxy_handler = tokio::spawn(async move {
        let _ = router.send(());
        loop {
            match listener.accept().await {
                Ok((stream, ip)) => {
                    info!("new connection from {}", ip);
                    if let Err(e) = Scheduler::handle_stream(stream, server_clone.clone()).await {
                        log::error!("failed to handle stream, {e}");
                    }
                }
                Err(e) => log::error!("failed to accept connection, {:?}", e),
            }
        }
    });
    let _ = handler.await;

    let server_status = server.clone();
    let (router, handler) = oneshot::channel();
    let status_update_handler = tokio::spawn(async move {
        let _ = router.send(());
        loop {
            let workers = server_status.workers.read().await;
            let workers: Vec<&String> = workers.keys().collect();
            let pending_taskes = server_status.queue.read().await.len();
            info!("online workers: {}, {:?}", workers.len(), workers);
            info!("pending taskes in queue: {}", pending_taskes);
            sleep(Duration::from_secs(10)).await;
        }
    });
    let _ = handler.await;

    let rest_handler = tokio::spawn(async move {
        let _ = start_rest(server.clone(), rest).await;
    });
    let _ = tokio::join!(proxy_handler, rest_handler, status_update_handler);
    std::future::pending::<()>().await;
    Ok(())
}
