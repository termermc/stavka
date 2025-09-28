mod cachestate;
mod hash;
mod origin;

use std::cell::RefCell;
use std::io;
use std::io::ErrorKind::NotFound;
use std::mem::size_of_val;
use std::num::NonZeroUsize;
use std::os::fd::AsRawFd;
use std::rc::Rc;
use bytes::Bytes;
use http::{response::Builder, StatusCode};
use monoio::{io::{
    sink::{Sink, SinkExt},
    stream::Stream,
    Splitable,
}, net::{TcpListener, TcpStream}, IoUringDriver};
use monoio::utils::bind_to_cpu_set;
use monoio_http::{
    common::{error::HttpError, request::Request, response::Response},
    h1::{
        codec::{decoder::RequestDecoder, encoder::GenericEncoder},
        payload::{FixedPayload, Payload},
    },
    util::spsc::{spsc_pair, SPSCReceiver},
};
use monoio_http::common::body::{Body, HttpBody};
use monoio_http_client::Client;

async fn thread_main() -> Result<(), io::Error> {
    let http_client = Rc::new(Client::default());
    let mut origin_manager = Rc::new(RefCell::new(origin::OriginManager::new()));

    origin_manager.borrow_mut().set_origin_host("stavka.localhost".to_owned(), "1.1.1.1".to_owned());

    let bind_addr = "0.0.0.0:50002";
    let listener = TcpListener::bind(bind_addr).expect(&*("failed to listen on port addr ".to_owned() + bind_addr));

    unsafe {
        let optval: libc::c_int = 1;
        let ret = libc::setsockopt(
            listener.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_REUSEPORT,
            &optval as *const _ as *const libc::c_void,
            size_of_val(&optval) as libc::socklen_t,
        );
        if ret != 0 {
            return Err(io::Error::last_os_error());
        }
    }

    println!("Listening");
    loop {
        let incoming = listener.accept().await;
        match incoming {
            Ok((stream, addr)) => {
                //println!("accepted a connection from {}", addr);
                monoio::spawn(handle_connection(
                    stream,
                    http_client.clone(),
                    origin_manager.clone(),
                ));
            }
            Err(e) => {
                println!("accepted connection failed: {}", e);
            }
        }
    };
}

fn thread_launcher(core: usize) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        bind_to_cpu_set([core]).expect("failed to set thread CPU bind");

        monoio::RuntimeBuilder::<IoUringDriver>::new()
            .enable_timer()
            .build()
            .expect("failed to build runtime")
            .block_on(thread_main()).expect("failed to execute runtime function");
    })
}

#[monoio::main(timer_enabled = true, worker_threads = 12)]
async fn main() {
    thread_main().await.expect("main failed")
}

async fn handle_connection(
    stream: TcpStream,
    http_client: Rc<Client>,
    origin_manager: Rc<RefCell<origin::OriginManager>>,
) {
    let (r, w) = stream.into_split();
    let sender = GenericEncoder::new(w);
    let mut receiver = RequestDecoder::new(r);
    let (mut tx, rx) = spsc_pair();
    monoio::spawn(handle_task(
        rx,
        sender,
        http_client,
        origin_manager,
    ));

    loop {
        match receiver.next().await {
            None => {
                //println!("connection closed, connection handler exit");
                return;
            }
            Some(Err(_)) => {
                println!("receive request failed, connection handler exit");
                return;
            }
            Some(Ok(item)) => match tx.send(item).await {
                Err(_) => {
                    println!("request handler dropped, connection handler exit");
                    return;
                }
                Ok(_) => {
                    //println!("request handled success");
                }
            },
        }
    }
}

async fn handle_task(
    mut receiver: SPSCReceiver<Request>,
    mut sender: impl Sink<Response<HttpBody>, Error = impl Into<HttpError>>,
    http_client: Rc<Client>,
    origin_manager: Rc<RefCell<origin::OriginManager>>,
) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        let request = match receiver.recv().await {
            Some(r) => r,
            None => {
                return Ok(());
            }
        };
        let resp = handle_request(
            request,
            http_client.clone(),
            origin_manager.clone(),
        ).await?;
        sender.send_and_flush(resp).await.map_err(|e| e.into())?;
    }
}

async fn handle_request(
    req: Request,
    http_client: Rc<Client>,
    origin_manager: Rc<RefCell<origin::OriginManager>>,
) -> Result<Response<HttpBody>, Box<dyn std::error::Error>> {
    let uri = req.uri();

    fn not_found() -> Response<HttpBody> {
        Builder::new().
            status(StatusCode::NOT_FOUND).
            body(
                HttpBody::from(
                    Payload::Fixed(FixedPayload::new(Bytes::from_static(b"hello world"))),
                ),
            ).unwrap()
    }

    let host = req.headers().get(http::header::HOST);
    if host == None {
        return Ok(not_found())
    }
    let host = host.unwrap().to_str();
    if host.is_err() {
        return Ok(not_found())
    }
    let mut host = host.unwrap();
    let colon_idx = host.rfind(':');
    match colon_idx {
        Some(idx) => {
            host = &host[..idx];
        }
        None => {}
    }

    let origin_manager = origin_manager.borrow();
    let origin = origin_manager.uri_to_origin_uri(uri.clone(), host);
    if origin.is_none() {
        return Ok(not_found())
    }
    let origin = origin.unwrap();

    // Make origin HTTP request.
    let mut origin_req = Request::builder().
        method(req.method()).
        uri(origin);

    for (k, v) in req.headers() {
        if k == http::header::HOST {
            continue
        }

        origin_req = origin_req.header(k, v);
    }

    let body = req.into_body();
    let origin_req = origin_req.body(body)?;
    let origin_res = http_client.send_request(origin_req).await?;

    let mut res = Builder::new().
        status(origin_res.status());
    for (k, v) in origin_res.headers() {
        res = res.header(k, v);
    }

    let body = origin_res.into_body();

    Ok(res.body(body).unwrap())

    // let mut has_error = false;
    // let mut has_payload = false;
    // let payload = match req.into_body() {
    //     Payload::None => Payload::None,
    //     Payload::Fixed(mut p) => match p.next().await.unwrap() {
    //         Ok(data) => {
    //             has_payload = true;
    //             Payload::Fixed(FixedPayload::new(data))
    //         }
    //         Err(_) => {
    //             has_error = true;
    //             Payload::None
    //         }
    //     },
    //     Payload::Stream(_) => unimplemented!(),
    // };
    //
    // let status = if has_error {
    //     StatusCode::INTERNAL_SERVER_ERROR
    // } else if has_payload {
    //     StatusCode::OK
    // } else {
    //     StatusCode::NO_CONTENT
    // };
    // Ok(Builder::new()
    //     .status(status)
    //     .header("Server", "monoio-http-demo")
    //     .body(payload)
    //     //.body(Payload::Fixed(FixedPayload::new(Bytes::from_static(b"Hello, World!"))))
    //     .unwrap())
}
