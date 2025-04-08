mod cachestate;
mod hash;

use std::io;
use std::mem::size_of_val;
use std::num::NonZeroUsize;
use std::os::fd::AsRawFd;
use std::thread::available_parallelism;
use bytes::Bytes;
use http::{response::Builder, HeaderMap, StatusCode};
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

async fn thread_main() -> Result<(), io::Error> {
    let listener = TcpListener::bind("0.0.0.0:50002").unwrap();

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
                monoio::spawn(handle_connection(stream));
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

async fn handle_connection(stream: TcpStream) {
    let (r, w) = stream.into_split();
    let sender = GenericEncoder::new(w);
    let mut receiver = RequestDecoder::new(r);
    let (mut tx, rx) = spsc_pair();
    monoio::spawn(handle_task(rx, sender));

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
    mut sender: impl Sink<Response, Error = impl Into<HttpError>>,
) -> Result<(), HttpError> {
    loop {
        let request = match receiver.recv().await {
            Some(r) => r,
            None => {
                return Ok(());
            }
        };
        let resp = handle_request(request).await;
        sender.send_and_flush(resp).await.map_err(Into::into)?;
    }
}

async fn handle_request(req: Request) -> Response {
    // let mut headers = HeaderMap::new();
    // headers.insert("Server", "monoio-http-demo".parse().unwrap());
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
    Builder::new()
        .status(StatusCode::OK)
        //.header("Server", "monoio-http-demo")
        //.body(Payload::None)
        .body(Payload::Fixed(FixedPayload::new(Bytes::from_static(b"Hello, World!"))))
        .unwrap()
}

