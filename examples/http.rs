#![allow(unused_variables)]
#![allow(unused_imports)]

extern crate fred;
extern crate tokio_core;
extern crate futures;
extern crate hyper;

use fred::RedisClient;
use fred::sync::borrowed::RedisClientRemote;
use fred::types::*;
use fred::error::*;

use tokio_core::reactor::Core;
use futures::Future;
use futures::future;

use hyper::{
  Error as HyperError,
  Uri,
  Method,
  Body,
  StatusCode
};
use hyper::header::{
  ContentLength,
  Headers,
  ContentType
};
use hyper::server::{
  Http,
  Request,
  Response,
  Service
};

use std::io::{
  Error as IoError,
  ErrorKind as IoErrorKind
};
use std::error::Error;

const NULL: &'static str = "null";
const INTERFACE: &'static str = "127.0.0.1";
const PORT: u16 = 3000;

#[derive(Clone)]
pub struct HttpInterface<'a> {
  client: &'a RedisClient
}

impl<'a> HttpInterface<'a> {

  pub fn new(client: &'a RedisClient) -> HttpInterface<'a> {
    HttpInterface { client }
  }

}

impl<'a> Service for HttpInterface<'a> {
  type Request = Request;
  type Response = Response;
  type Error = hyper::Error;
  type Future = Box<Future<Item=Response, Error=hyper::Error>>;

  fn call(&self, req: Request) -> Self::Future {
    match (req.method(), req.path()) {
      (&Method::Get, "/foo") => {
        let client = self.client.clone();

        Box::new(client.get("foo").then(|result| {
          let (output, code) = match result {
            Ok((_, value)) => match value {
              Some(v) => match v.into_string() {
                Some(s) => (s, StatusCode::Ok),
                None => (NULL.to_owned(), StatusCode::Ok)
              },
              None => ("".to_owned(), StatusCode::NotFound)
            },
            Err(e) => (format!("{:?}", e), StatusCode::InternalServerError)
          };

          Ok(Response::new().with_status(code)
            .with_header(ContentLength(output.len() as u64))
            .with_header(ContentType::plaintext())
            .with_body(output))
        }))
      },
      _ => {
        Box::new(future::ok(
          Response::new().with_status(StatusCode::NotFound)
        ))
      },
    }
  }

}

fn main() {
  let addr_string = format!("{}:{}", INTERFACE, PORT);

  let addr = match addr_string.parse() {
    Ok(addr) => addr,
    Err(e) => panic!("Error parsing address string: {:?}", e)
  };

  let config = RedisConfig::default();
  let policy = ReconnectPolicy::Constant {
    delay: 5000,
    attempts: 0,
    max_attempts: 10
  };
  let client = RedisClient::new(config);

  // give the service its own clone of the client
  let http_client = client.clone();
  let server = Http::new().bind(&addr, move || {
    Ok(HttpInterface::new(&http_client))
  });

  let server = match server {
    Ok(s) => s,
    Err(e) => panic!("Error creating HTTP server! {:?}", e)
  };

  // use the same event loop handle as the http server for the client
  let handle = server.handle();
  let connection = client.connect_with_policy(&handle, policy);

  println!("Starting HTTP server on port {:?}", PORT);
  // run the http server until the redis connection dies
  let _ = server.run_until(connection.map_err(|_| ()));
}