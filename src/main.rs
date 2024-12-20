mod cache;
mod commands;
mod connection;
mod frame;
mod id_generator;
mod parse;
mod server;

// How to group actions by request, for example multi-get

use crate::connection::Connection;
// use memory_cache::memory_cache::MemoryCache;
use crate::cache::Cache;
use tokio::net::{TcpListener, TcpStream};

async fn process(socket: TcpStream) {
    println!("Conn");
    let mut connection = Connection::new(socket);

    connection.read_frame().await.unwrap();
}

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:8080").await.unwrap();

    println!("Listening");

    let cache = Cache::new();

    loop {
        let (socket, addr) = listener.accept().await.unwrap();
        // Clone the handle to the hash map.
        let cache = cache.clone();

        tokio::spawn(async move {
            process(socket).await;
        });
    }
}
