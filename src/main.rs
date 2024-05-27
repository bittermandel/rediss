// Uncomment this block to pass the first stage
use std::{
    io::{Read, Write},
    net::TcpListener,
    thread,
};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                println!("accepted new connection");

                thread::spawn(move || loop {
                    let mut buf = [0; 512];
                    let len = _stream.read(&mut buf).unwrap();
                    if len == 0 {
                        break;
                    }

                    _stream.write(b"+PONG\r\n").unwrap();
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
