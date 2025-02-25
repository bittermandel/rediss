// Uncomment this block to pass the first stage
use std::{
    collections::HashMap,
    io::{BufReader, Read, Write},
    net::TcpListener,
    str::from_utf8,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use chrono::Utc;
use parser::{parse_command, RespType};

mod parser;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    let kv = Arc::new(Mutex::new(HashMap::<String, RedisValue>::new()));

    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                println!("accepted new connection");
                let kv = Arc::clone(&kv);
                thread::spawn(move || loop {
                    let mut reader = BufReader::new(&_stream);
                    let mut input = [0; 512];
                    let bytes_read = reader.read(&mut input).unwrap();
                    if bytes_read == 0 {
                        break;
                    }

                    let (_, result) = parse_command(from_utf8(&input).unwrap()).unwrap();

                    match result {
                        parser::RespType::Array(array) => {
                            let command = &array[0];
                            match command {
                                RespType::BulkString(command) => {
                                    let command = from_utf8(command).unwrap();
                                    match command {
                                        "ECHO" => {
                                            let mut return_value = String::new();

                                            for value in &array[1..] {
                                                match value {
                                                    RespType::BulkString(bulkstring) => {
                                                        // If the second argument is a BulkString,
                                                        return_value += DataType::BulkString(
                                                            from_utf8(&bulkstring)
                                                                .unwrap()
                                                                .to_string(),
                                                        )
                                                        .serialize()
                                                        .as_str();
                                                    }
                                                    RespType::Array(array) => {
                                                        let mut data_types: Vec<DataType> =
                                                            Vec::new();

                                                        for array_entry in array {
                                                            match array_entry {
                                                                RespType::BulkString(
                                                                    bulkstring,
                                                                ) => {
                                                                    // If the second argument is a BulkString,
                                                                    data_types.push(
                                                                        DataType::BulkString(
                                                                            from_utf8(&bulkstring)
                                                                                .unwrap()
                                                                                .to_string(),
                                                                        ),
                                                                    );
                                                                }
                                                                _ => (),
                                                            }
                                                        }

                                                        return_value += DataType::Array(data_types)
                                                            .serialize()
                                                            .as_str();
                                                    }
                                                    _ => (),
                                                }
                                            }
                                            _stream.write(return_value.as_bytes()).unwrap();
                                        }
                                        "PING" => {
                                            _stream.write("$4\r\nPONG\r\n".as_bytes()).unwrap();
                                        }
                                        "SET" => {
                                            let key: String;
                                            let mut value = RedisValue {
                                                value: "".to_string(),
                                                expiration_time: None,
                                            };
                                            match &array[1] {
                                                RespType::BulkString(bulkstring) => {
                                                    key =
                                                        from_utf8(bulkstring).unwrap().to_string();
                                                }
                                                _ => panic!("should be a BulkString"),
                                            };
                                            match &array[2] {
                                                RespType::BulkString(bulkstring) => {
                                                    value.value =
                                                        from_utf8(bulkstring).unwrap().to_string();
                                                }
                                                _ => panic!("should be a BulkString"),
                                            };
                                            if array.len() > 3 {
                                                let argument: String;
                                                let argument_value: String;
                                                match &array[3] {
                                                    RespType::BulkString(bulkstring) => {
                                                        argument = from_utf8(bulkstring)
                                                            .unwrap()
                                                            .to_string();
                                                    }
                                                    _ => panic!("should be a BulkString"),
                                                };
                                                match &array[4] {
                                                    RespType::BulkString(bulkstring) => {
                                                        argument_value = from_utf8(bulkstring)
                                                            .unwrap()
                                                            .to_string();
                                                    }
                                                    _ => panic!("should be a BulkString"),
                                                };

                                                match argument.as_str() {
                                                    "px" => {
                                                        let now = Utc::now()
                                                            + Duration::from_millis(
                                                                argument_value.parse().unwrap(),
                                                            );
                                                        value.expiration_time =
                                                            Some(now.timestamp_millis());
                                                    }
                                                    _ => (),
                                                }
                                            }

                                            kv.lock().unwrap().insert(key, value);
                                            _stream.write("+OK\r\n".as_bytes()).unwrap();
                                        }
                                        "GET" => {
                                            let key: String;

                                            match &array[1] {
                                                RespType::BulkString(bulkstring) => {
                                                    key =
                                                        from_utf8(bulkstring).unwrap().to_string();
                                                }
                                                _ => panic!("should be a BulkString"),
                                            };

                                            let kv = kv.lock().unwrap();
                                            let value = kv.get(&key);

                                            match value {
                                                Some(value) => {
                                                    if let Some(exp_time) = value.expiration_time {
                                                        println!(
                                                            "{:?} - {:?}",
                                                            Utc::now().timestamp_millis(),
                                                            exp_time,
                                                        );
                                                        if Utc::now().timestamp_millis() > exp_time
                                                        {
                                                            _stream
                                                                .write("$-1\r\n".as_bytes())
                                                                .unwrap();
                                                            break;
                                                        }
                                                    }
                                                    _stream
                                                        .write(
                                                            DataType::BulkString(
                                                                value.value.clone(),
                                                            )
                                                            .serialize()
                                                            .as_bytes(),
                                                        )
                                                        .unwrap();
                                                }
                                                None => {
                                                    _stream.write("$-1\r\n".as_bytes()).unwrap();
                                                }
                                            }
                                        }
                                        _ => (),
                                    }
                                }
                                _ => break,
                            }
                        }
                        _ => break,
                    }
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

#[derive(PartialEq, Eq, Hash)]
pub struct RedisValue {
    value: String,
    expiration_time: Option<i64>,
}

pub enum DataType {
    SimpleString(String),
    BulkString(String),
    Array(Vec<DataType>),
}

impl DataType {
    pub fn serialize(self) -> String {
        match self {
            DataType::BulkString(s) => format!("${}\r\n{}\r\n", s.chars().count(), s),
            DataType::Array(array) => {
                let mut return_value = format!("*{}\r\n", array.len());
                for value in array {
                    return_value += value.serialize().as_str();
                }

                return_value
            }
            _ => unimplemented!(),
        }
    }
}
