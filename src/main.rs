mod database;

use std::sync::Arc;
use tiny_http::{Request, Response, Server};

use crate::database::DB;

type DATABASE = DB<String, String>;

fn main() {
    let server = Server::http("0.0.0.0:8000").unwrap();

    // Shared state for key-value store
    let kv_store: Arc<DATABASE> = Arc::new(DB::new());

    for request in server.incoming_requests() {
        println!("got request");
        let kv_store = kv_store.clone();
        handle_request(request, kv_store);
    }
}

fn handle_request(request: Request, kv_store: Arc<DATABASE>) {
    let path = request.url();
    if path.starts_with("/set?") {
        handle_set(request, kv_store);
    } else if path.starts_with("/get?") {
        handle_get(request, kv_store);
    } else {
        let response = Response::from_string("Not Found").with_status_code(404);
        request.respond(response).unwrap();
    }
}

fn handle_get(request: Request, kv_store: Arc<DATABASE>) {
    println!("get request");
    let path = request.url();
    let query = path.trim_start_matches("/get?");
    let key = query.trim();

    println!("get key: {}", key);

    if let Some(value) = kv_store.get(&key.to_owned()) {
        let response = Response::from_string(value.clone());
        request.respond(response).unwrap();
    } else {
        let response = Response::from_string("Key not found").with_status_code(404);
        request.respond(response).unwrap();
    }
}

fn handle_set(mut request: Request, kv_store: Arc<DATABASE>) {
    println!("set request");
    let mut content = String::new();
    request.as_reader().read_to_string(&mut content).unwrap();

    let path = request.url();
    let query = path.trim_start_matches("/set?");
    let parts: Vec<&str> = query.splitn(2, '=').collect();

    if parts.len() == 2 {
        let key = parts[0].to_string();
        let value = parts[1].to_string();

        println!("set key {} to value {}", key, value);

        kv_store.insert(key, value);

        let response = Response::from_string("OK");
        request.respond(response).unwrap();
    } else {
        let response = Response::from_string("Invalid input").with_status_code(400);
        request.respond(response).unwrap();
    }
}
