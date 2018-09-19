#![feature(async_await, await_macro, futures_api)]

#[macro_use] extern crate redis_async;
mod error;

use byteorder::{LE, ReadBytesExt};
use crate::error::{Error, Result};
use futures::{
    compat::{Future01CompatExt, TokioDefaultSpawner},
    future::{FutureExt, TryFutureExt},
};
use redis_async::{
    client as redis_client,
    resp::{FromResp, RespValue},
};
use serenity::model::event::GatewayEvent;
use std::{
    net::SocketAddr,
    str::FromStr,
};

fn main() {
    tokio::run(try_main().map_err(|why| {
        println!("Err running program: {:?}", why);
    }).boxed().compat(TokioDefaultSpawner));
}

async fn try_main() -> Result<()> {
    let redis_addr = SocketAddr::from_str("127.0.0.1:6379")?;
    let client = await!(redis_client::paired_connect(&redis_addr).compat())?;

    loop {
        let parts: Vec<RespValue> = await!(client.send(resp_array![
            "BLPOP",
            "sharder:from",
            0
        ]).compat())?;

        let (event, shard_id) = parse_parts(parts)?;

        println!("Received event on shard {}: {:?}", shard_id, event);
    }
}

fn parse_parts(mut parts: Vec<RespValue>) -> Result<(GatewayEvent, u64)> {
    let part = if parts.len() == 2 {
        parts.remove(1)
    } else {
        println!("blpop result part count != 2: {:?}", parts);

        return Err(Error::InvalidBlpop);
    };

    let mut message: Vec<u8> = FromResp::from_resp(part)?;
    let message_len = message.len();
    let shard_id = {
        let mut shard_bytes = &message[message_len - 2..];
        shard_bytes.read_u16::<LE>()? as u64
    };
    message.truncate(message_len - 2);

    let event = serde_json::from_slice(&message)?;

    Ok((event, shard_id))
}
