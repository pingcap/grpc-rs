extern crate grpc;
extern crate protobuf;
extern crate futures;

#[path="./generated/route_guide.rs"]
mod route_guide;
#[path="./generated/route_guide_grpc.rs"]
mod route_guide_grpc;

use std::sync::Arc;

use grpc::{Environment, ChannelBuilder, Result};
use futures::{Future, Stream, stream, Sink};

use route_guide::{Point, Rectangle, RouteNote};
use route_guide_grpc::RouteGuideClient;

fn new_point(lat: i32, lon: i32) -> Point {
    let mut point = Point::new();
    point.set_latitude(lat);
    point.set_longitude(lon);
    point
}

fn new_rect(lat1: i32, lon1: i32, lat2: i32, lon2: i32) -> Rectangle {
    let mut rect = Rectangle::new();
    rect.set_hi(new_point(lat1, lon1));
    rect.set_lo(new_point(lat2, lon2));
    rect
}

fn new_note(lat: i32, lon: i32, msg: &str) -> RouteNote {
    let mut note = RouteNote::new();
    note.set_location(new_point(lat, lon));
    note.set_message(msg.to_owned());
    note
}

fn main() {
    let env = Arc::new(Environment::new(2));
    let channel = ChannelBuilder::new(env).connect("127.0.0.1:50051");
    let client = RouteGuideClient::new(channel);
    let point = new_point(409146138, -746188906);
    let get_feature = client.get_feature_async(point).unwrap().and_then(|f| {
        println!("async get_feature: {:?}", f);
        Ok(())
    });

    let rect = new_rect(400000000, -750000000, 420000000, -730000000);
    let list_features = client.list_features(rect).unwrap().for_each(|f| {
        println!("server streaming list_features: {:?}", f);
        Ok(())
    });

    let call = client.record_route().unwrap();
    let points: Vec<Result<_>> = vec![
        Ok((416560744, -746721964)),
        Ok((406411633, -741722051)),
        Ok((411633782, -746784970)),
        Ok((406411633, -741722051)),
        Ok((415830701, -742952812)),
    ];
    let record_route = call.send_all(stream::iter(points).map(|(lat, lon)| new_point(lat, lon)))
        .and_then(|(call, _)| call.into_receiver())
        .and_then(|s| {
            println!("client streaming record_route: {:?}", s);
            Ok(())
        });

    let mut call = client.route_chat().unwrap();
    let route_chat = call.take_receiver().unwrap().for_each(|note| {
        println!("duplex streaming route_chat: {:?}", note);
        Ok(())
    });

    let notes: Vec<Result<_>> = vec![
        Ok(new_note(0, 0, "First message")),
        Ok(new_note(0, 1, "Second message")),
        Ok(new_note(1, 0, "Third message")),
        Ok(new_note(0, 0, "Fourth message")),
    ];
    let write = call.send_all(stream::iter(notes));

    let feature = client.get_feature(new_point(0, 0));
    println!("sync get_feature: {:?}", feature);

    get_feature.join5(list_features, record_route, route_chat, write).wait().unwrap();
}
