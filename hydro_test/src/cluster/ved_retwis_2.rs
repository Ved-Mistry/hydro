use hydro_lang::*;
use rand::prelude::*;
use rand::distributions::WeightedIndex;
use std::{collections::{BTreeSet, HashMap}, iter, sync::{Arc, Mutex}};
use once_cell::sync::Lazy;

pub enum Value {
    Int(u32),
    Str(String),
    Map(HashMap<String, String>),
    Set(BTreeSet<u32>),
}

// Global shared database (mimicking Redis)
static SHARED_REDIS: Lazy<Arc<Mutex<HashMap<String, Value>>>> = Lazy::new(|| {
    let mut db = HashMap::new();
    db.insert("next_user_id".to_string(), Value::Int(1000));
    db.insert("next_post_id".to_string(), Value::Int(1));
    db.insert("users".to_string(), Value::Map(HashMap::new()));
    Arc::new(Mutex::new(db))
});

/// The single coordinator—representing, e.g., Redis or the Retwis coordinator
pub struct Coordinator {
    pub redis: Arc<Mutex<HashMap<String, Value>>>,
}

impl Coordinator {
    /// Create a new Coordinator with default counters
    pub fn new() -> Self {
        Self {
            redis: SHARED_REDIS.clone(),
        }
    }
}

/// A cluster of these `Client` processes. Each cluster member is effectively one client.
pub struct Client {}

/// Creates one coordinator process and a `Cluster` of 3 clients. Each client has its own RNG.
pub fn ved_retwis_2<'a>(
    flow: &FlowBuilder<'a>,
    _num_users: u32,
) -> (Process<'a, Coordinator>, Process<'a, Coordinator>, Process<'a, Coordinator>, Process<'a, Coordinator>, Cluster<'a, Client>) {
    // 1 coordinator for each of 4 operations
    let coord1 = flow.process::<Coordinator>();
    let coord2 = flow.process::<Coordinator>();
    let coord3 = flow.process::<Coordinator>();
    let coord4 = flow.process::<Coordinator>();


    // Cluster of clients
    let client_cluster = flow.cluster::<Client>();

    // Each cluster node runs the logic in `source_iter(...)` *independently*
    let client_transactions = client_cluster.source_iter(q!({
        let mut rng = rand::thread_rng();

        let choices = [2, 3, 4];
        let weights = [0.5, 0.4, 0.1];
        let dist = WeightedIndex::new(&weights).unwrap();

        let choices2 = ["0", "1", "2"];

        let choices3 = ["LMAO", "Spring Break", "Selfie", "Politics", "Food", "Boba", "Fit Check", "Racism", "Travel", "Family"];

        // First, yield operation 1 with its mandatory args
        iter::once((1, vec![
            "tweeter".to_string() + &CLUSTER_SELF_ID.raw_id.to_string(),
            "password".to_string(),
        ]))
        // Then produce 4 more random operations
        //Split into different streams for each operator and send as different message streams to coordinator (coordinator functions filters for each op when reading)
        .chain((0..4).map(move |_| {
            let op = choices[dist.sample(&mut rng)];
            let args = match op {
                2 => vec![
                    "tweeter".to_string() + &CLUSTER_SELF_ID.raw_id.to_string(),
                    "tweeter".to_string() + choices2.choose(&mut rng).unwrap(),
                ],
                3 => vec![
                    "tweeter".to_string() + &CLUSTER_SELF_ID.raw_id.to_string(),
                    choices3.choose(&mut rng).unwrap().to_string(),
                ],
                4 => vec![
                    "tweeter".to_string() + &CLUSTER_SELF_ID.raw_id.to_string(),
                ],
                _ => unreachable!(),
            };
            (op, args)
        }))
    }));

    let op1_stream = client_transactions.clone().filter(q!(| (op, _args) | op == &1));
    let op2_stream = client_transactions.clone().filter(q!(| (op, _args) | op == &2));
    let op3_stream = client_transactions.clone().filter(q!(| (op, _args) | op == &3));
    let op4_stream = client_transactions.clone().filter(q!(| (op, _args) | op == &4));

    let c1_receive = op1_stream.send_bincode(&coord1);
    let c2_receive = op2_stream.send_bincode(&coord2);
    let c3_receive = op3_stream.send_bincode(&coord3);
    let c4_receive = op4_stream.send_bincode(&coord4);

    let _shared_redis = &*SHARED_REDIS;

    c1_receive.for_each(q!(move |(_client_id, (_op, args))| {
        let username = args[0].to_string();
        let password = &args[1];
        println!("createAccount for username={username}, password={password}");
    }));

    c2_receive.for_each(q!(move |(_client_id, (_op, args))| {
        let username = args[0].to_string();
        let user_to_follow= &args[1];
        println!("followUser from user {username} to {user_to_follow}");
    }));

    c3_receive.for_each(q!(move |(_client_id, (_op, args))| {
        let username = args[0].to_string();
        let content  = &args[1];
        println!("post from user {username}, content={content}");
    }));

    c4_receive.for_each(q!(move |(_client_id, (_op, args))| {
        let username = args[0].to_string();
        println!("readOwnTimeline from user {username}");
    }));

    (coord1, coord2, coord3, coord4, client_cluster)
}
