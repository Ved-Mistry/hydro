use hydro_lang::*;
use rand::prelude::*;
use rand::distributions::WeightedIndex;
use std::{collections::{BTreeSet, HashMap}, iter};

#[derive(Clone)]
pub enum Value {
    Int(u32),
    Str(String),
    Map(HashMap<String, String>),
    Set(BTreeSet<u32>),
}

// Global shared database (mimicking Redis)
pub type Redis = HashMap<String, Value>;

/// The single coordinator—representing, e.g., Redis or the Retwis coordinator
pub struct Coordinator {}

pub struct DbState{}


/// A cluster of these `Client` processes. Each cluster member is effectively one client.
pub struct Client {}

/// Creates one coordinator process and a `Cluster` of 3 clients. Each client has its own RNG.
pub fn ved_retwis_2<'a>(
    flow: &FlowBuilder<'a>,
    _num_users: u32,
) -> (Process<'a, Coordinator>, Cluster<'a, Client>) {
    // 1 coordinator for each of 4 operations
    let coord = flow.process::<Coordinator>();
    // let coord2 = flow.process::<Coordinator>();
    // let coord3 = flow.process::<Coordinator>();
    // let coord4 = flow.process::<Coordinator>();
    // let db_proc = flow.process::<DbState>();


    // Cluster of clients
    let client_cluster = flow.cluster::<Client>();

    // Each cluster node runs the logic in `source_iter(...)` *independently*
    let client_transactions = client_cluster.source_iter(q!({
        let mut rng = rand::thread_rng();

        let choices = [2, 3, 4];
        let weights = [0.5, 0.3, 0.2];
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

    let c1_receive = op1_stream.send_bincode(&coord);
    let c2_receive = op2_stream.send_bincode(&coord);
    let c3_receive = op3_stream.send_bincode(&coord);
    let c4_receive = op4_stream.send_bincode(&coord);

    // Dummy process to emit the initial Redis
    // let _initial_db = db_proc.source_iter(q!({
    //     let mut db: Redis = HashMap::new();
    //     db.insert("next_user_id".into(), Value::Int(1000));
    //     db.insert("next_post_id".into(), Value::Int(1));
    //     db.insert("users".into(), Value::Map(HashMap::new()));
    //     iter::once(db)
    // }));

    //Action 1: Make a User Account
    let account_db = c1_receive.map(q!(move |(_client_id, (_op, args))| {
        let username = args[0].to_string();
        let password = args[1].clone();
        println!("createAccount for username={username}, password={password}");
        (username, password)
    }));
    let users = account_db.map(q!(|(username, _password)| username));

    //Action 2: Follow a User
    let follow_user_mapped = c2_receive.map(q!(move |(_client_id, (_op, args))| {
        let username = args[0].to_string();
        let user_to_follow= args[1].clone();
        println!("followUser from user {username} to {user_to_follow}");
        (username, user_to_follow)
    }));

    //Action 3: Make a Post
    let post_mapped = c3_receive.map(q!(move |(_client_id, (_op, args))| {
        let username = args[0].to_string();
        let content  = args[1].clone();
        println!("post from user {username}, content={content}");
        (username, content)
    }));

    //Action 4: Read Timeline
    let read_timeline_mapped = c4_receive.map(q!(move |(_client_id, (_op, args))| {
        let username = args[0].to_string();
        println!("readOwnTimeline from user {username}");
        username
    }));

    let coord_tick = coord.tick();
    
    unsafe {
        let users_tick = users.tick_batch(&coord_tick).persist();
        let follow_user_db = follow_user_mapped
            .tick_batch(&coord_tick)
            .anti_join(users_tick.clone())
            .map(q!(|(username, user_to_follow)| (user_to_follow, username)))
            .anti_join(users_tick.clone())
            .map(q!(|(user_to_follow, username)| (username, user_to_follow)))
            .persist();

        let posts_db = post_mapped
            .tick_batch(&coord_tick)
            .anti_join(users_tick)
            .persist();

        let _read_timeline = follow_user_db
            .anti_join(read_timeline_mapped.tick_batch(&coord_tick))
            .inspect(q!(|(username, user_to_follow)| println!("readOwnTimeline: {username} following {user_to_follow}")))
            .map(q!(|(username, user_to_follow)| (user_to_follow, username)))
            .join(posts_db) // (user_to_follow, (follower, content))
            .inspect(q!(|(user_to_follow, (follower, content))| println!("readOwnTimeline: {follower} seeing {user_to_follow}'s post {content}")))
            .map(q!(|(user_to_follow, (follower, content))| (follower, (user_to_follow, content))))
            .fold_keyed_commutative(q!(|| String::new()), q!(|combined, (user_to_follow, content)| {
                *combined = format!("{combined}{user_to_follow}: {content}\n");
            }))
            .all_ticks()
            .for_each(q!(|(username, feed)| {
                println!("{username}'s feed:\n{feed}");
            }));
    }

    

    // let upd4 = c4_receive.join(_initial_db.clone()).map(q!(|((_cid, (_op, args)), mut db)| {
    //     let u = args[0].clone();
    //     println!("readTimeline: {}", u);
    //     // e.g., read from db
    //     db
    // }));

    (coord, client_cluster)
}
