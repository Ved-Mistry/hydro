use hydro_lang::*;
use rand::Rng;

pub struct Coordinator {}
pub struct Client {}

/// Creates one coordinator and three clients.
/// Each client (Twitter account) gets its own random generator
/// and sends operations (0..4) to the coordinator (Redis).
pub fn retwis_3clients_processvector<'a>(
    flow: &FlowBuilder<'a>,
) -> (
    Process<'a, Coordinator>,
    Vec<Process<'a, Client>>,
) {
    // Single coordinator to represent our Redis
    let coordinator = flow.process::<Coordinator>();

    // Three separate clients
    let mut clients = Vec::new();

    for i in 0..3 {
        // Each client is a separate process
        let client = flow.process::<Client>();

        // Each client has its own source_iter with a random generator
        let client_transactions = client.source_iter(q!({
            let mut rng = rand::thread_rng();
            // For demo: generate 5 random operations (0..4)
            (0..5).map(move |_| rng.gen_range(0..4))
        }));

        // Send each operation to the coordinator
        let c_receive = client_transactions.send_bincode(&coordinator);

        // Log for demonstration
        c_receive.clone().for_each(q!(move |t| println!(
            "Client {i}, sent operation {t}"
        )));

        clients.push(client);
    }

    // Return the single coordinator and the three clients
    (coordinator, clients)
}