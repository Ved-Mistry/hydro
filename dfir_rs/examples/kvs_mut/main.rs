use std::net::SocketAddr;

use clap::{Parser, ValueEnum};
use client::run_client;
use dfir_rs::lang::graph::{WriteConfig, WriteGraphType};
use dfir_rs::util::ipv4_resolve;
use server::run_server;

mod client;
mod helpers;
mod protocol;
mod server;

#[dfir_rs::main]
async fn main() {
    let opts = Opts::parse();
    match opts.role {
        Role::Client => {
            run_client(opts).await;
        }
        Role::Server => {
            run_server(opts).await;
        }
    }
}

// The `Opts` structure contains the command line arguments accepted by the application and can
// be modified to suit your requirements. Refer to the clap crate documentation for more
// information.  The lines starting with
// `///` contain the message that appears when you run the compiled binary with the '--help'
// arguments, so feel free to change it to whatever makes sense for your application.
// See https://docs.rs/clap/latest/clap/ for more information.
/// A simple KVS server & client. New values overwrite old values.
#[derive(Parser, Debug)]
struct Opts {
    /// The role this application process should assume. The example in the template provides two
    /// roles: server and client.
    #[clap(value_enum, long)] // value_enum => parse as enum. long => "--role" instead of "-r".
    role: Role,

    /// The server's network address. The server listens on this address. The client sends messages
    /// to this address. Format is `"ip:port"`.
    // `value_parser`: parse using ipv4_resolve
    #[clap(long, value_parser = ipv4_resolve, default_value = DEFAULT_SERVER_ADDRESS)]
    address: SocketAddr,

    /// If specified, a graph representation of the flow used by the program will be
    /// printed to the console in the specified format. This parameter can be removed if your
    /// application doesn't need this functionality.
    #[clap(long)]
    graph: Option<WriteGraphType>,

    #[clap(flatten)]
    write_config: Option<WriteConfig>,
}

/// The default server address & port on which the server listens for incoming messages. Clients
/// send message to this address & port.
pub const DEFAULT_SERVER_ADDRESS: &str = "localhost:52061";

/// A running application can assume one of these roles. The launched application process assumes
/// one of these roles, based on the `--role` parameter passed in as a command line argument.
#[derive(Clone, ValueEnum, Debug)]
enum Role {
    Client,
    Server,
}

#[test]
fn test() {
    use std::io::Write;

    use dfir_rs::util::{run_cargo_example, wait_for_process_output};

    let (_server, _, mut server_stdout) =
        run_cargo_example("kvs_mut", "--role server --address 127.0.0.1:2061");

    let (_client1, mut client1_stdin, mut client1_stdout) =
        run_cargo_example("kvs_mut", "--role client --address 127.0.0.1:2061");

    let mut server_output = String::new();
    wait_for_process_output(
        &mut server_output,
        &mut server_stdout,
        "Server is live! Listening on 127\\.0\\.0\\.1:2061\n",
    );

    let mut client1_output = String::new();
    wait_for_process_output(
        &mut client1_output,
        &mut client1_stdout,
        "Client is live! Listening on 127\\.0\\.0\\.1:\\d+ and talking to server on 127\\.0\\.0\\.1:2061\n",
    );

    client1_stdin.write_all(b"PUT a,7\n").unwrap();

    let (_client2, mut client2_stdin, mut client2_stdout) =
        run_cargo_example("kvs_mut", "--role client --address 127.0.0.1:2061");

    let mut client2_output = String::new();
    wait_for_process_output(
        &mut client2_output,
        &mut client2_stdout,
        "Client is live! Listening on 127\\.0\\.0\\.1:\\d+ and talking to server on 127\\.0\\.0\\.1:2061\n",
    );

    client2_stdin.write_all(b"GET a\n").unwrap();
    wait_for_process_output(
        &mut client2_output,
        &mut client2_stdout,
        r#"Got a Response: KvsResponse \{ key: "a", value: "7" \}"#,
    );

    client1_stdin.write_all(b"PUT a,8\n").unwrap();
    client1_stdin.write_all(b"GET a\n").unwrap();
    wait_for_process_output(
        &mut client1_output,
        &mut client1_stdout,
        r#"Got a Response: KvsResponse \{ key: "a", value: "8" \}"#,
    );
}
