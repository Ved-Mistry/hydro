use hydro_deploy::Deployment;
use hydro_lang::deploy::TrybuildHost;

#[tokio::main]
async fn main() {
    let mut deployment = Deployment::new();
    let _localhost = deployment.Localhost();

    let builder = hydro_lang::FlowBuilder::new();
    let num_users: u32 = 3;

    let (coordinator, client_cluster) =
        hydro_test::cluster::ved_retwis_2::ved_retwis_2(&builder, num_users);

    let _rustflags = "-C opt-level=3 -C codegen-units=1 -C strip=none -C debuginfo=2 -C lto=off";

    let _nodes = builder
        .with_process(&coordinator, TrybuildHost::new(deployment.Localhost()))
        .with_cluster(
            &client_cluster, 
            (0..num_users).map(|_| TrybuildHost::new(deployment.Localhost())),
        )
        .deploy(&mut deployment);

    deployment.deploy().await.unwrap();

    deployment.start().await.unwrap();

    tokio::signal::ctrl_c().await.unwrap();
}