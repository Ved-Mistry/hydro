use hydroflow::hydroflow_syntax;

fn main() {
    let mut df = hydroflow_syntax! {
        source_iter(0..10) -> ;
    };
    df.run_available();
}
