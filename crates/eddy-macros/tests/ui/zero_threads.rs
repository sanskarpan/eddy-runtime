use eddy_macros::main;

#[eddy::main(worker_threads = 0)]
fn main() {
    println!("never compiled");
}
