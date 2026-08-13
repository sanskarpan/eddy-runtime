use eddy_macros::main;

#[eddy::main(banana = true)]
fn main() {
    println!("never compiled");
}
