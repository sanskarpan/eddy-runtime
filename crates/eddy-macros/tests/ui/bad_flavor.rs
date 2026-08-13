use eddy_macros::main;

#[eddy::main(flavor = "wrong")]
fn main() {
    println!("never compiled");
}
