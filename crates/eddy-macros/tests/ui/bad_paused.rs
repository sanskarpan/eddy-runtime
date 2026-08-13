use eddy_macros::test;

#[eddy::test(start_paused = "yes")]
fn paused() {
    println!("never compiled");
}
