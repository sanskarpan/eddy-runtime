RUSTFLAGS_LOOM := --cfg loom

.PHONY: test loom miri bench bench-vs-tokio console console-web check

test:
	cargo test --workspace

loom:
	RUSTFLAGS="$(RUSTFLAGS_LOOM)" cargo test -p eddy --lib

miri:
	cargo +nightly miri test -p eddy --lib -- task:: sync:: time::wheel::

bench:
	cargo bench -p eddy

bench-vs-tokio:
	cargo bench -p eddy --bench vs_tokio

console:
	cargo run -p eddy-console

console-web:
	cargo run -p eddy-console-web

check:
	cargo fmt --all -- --check
	cargo clippy -p eddy --all-targets -- -D warnings
	cargo test -p eddy --all-targets
