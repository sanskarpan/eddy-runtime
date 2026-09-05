RUSTFLAGS_LOOM := --cfg loom

.PHONY: test loom miri bench bench-vs-tokio bench-instrumentation bench-report console console-web check

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

bench-instrumentation:
	cargo bench -p eddy --bench vs_tokio --features instrumentation

bench-report:
	cargo bench -p eddy --bench vs_tokio -- instrumentation-overhead --save-baseline "$${BASELINE:-phase18}"
	cargo bench -p eddy --bench vs_tokio --features instrumentation -- instrumentation-overhead --baseline "$${BASELINE:-phase18}"

console:
	cargo run -p eddy-console

console-web:
	cargo run -p eddy-console-web

check:
	cargo fmt --all -- --check
	cargo clippy -p eddy --all-targets -- -D warnings
	cargo test -p eddy --all-targets
