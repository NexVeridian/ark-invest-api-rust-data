precommit:
	cargo update
	cargo check
	cargo fmt
	cargo t
	cargo clippy --fix --allow-dirty
