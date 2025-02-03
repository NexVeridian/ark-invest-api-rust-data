check:
    just precommit-shared
    nix flake update
    nix flake check -v

precommit:
    just precommit-shared
    cargo check
    just test

alias t := test
test:
    cargo t --no-fail-fast

precommit-shared:
    cargo upgrade -v
    cargo update
    cargo fmt --all
    just clippy

clippy:
    cargo clippy --all --fix --allow-dirty -- -W clippy::nursery -W rust-2018-idioms \
        -A clippy::future_not_send -A clippy::option_if_let_else -A clippy::or_fun_call
