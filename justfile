set shell := ["bash", "-c"]
set dotenv-path := "./config/dev.env"
engine := `if command -v docker >/dev/null 2>&1; then echo "docker"; else echo "podman"; fi`

precommit upgrade="false":
    #!/usr/bin/env bash
    if [[ {{upgrade}} == "true" ]]; then just upgrade; fi
    just clippy
    cargo check
    just test

check upgrade="false":
    #!/usr/bin/env bash
    if [[ {{upgrade}} == "true" ]]; then just upgrade; fi
    just clippy
    nix flake check -v

alias t := test
test:
    cargo t --workspace --no-fail-fast --no-tests=pass -E "all() - test(get_api) - kind(bin)"

alias u := upgrade
alias update := upgrade
upgrade:
    nix flake update
    cargo upgrade -v --recursive
    cargo update --workspace

clippy:
    cargo fmt --all
    cargo clippy --workspace --fix --allow-dirty
