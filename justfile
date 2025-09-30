precommit:
    just clippy
    cargo check --workspace
    just test

check attic="false":
    #!/usr/bin/env bash
    just clippy

    if [[ {{attic}} = "false" ]]; then
        nix-fast-build --no-link
    else
        attic cache create {{attic}} | true
        attic use {{attic}} | true
        nix-fast-build --no-link
        for i in {1..10}; do
          attic push {{attic}} /nix/store/*/ && break || [ $i -eq 5 ] || sleep 5
        done
    fi

alias t := test
test:
    cargo t --workspace --no-fail-fast --no-tests=pass

bacon:
    bacon nextest

alias update := upgrade
upgrade force="false":
    #!/usr/bin/env bash
    nix flake update
    if [[ {{force}} == "true" ]] then
        cargo upgrade -v --recursive -i
        cargo update --workspace --recursive --verbose
    else
        cargo upgrade -v --recursive
        cargo update --workspace --recursive --verbose
    fi

alias fmt := clippy
clippy:
    cargo fmt --all
    tombi fmt
    cargo clippy --all-targets --workspace --fix --allow-dirty
    cargo machete --fix
