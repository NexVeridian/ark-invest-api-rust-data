# Contributing code
- Make sure the test pass
- Run `cargo clippy --fix --allow-dirty`

# Dev Install
## Dev Containers
Install docker, vscode and the [Dev Containers Extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)

`git clone`

`Ctrl+Shift+P` **Dev Containers: Open Folder in Container**

Run code with `F5` or `cargo run`  

Run tests with `cargo t`

## Docker Compose
`git clone`

`docker compose -f docker-compose.dev.yml build && docker compose -f docker-compose.dev.yml up`

Remove the cargo cache for buildkit with `docker builder prune --filter type=exec.cachemount`

# License
All code in this repository is dual-licensed under either [License-MIT](./LICENSE-MIT) or [LICENSE-APACHE](./LICENSE-Apache) at your option. This means you can select the license you prefer. [Why dual license](https://github.com/bevyengine/bevy/issues/2373)

# Your contributions
Any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
