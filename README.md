Fetches and caches data from csv download and saves the data in parquet format

# Dev Install
## Dev Containers
Install docker, vscode and the [Dev Containers Extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)

`git clone`

`Ctrl+Shift+P` **Dev Containers: Open Folder in Container**

Run code with `F5` or `cargo run`  

Run tests with `cargo t`

## Docker Compose
`git clone`

`docker compose build && docker compose up`

Remove the cargo cache for buildkit with `docker builder prune --filter type=exec.cachemount`
