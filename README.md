Fetches and caches data from csv download and saves the data in parquet format

# Dev Install
## Dev Containers
Install docker, vscode, [Remote Development Extension Pack](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.vscode-remote-extensionpack), and the [GitHub Repositories Extension](https://marketplace.visualstudio.com/items?itemName=GitHub.remotehub)

`Ctrl+Shift+P` **Dev Containers: Clone Repository in Container Volume** 

Select github then paste the url `https://github.com/NexVeridian/ark-invest-api-rust-data`

Run code with `F5` or `cargo run`  

## Docker Compose
`git clone`

`docker compose build && docker compose up`

Remove the cargo cache for buildkit with `docker builder prune --filter type=exec.cachemount`
