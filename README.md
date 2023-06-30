Fetches and caches ETF data daily, from csv download or api, and saves the data in parquet format

[api.NexVeridian.com](https://api.NexVeridian.com)

Not affiliated with Ark Invest

# Install for csv download
Copy docker-compose.yml

Create data folder next to docker-compose.yml
```
├───data
│   └───parquet
├───docker-compose.yml
```

`docker compose up --pull always`

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

# Install for api
`git clone`

in main.rs change `Source::Ark` to `Source::ApiIncremental` or `Source::ApiFull` for first run

in docker-compose.yml remove this line`image: ghcr.io/NexVeridian/ark-invest-api-rust-data:latest`

uncomment everything else
