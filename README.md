Fetches and caches ETF data daily, from csv download or api, and saves the data in parquet format

[api.NexVeridian.com](https://api.NexVeridian.com)

Not affiliated with Ark Invest

# Install
Copy docker-compose.yml

Create data folder next to docker-compose.yml
```
├───data
│   └───parquet
├───docker-compose.yml
```

`docker compose up --pull always`

# Changing the data source
In docker-compose.yml, change the data source by changing the environment variable
```
environment:
	- ARK_SOURCE=ApiIncremental
```
Env string ARK_SOURCE must be in the enum Source
```
pub enum Source {
    // Reads Parquet file if exists
    Read,
    // From ARK Invest
    Ark,
    // From api.NexVeridian.com (Default)
    ApiIncremental,
	// From api.NexVeridian.com, not usually nessisary, use ApiIncremental
    ApiFull,
}
```

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

