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
```rust
pub enum Source {
    // Reads Parquet file if exists
    Read,
    // From ARK Invest
    Ark,
    // From api.NexVeridian.com
    #[default]
    ApiIncremental,
    // From api.NexVeridian.com, not usually nessisary, use ApiIncremental
    ApiFull,
    // From arkfunds.io/api, avoid using, use ApiIncremental instead
    ArkFundsIoIncremental,
    // From arkfunds.io/api, avoid using, use ApiFull instead
    ArkFundsIoFull,
}
```

# License
All code in this repository is dual-licensed under either [License-MIT](./LICENSE-MIT) or [LICENSE-APACHE](./LICENSE-Apache) at your option. This means you can select the license you prefer. [Why dual license](https://github.com/bevyengine/bevy/issues/2373)
