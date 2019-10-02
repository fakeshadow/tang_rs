# tang_rs
a connection pool for tokio-postgres and redis-rs. support async/await syntex.

#### Requirement:
`rustc 1.40.0-nightly (ddf43867a 2019-09-26)`<br>

#### Features:
default features use tokio0.2 and have both tokio-postgres and redis-rs(std::future branch) as dependecies.
<br>
<br>
tokio-executor and tokio-timer are mandatory features when you use `default-features = false`