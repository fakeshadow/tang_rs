# tang_rs
a connection pool for tokio-postgres and redis-rs. support async/await syntex.

#### Requirement:
`rustc 1.39.0-beta.5 (fa5c2f3e5 2019-10-02)`<br>
* some example may require nightly in their dependencies.

#### Features:
default features use tokio0.2 and have both tokio-postgres and redis-rs(std::future branch) as dependencies.
<br>
<br>
tokio-executor and tokio-timer are mandatory features when you use `default-features = false`