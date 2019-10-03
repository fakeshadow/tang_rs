(October 3, 2019)
----------------------

### Breaking
- Return error type `tang_rs::PostgresPoolError` and `tang_rs::RedisPoolEror` when use `Pool.get()` 

### Add
- `PoolRef.take_conn()` method to take the ownership of connection out from pool.
- `Builder.queue_timeout(<Duration>)` method to indicate the timeout of waiting queue for pool.