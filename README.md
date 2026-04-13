# optd

Query Optimizer Service.


## Get Started

To interact with the CLI, run

```bash
cargo run -p optd-cli
```




## Structure

- `optd/core`: The core optimizer implementation (IR, properties, rules, cost model, cardinality estimation).
- `optd/catalog`: A persistent catalog implementation.
- `connectors/datafusion`: Utilities needed to use optd in DataFusion.
- `cli`: command line interface based on [`datafusion-cli`](https://datafusion.apache.org/user-guide/cli/index.html).
- `sqlFiles`: 





### Non presistent

cargo run -p outputer -- -f .\sqlFiles\q3.example.sql   -p  .\sqlFiles\populate.sql > .\outputs\nonpresistent.txt 

### Presistent

cargo run -p outputer -- -f .\sqlFiles\q3.example.sql -m  -p  .\sqlFiles\populate.sql .\sqlFiles\memo.sql .\sqlFiles\memopopulate.sql > .\outputs\presistent.txt 



#### Problemas

thread 'tokio-runtime-worker' (17284) panicked at optd\core\src\ir\properties\tuple_ordering.rs:101:9:
assertion `left == right` failed
  left: 0
 right: 2
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace