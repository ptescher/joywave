=<p align="center">
<img src="docs/source/_static/images/logo.png" width="128" alt="logo"/>
</p>

  <p align="center"><b>Joywave</b>: A dangerously fast query engine for blockchain based big data.</p>
    <p align="center">


## Description
Joywave takes SQL queries and runs directly on blocks of data sourced from blockchain nodes. This could be JSONRPC websocket subscriptions, [StreamingFast Firehose](https://firehose.streamingfast.io) connections, etc.

We then grab chunks of that data in partitions, and spread those partitions out to multiple cores. We can even spread the load across multiple instances all coordinated via protocol buffers.

It then exposes those primitives via a PostgreSQL socket.

This would be a pretty large task if not for the work of the [Arrow DataFusion](https://datafusion.apache.org) library that does all the heavy lifting. All we have to do is source the blocks/transactions/receipts/logs.

DataFusion features a full query planner, a columnar, streaming, multi-threaded, vectorized execution engine, and partitioned data sources. You can customize DataFusion at almost all points including additional data sources, query languages, functions, custom operators and more.

DataFusion Ballista then takes this system and turns it into a distributed compute platform, similar to Apache Spark.

A rough benchmark extracting ERC20 transfers can process about 2k blocks and 1M token transfers a minute on a laptop over WiFi without breaking a sweat. Since the whole thing can be run in parallell across multiple systems it should scale horizontaly until the block source and or network bandwitdh becomes the bottleneck.

## Usage

```bash
cargo run
```

```bash
psql -h localhost
```

```sql
SELECT
  timestamp,
  transaction_hash::BYTEA,
  transfer_from::BYTEA,
  transfer_to::BYTEA,
  string_from_uint256_bytes(transfer_amount) as transfer_amount_string
FROM erc20_transfers
WHERE block_number > 21840000 AND block_number < 21850000
LIMIT 10;

SELECT
  token_address::BYTEA,
  string_from_uint256_bytes(uint256_sum(transfer_amount)) as volume,
  COUNT(1) as transfers
FROM erc20_transfers
WHERE block_number > 21840000 AND block_number < 21840010
GROUP BY token_address
ORDER BY transfers DESC
LIMIT 10;

SELECT
  timestamp,
  block_number,
  b58_string_from_bytes(input_mint::BYTEA) as input_mint,
  input_amount,
  b58_string_from_bytes(output_mint::BYTEA) as output_mint
FROM jupiter_v6_swaps
WHERE block_number > 300023000 AND block_number < 300023010;
```

## Roadmap

- [ ] Support network scheduler with etcd persistance
- [ ] Example with PostgreSQL and/or Redis output
- [ ] Add tables for other chains, other IDL decoding
- [ ] Add common union tables for multichain swaps and transfers
- [ ] Add tests
- [ ] Add documentation
