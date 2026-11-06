# 500-wallet synthetic transport comparison

The project's supplied local pressure-test report compares submission latency when transaction sends and receipt lookups share an HTTP connection pool versus when receipt lookups use an isolated pool.

## Reported results

| Submission latency | Shared pool | Isolated receipt pool |
| --- | ---: | ---: |
| Median (p50) | 400.2 ms | 231.7 ms |
| 95th percentile (p95) | 511.0 ms | 341.0 ms |
| Maximum | 539.2 ms | 370.6 ms |

All 500 synthetic submissions were observed in both reported runs.

The reported reduction in p95 submission latency is approximately **33%**, calculated from the two supplied percentile values. This comparison tests local synthetic transport behavior; it is not a measurement of transaction inclusion on a live blockchain.

## What was being tested

The design question was whether high-volume receipt traffic interfered with a burst of outgoing transaction requests. The implementation separated the receipt workload into its own bounded `aiohttp` session and connector.

The archive also includes tests covering launch scheduling, adaptive lane selection, endpoint fallback, receipt pressure and supply boundaries.

The numbers above are taken from the supplied project report. They have not been independently reproduced from a live chain or rerun here.

## Interpreting the result

Separating two workloads that compete for a finite connection pool can improve submission latency even if the overall number of operations stays the same. The result supports explaining this design change as a practical concurrency optimization.
