# Toy Transaction Processor

## Running

* Production: `cargon run -- <path_to_csv> > output.csv`
* With debug output: `RUST_LOG=info cargo run -- <path_to_csv> > output.csv`

## Assumptions

* On dispute, I believe TXns should go either way; i.e. on withdraw funds are added and on deposit funds deducted. However, this iteration follows the spec which states:

> A dispute represents a client's claim that a transaction was erroneous and should be reversed. The transaction shouldn't be reversed yet but the associated funds should be held. This means that the clients available funds should decrease by the amount disputed, their held funds should increase by the amount disputed, while their total funds should remain the same.

## AI Usage Documentation

* Prompts on test cases and erroronious edge case detection.
* Testcase generation