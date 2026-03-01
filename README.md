# Toy Transaction Processor

## Assumptions

* On dispute, I believe TXns should go either way; i.e. on withdraw funds are added and on deposit funds deducted. However, this iteration follows the spec which states:

> A dispute represents a client's claim that a transaction was erroneous and should be reversed. The transaction shouldn't be reversed yet but the associated funds should be held. This means that the clients available funds should decrease by the amount disputed, their held funds should increase by the amount disputed, while their total funds should remain the same.

## AI Usage Documentation