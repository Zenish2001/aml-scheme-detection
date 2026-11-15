# Data dictionary

The cleaned data in `data/clean/`, written by `IngestMain` and `EllipticMain`. All of it is Parquet, except the exchange rates.

## `data/clean/txns`: IBM HI-Small transactions

One row per transaction, 5,078,345 rows, in one folder per day.

| Column | Type | Meaning |
|---|---|---|
| `timestamp` | text | The time as written in the file, such as `2022/09/01 00:06` |
| `ts` | timestamp | The same time, parsed |
| `day` | date | The calendar day (the folder the row is in) |
| `fromBank`, `fromAcct` | text | The paying bank and account. Text, because many start with zeros |
| `toBank`, `toAcct` | text | The receiving bank and account |
| `amountPaid`, `payCcy` | number, text | The amount paid, and its currency |
| `amountReceived`, `recvCcy` | number, text | The amount received, and its currency |
| `amountPaidUsd`, `amountReceivedUsd` | number | The same amounts in US dollars |
| `format` | text | Payment format: ACH, Bitcoin, Cash, Cheque, Credit Card, Reinvestment or Wire |
| `isLaundering` | 0 or 1 | 1 for a laundering transaction |
| `scheme` | text or empty | For laundering: one of the 8 schemes, or `UNASSIGNED`. Empty for normal transactions |

## `data/clean/usd_rates`: exchange rates

One CSV file with a header, one row per currency (15).

| Column | Type | Meaning |
|---|---|---|
| `currency` | text | The currency's name, as in the transactions |
| `usdPerUnit` | number | US dollars per unit of that currency |

## `data/clean/elliptic/transactions`: Elliptic transactions

One row per transaction, 203,769 rows, in one folder per time step.

| Column | Type | Meaning |
|---|---|---|
| `txId` | whole number | The transaction's ID |
| `timeStep` | 1 to 49 | The time step (the folder the row is in) |
| `local_1` to `local_93` | number | Anonymized features of the transaction itself |
| `agg_1` to `agg_72` | number | Anonymized features aggregated over its neighbors |
| `label` | text | `illicit`, `licit` or `unknown` |
| `isIllicit` | 1, 0 or empty | 1 for illicit, 0 for licit, empty when unknown |

## `data/clean/elliptic/edges`: payments between Elliptic transactions

One row per payment, 234,355 rows.

| Column | Type | Meaning |
|---|---|---|
| `src` | whole number | The transaction the money came from |
| `dst` | whole number | The transaction it went to |

## `data/clean/features`: transactions with account features

The columns of `data/clean/txns`, plus `senderAccount` and `receiverAccount` (bank and account number joined by a vertical bar), `hour` (whole hours since 1 January 1970, in UTC), and 20 features describing the 24 whole hours before the transaction's hour. Each of these 10 appears twice, as `sender...` and `receiver...`:

| Feature | Meaning |
|---|---|
| `Sent24hCount`, `Sent24hUsd`, `Sent24hMaxUsd` | Payments sent: how many, their total and the largest, in US dollars |
| `Sent24hParties` | How many different accounts it paid |
| `Recv24hCount`, `Recv24hUsd`, `Recv24hMaxUsd` | Payments received: how many, their total and the largest |
| `Recv24hParties` | How many different accounts paid it |
| `Currencies24h`, `Formats24h` | How many different currencies and payment formats it used |

Part 2b adds `txnId`, a unique ID per transaction, and 9 more features:

| Feature | Meaning | With no history |
|---|---|---|
| `isCrossCurrency`, `isSelfTransfer`, `isSameBank` | 1 if paid and received in different currencies, between the same account, or within the same bank | — |
| `hourOfDay` | The hour of the day, 0-23, in UTC | — |
| `isJustUnder10k` | 1 if the payment is between $9,000 and $9,999.99 | — |
| `pairPayments7d` | Payments from the same sender to the same receiver in the 168 whole hours before | 0 |
| `senderMinutesSinceReceived` | Minutes since the sender last received money, before this minute | 10,080 |
| `senderPassThrough24h` | (Sent in the last 24 hours + this payment) / received in the last 24 hours | -1 |
| `amountVsSenderAverage24h` | This payment / the sender's average payment in the last 24 hours | -1 |

## `data/clean/features_graph`: transactions with all features

The columns of `data/clean/features`, plus 5 graph features about the 7 days (168 whole hours) before the transaction's hour:

| Feature | Meaning |
|---|---|
| `reciprocal7d` | Payments from the receiver back to the sender |
| `cyclePaths7d` | Accounts x where the receiver paid x, then x paid the sender, so this payment closes a 3-step loop |
| `scatterGatherPaths7d` | Other go-betweens x: some account paid both the sender and x, then x paid the receiver. 0 when the sender or receiver is a hub |
| `isBitcoin` | 1 if the payment is in Bitcoin |
| `senderRecvOtherRail24h` | Payments the sender received in the last 24 hours through the other rail (bank if this payment is in Bitcoin, and Bitcoin if not) |
