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
