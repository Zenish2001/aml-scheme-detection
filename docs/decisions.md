# Design decisions

Each decision, why it was made, and the evidence behind it. Newest last.

## 1. Three modules: common, batch and monitor

**Decision:** Spark code (`batch`) and Akka code (`monitor`) live in separate sbt modules, with shared code in `common`.
**Why:** Spark and Akka each bring many libraries, and some of their versions clash. Separate modules keep each one's dependencies apart.

## 2. Read the IBM file with an explicit schema

**Decision:** `Ingest` gives Spark our own column names and types instead of reading them from the header. Bank and account IDs stay text.
**Why:** The header has two columns both called "Account". Many IDs start with zeros, and bank `021174` is not the number 21174.
**Evidence:** 5,078,345 rows read, 0 rejected.

## 3. Keep bad rows, and count them

**Decision:** A value that doesn't fit its type becomes null instead of stopping the job, and `Ingest.isValid` marks rows where everything parsed.
**Why:** Rejected rows can then be counted and reported, rather than silently dropped or crashing the run.

## 4. Save cleaned data as Parquet, one folder per day

**Decision:** The cleaned data is saved in Parquet format, partitioned by day (Elliptic: by time step).
**Why:** Spark reads Parquet much faster than CSV, and a single day can be read on its own for quick experiments.

## 5. Attach schemes by matching the patterns file to the transactions

**Decision:** Each transaction listed in `HI-Small_Patterns.txt` is matched to the same transaction in the main file on seven columns (time, both banks, both accounts, amount paid and its currency). Laundering transactions that no attempt lists get the scheme `UNASSIGNED`.
**Evidence:** All 3,209 listed transactions match; matched = listed for all 8 schemes; 1,968 laundering transactions are unassigned.

## 6. Guard against the Sep 11–18 tail

**Decision:** Models will not use date-based features, and results will be reported with and without Sep 11–18.
**Why:** Those days are 59% laundering (655 of 1,108 transactions), against 0.1% overall, because long schemes finish after the main 10 days. A model could score well just by learning that late transactions are laundering.

## 7. Read exchange rates from the data, not from the real world

**Decision:** Rates come from the data's own cross-currency transactions: amount received / amount paid.
**Why:** The simulator uses its own fixed rates. Every euro-to-dollar payment uses 1 EUR = 1.1718 USD, while the real rate in September 2022 was about 1.00, so real rates would make the paid and received amounts of one transaction disagree by about 17%. The simulator's rates match real ones from around August–September 2020.
**Evidence:** For almost every currency pair without Bitcoin, 90% of rates lie within 0.1% of each other.

## 8. Use total received / total paid, not the median rate

**Decision:** Each pair's rate is its total amount received divided by its total amount paid.
**Why:** Amounts are rounded to the cent, or to 6 decimals for Bitcoin, which badly distorts the rate of a tiny payment: the US Dollar to Bitcoin pair's rates spread 39.6%. Adding up the amounts first lets large payments dominate.
**Evidence:** Bitcoin = $11,882; going through Euro, UK Pound, Swiss Franc or Canadian Dollar instead gives $11,880–11,883.

## 9. One place to start Spark on a laptop

**Decision:** `LocalSpark.session` starts every Spark session, with both driver addresses set to 127.0.0.1.
**Why:** On a Mac, the hostname can resolve to 127.0.0.1 while Spark advertises the Wi-Fi address, and then Spark can't reach itself, so tests fail with "Connection refused".

## 10. Keep Elliptic's unknown labels

**Decision:** Elliptic transactions keep the label `unknown` (77% of them), with `isIllicit` left empty, rather than being dropped.
**Why:** They're still part of the payment graph, which graph features need, even though models can only learn from the labelled ones.

## 11. Account features from hourly totals, counting only whole hours before

**Decision:** Each account's activity is first added up per hour, and its 24-hour features add up the 24 hourly rows before the transaction's own hour.
**Why:** Comparing every payment with every other payment of the same account is far too slow for accounts with thousands of payments a day, while hourly rows cap the look-back at 24 rows. Payments earlier in the same hour aren't counted: times are only given to the minute, and counting a later payment would leak the future.
**Evidence:** For 1,053 sampled transactions, a brute-force count matches the feature exactly.

## 12. Read times as UTC

**Decision:** Spark reads the data's times as UTC.
**Why:** The times have no time zone. Read in a zone like India's (UTC + 5:30), every hour would be split differently, so hourly features would change from one computer to another.

## 13. Fixed values when there's no history

**Decision:** A feature with nothing to look at takes a fixed value instead of being empty: -1 for a ratio, a week (10,080 minutes) for a time gap, and 0 for a count.
**Why:** Models need a value in every column, and a fixed value that can't otherwise occur lets them treat "no history" as its own case.
**Note:** Early in the data, every account has little history, so results should also be checked on later days alone.

## 14. Compare with medians, not averages

**Decision:** Comparisons between laundering and normal transactions use medians and the share above zero.
**Why:** A few hub accounts make thousands of payments a day. A normal sender averaged 845 payments in the previous 24 hours, while the median is 3, so averages describe the hubs rather than a typical account.
