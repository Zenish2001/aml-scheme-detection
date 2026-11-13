# AML Scheme Detection

**A real-time transaction monitoring system for anti-money-laundering compliance, in Scala.**
A personal project by Zenish Borad, built with techniques from Northeastern’s CSYE 7200 (Big-Data Systems Engineering Using Scala)

![CI](https://github.com/Zenish2001/aml-scheme-detection/actions/workflows/ci.yml/badge.svg)

The system detects networks of bank accounts laundering money, names which of eight known schemes they use, follows money between bank payments and Bitcoin, and scores new transactions live, grouping alerts into cases with SAR-style summaries.

## Architecture

| Module | What it does | Built with |
|---|---|---|
| `common` | The rules DSL and the model-weights format, shared by both paths | Scala parser combinators |
| `batch` | Ingestion and validation, account and graph features, stage 1 and stage 2 models | Spark 4.0.1 (Spark SQL, MLlib, GraphX), GraphFrames, TableParser |
| `monitor` | Live monitor: replayer, scorer, rule engine, case builder | Akka 2.8.8 typed actors and Streams |

## Getting started

Requirements: JDK 17 and sbt.

```
sbt test                  # compile and test every module
sbt "batch/run"           # run the batch path
sbt "monitor/run"         # run the live monitor
```

Download the data first: see [`data/README.md`](data/README.md).

## Project plan

| Part | What | Status |
|---|---|---|
| 0 | Setup: repo, build, CI | ⬜ |
| 1 | IBM data, exchange rates and Elliptic, loaded and validated | ⬜ |
| 2 | Account features | ⬜ |
| 3 | Graph and cross-rail features | ⬜ |
| 4 | Stage 1, rules DSL and reason codes | ⬜ |
| 5 | Stage 2: which of 8 schemes | ⬜ |
| 6 | Cross-rail analysis and the Elliptic real-data check | ⬜ |
| 7 | Real-time monitor and SAR-style cases | ⬜ |
| 8 | Stretch: HI-Medium and Elliptic2 | ⬜ |
| 9 | Write-up and demo | ⬜ |

Progress is tracked in [Issues](../../issues) and [Milestones](../../milestones).

## Acceptance criteria

| Criterion | Target | Result |
|---|---|---|
| Live-monitor latency | 95% of transactions within 50 ms | |
| Alert precision | ≥ 10% of the top 100 daily alerts are laundering | |
| Batch throughput | 5,078,345 HI-Small transactions in < 20 min | |
| Model against rules | ≥ 2× the cases found at the same alert volume | |
| Analyst workload | ≥ 30% fewer review hours than the rules | |
| Real-data check | Graph features raise Elliptic PR-AUC by ≥ 10% | |

Design notes and results are in [`docs/`](docs/).

## How we work

See [CONTRIBUTING.md](CONTRIBUTING.md): one branch and one pull request per task, CI on every pull request, and a tagged release at each milestone. Sources are listed in [ATTRIBUTION.md](ATTRIBUTION.md).
