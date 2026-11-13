#!/usr/bin/env bash
# One-time setup of labels, milestones and one issue per part, using the GitHub CLI (gh).
# Run from the repo folder after `gh auth login`:   bash scripts/setup-github.sh
# Edit the due dates first if the schedule has moved.
set -euo pipefail

ZENISH="Zenish2001"

assign() { if [ -n "$1" ]; then echo "--assignee $1"; fi; }

echo "Creating labels…"
for l in "data:0e8a16" "features:1d76db" "models:5319e7" "dsl:fbca04" "monitor:d93f0b" "docs:c5def5" "stretch:ededed"; do
  gh label create "${l%%:*}" --color "${l##*:}" --force >/dev/null
done

echo "Creating milestones…"
milestone() { gh api "repos/{owner}/{repo}/milestones" -f title="$1" -f due_on="$2T23:59:59Z" >/dev/null || true; }
milestone "M1: Data ingested"            "2026-11-22"
milestone "M2: Features"                 "2026-12-06"
milestone "M3: Stage 1, DSL, reasons"    "2026-12-20"
milestone "M4: Stage 2 and real data"    "2027-01-03"
milestone "M5: Live monitor"             "2027-01-17"
milestone "M6: Write-up and demo"    "2027-01-31"

echo "Creating issues…"
issue() { gh issue create --title "$1" --body "$2" --label "$3" --milestone "$4" $(assign "$5") >/dev/null; echo "  $1"; }
issue "Part 0: Repo, build and CI" "Three-module build (common, batch, monitor), CI green." "docs" "M1: Data ingested" "$ZENISH"
issue "Part 1a: Load IBM transactions and scheme labels" "TableParser for the transactions, parse the patterns file, join scheme labels. Done when loaded + rejected rows = 5,078,345 and scheme counts match the paper." "data" "M1: Data ingested" "$ZENISH"
issue "Part 1b: Exchange rates and USD conversion" "Parse daily rates and BTC-USD with TableParser, add amountUsd to every transaction." "data" "M1: Data ingested" "$ZENISH"
issue "Part 1c: Load the Elliptic dataset" "Features, classes and edges; check the published counts (203,769 transactions, 234,355 edges)." "data" "M1: Data ingested" "$ZENISH"
issue "Part 2: Account features" "Past-24-hour features with time-bounded windows, each with a spec on hand-made rows." "features" "M2: Features" "$ZENISH"
issue "Part 3: Graph and cross-rail features" "Fan-out, fan-in, cycles, scatter-gather, rail changes, Bitcoin hops, each with a hand-drawn graph in its spec." "features" "M2: Features" "$ZENISH"
issue "Part 4a: Stage 1 model, alert budget and rule comparison" "GBT vs logistic regression, precision at k = 50/100/200, rules at the same alert volume, weights export." "models" "M3: Stage 1, DSL, reasons" "$ZENISH"
issue "Part 4b: Rules DSL with parser combinators" "Parse rules.txt into case classes; errors as values (Either); spec for every rule form and a malformed line." "dsl" "M3: Stage 1, DSL, reasons" "$ZENISH"
issue "Part 4c: Reason codes" "Every alert lists why it was flagged, with a spec for each reason." "models" "M3: Stage 1, DSL, reasons" "$ZENISH"
issue "Part 5: Stage 2, which of 8 schemes" "Random forest vs baselines, per-scheme F1, confusion matrix, both stages together." "models" "M4: Stage 2 and real data" "$ZENISH"
issue "Part 6: Cross-rail analysis and Elliptic check" "Recall with and without a Bitcoin hop; graph features on Elliptic's later time steps (+10% PR-AUC target)." "models" "M4: Stage 2 and real data" "$ZENISH"
issue "Part 7: Real-time monitor and SAR-style cases" "Akka Streams replayer and typed actors; 95th-percentile latency; scores match batch; typed-testkit specs." "monitor" "M5: Live monitor" "$ZENISH"
issue "Part 9: Write-up and demo" "README results table, a short design write-up, and a recorded demo for recruiters." "docs" "M6: Write-up and demo" "$ZENISH"
echo "Done."
