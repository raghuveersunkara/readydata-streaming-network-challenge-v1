# Review guide

A quick start for reviewing the ReadyData streaming challenge solution. All the
solution code and docs are under [`solution/`](solution/). The challenge brief
is in [`README.md`](README.md).

## Running the solution

Need docker with 8GB of memory

```bash
git clone https://github.com/raghuveersunkara/readydata-streaming-network-challenge-v1.git
cd readydata-streaming-network-challenge-v1
./solution/run.sh up        # executes everything, all services
./solution/run.sh test      # runs full test suite
```

It needs about 10 minutes to collect data, after that

```bash
./solution/airflow/run.sh quality                                   # data quality checks
./solution/sql/run.sh all                                           # 10 sql reports
./solution/airflow/run.sh export 01_accepted_event_totals.sql       # CSV into solution/exports/
```

To stop, run `./solution/run.sh down`. That keeps the data.

## Best place to start reviewing (ideally in that order)
- [`solution/AI_USAGE.md`](solution/AI_USAGE.md): Details of how AI is used in implementing this challenge
- [`solution/BUILD_BRIEF.md`](solution/BUILD_BRIEF.md): Complete build prompt that drives spec driven development for this solution. It describes what to build as sub projects and phases, and it is detailed enough for an AI coding assistant to produce the specs and then the code from it on phase at a time. 

- [`solution/SPECS.md`](solution/SPECS.md): each module has a spec of
  numbered requirements, each linked to the test that proves it, so you can
  review behaviour without reading all the code. Each spec is manually approved before implementing.
- [`solution/README.md`](solution/README.md): architecture, failure handling
  and a full demonstration checklist
- [`solution/TRADEOFFS.md`](solution/TRADEOFFS.md): design choices,
  assumptions, and how it would evolve for production systems