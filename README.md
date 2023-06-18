# PostHog self hosted migration tools

## TL;DR

We provide a bulk export functionality to allow users to migrate from
self-hosted to Cloud deployments using e.g. the Replicator App. This can be
slow and unreliable.

Git clone this repo and run something like:

```bash
pip3 install -r requirements.txt
python3 ./migrate.py \
   --clickhouse-url https://some.clickhouse.cluster:8443 \
   --clickhouse-user default \
   --clickhouse-password some-password \
   --clickhouse-database posthog \
   --team-id 1234 \
   --posthog-url https://app.posthog.com \
   --posthog-api-token "abx123" \
   --start-date 2023-06-18T13:00:00Z \
   --end-date 2023-06-18T13:10:00 \
   --fetch-limit 10000
```

The script prints out a "cursor" that, in the case that the migration fails, can
be used to resume from where if got too previously. That would look like:

```bash
python3 ./migrate.py \
   --clickhouse-url https://some.clickhouse.cluster:8443 \
   --clickhouse-user default \
   --clickhouse-password some-password \
   --clickhouse-database posthog \
   --team-id 1234 \
   --posthog-url https://app.posthog.com \
   --posthog-api-token "abx123" \
   --start-date 2023-06-18T13:00:00Z \
   --end-date 2023-06-18T13:10:00 \
   --fetch-limit 10000 \
   --cursor the-cursor-value-from-the-output
```

Run `python3 ./migrate.py --help` to get a complete list of options.

## What it does

To aid in getting people moved over, this tool:

 1. reads event data directly from ClickHouse.
 1. uses the PostHog Python library to ingest the data into PostHog cloud.

Why pull directly from ClickHouse? Mainly, it removes the requirement to have a
working installation of PostHog down to just needing to have ClickHouse
responsive. It will also help with performance.

NOTE: this script will add a $lib = posthog-python property, overriding anything
else that was already there.

# Caveats

## Memory usage isn't optimal

How do we efficiently handle ingesting in time order. The sort key for the
`events` table in ClickHouse concatenates the event `timestamp` to a day so
we cannot order efficiently by timestamp. I'm not sure on the implications of
e.g. if we decide to order by `timestamp` instead of `toDate(timestamp)` on
memory usage. It might be that it manages to, as it's streaming through only
need to order the events in a day at any one time thus making it scale with
date range increases not _so_ badly although if you have loads of data within
one day it could still be an issue. If it turns out it doesn't scale so well,
you can always run this multiple times with different incrementing day
ranges.

To avoid migrations scaling too badly we end up doing multiple queries using
`LIMIT` to keep the memory usage down. Assuming there's not too many rows in a
single day for which the timestamp, uuid cursor needs to be calculated it should
be ok.

Note that increasing `--fetch-limit` may reduce the load on the cluster due to
the reduced duplication of query execution, assuming you've got enough memory.

## Performance isn't as good as it should be

Due to the above limitation around time ordering, we need to perform some
expensive queries which do not support streaming and as such it is not as
performant as I would like. It's possible that we could either:

1. write the data to a new table with a refined sort key, although this risks
   e.g. disk space issues.
2. offer a way to drop the time ordering requirement. Perhaps some do not need
   to worry about this.
3. identify a subset of events that need to be in time order and do these first,
   then do the rest. This will however break point in time person properties
   that are denormalized onto events at ingestion time.
