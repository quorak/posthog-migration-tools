# PostHog self hosted migration tools

We provide a bulk export functionality to allow users to migrate from
self-hosted to Cloud deployments using e.g. the Replicator App. This can be
slow and unreliable.

To aid in getting people moved over, this tool:

 1. reads event data directly from ClickHouse and writes them to file.
 1. uses the PostHog Python library to ingest the data into PostHog cloud.

Why pull directly from ClickHouse? Mainly, it removes the requirement to have a
working installation of PostHog down to just needing to have ClickHouse
responsive. It will also help with performance.

NOTE: this script will add a $lib = posthog-python property, overriding anything
else that was already there.

# Open questions

1. How do we efficiently handle ingesting in time order. The sort key for the
   `events` table in ClickHouse concatenates the event `timestamp` to a day so
   we cannot order efficiently by timestamp. I'm not sure on the implications of
   e.g. if we decide to order by `timestamp` instead of `toDate(timestamp)` on
   memory usage. It might be that it manages to, as it's streaming through only
   need to order the events in a day at any one time thus making it scale with
   date range increases not _so_ badly although if you have loads of data within
   one day it could still be an issue. If it turns out it doesn't scale so well,
   you can always run this multiple times with different incrementing day
   ranges.
