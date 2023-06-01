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
