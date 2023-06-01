# Tool to migrate data directly from ClickHouse to PostHog Cloud. We do this to
# maximize the compatability with existing self-hosted installations, as well as
# speed. The command takes a ClickHouse host and credentials, the team_id for
# which we want to migrate events, and a PostHog host and API token that should
# be used to create the events.
#
# The script also takes a `--start-date` and `--end-date` parameter, which can be used to
# migrate a subset of the data. To aid in resuming a migration when something
# failes, we output the last cursor that was migrated to stdout. This can be
# used to resume the migration by setting `--start-date` to the last cursor
# that was migrated.
#
# We use the aiochclient and the iterator API to stream data from ClickHouse,
# and the PostHog Python client to send events to PostHog Cloud. We use the
# `batch` method to send events in batches of 1000, which is the maximum
# allowed by PostHog Cloud.

import argparse
import asyncio
import json
import logging
from datetime import datetime, timedelta
import sys
from typing import Any, List, Optional, Tuple

from aiochclient import ChClient

from posthog import Posthog

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_args(sys_args: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Migrate data from ClickHouse to PostHog Cloud. This tool is intended to be used for self-hosted instances of PostHog."
    )
    parser.add_argument(
        "--clickhouse-host",
        type=str,
        required=True,
        help="The host of the ClickHouse instance to migrate from.",
    )
    parser.add_argument(
        "--clickhouse-user",
        type=str,
        required=True,
        help="The user to use to connect to ClickHouse.",
    )
    parser.add_argument(
        "--clickhouse-password",
        type=str,
        required=True,
        help="The password to use to connect to ClickHouse.",
    )
    parser.add_argument(
        "--clickhouse-database",
        type=str,
        required=True,
        help="The database to use to connect to ClickHouse.",
    )
    parser.add_argument(
        "--team-id",
        type=int,
        required=True,
        help="The team ID to migrate events for.",
    )
    parser.add_argument(
        "--posthog-host",
        type=str,
        required=True,
        help="The host of the PostHog instance to migrate to.",
    )
    parser.add_argument(
        "--posthog-api-token",
        type=str,
        required=True,
        help="The API token to use to connect to PostHog.",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        help="The date to start migrating from. Defaults to 30 days ago.",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="The date to stop migrating at. Defaults to today.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="The number of events to batch together when sending to PostHog.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="If set, the script will not send any events to PostHog.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="If set, the script will output debug information.",
    )
    return parser.parse_args(sys_args)


def get_clickhouse_client(
    host: str, user: str, password: str, database: str
) -> ChClient:
    return ChClient(
        url=f"http://{host}:8123/", user=user, password=password, database=database
    )


def get_posthog_client(host: str, api_token: str) -> Posthog:
    return Posthog(api_key=api_token, host=host)


def get_start_date(start_date: Optional[str]) -> datetime:
    if start_date:
        return datetime.strptime(start_date, "%Y-%m-%d")
    return datetime.now() - timedelta(days=30)


def get_end_date(end_date: Optional[str]) -> datetime:
    if end_date:
        return datetime.strptime(end_date, "%Y-%m-%d")
    return datetime.now()


async def migrate_events(
    clickhouse_client: ChClient,
    posthog_client: Posthog,
    team_id: int,
    start_date: datetime,
    end_date: datetime,
    batch_size: int,
    dry_run: bool,
) -> Tuple[Any, int]:
    # We use the iterator API to stream data from ClickHouse. This is the
    # fastest way to get data out of ClickHouse, and it also allows us to
    # resume the migration if it fails.
    #
    # We use the `timestamp` column to resume the migration. This column is
    # indexed, so it's very fast to query.
    #
    # We use the `batch` method to send events to PostHog Cloud. This allows us
    # to send events in batches of 1000, which is the maximum allowed by
    # PostHog Cloud.
    #
    # We use the `dry_run` flag to test the migration without sending any
    # events to PostHog Cloud.
    #
    # We use the `verbose` flag to output debug information.
    #
    # We output the last timestamp that was migrated to stdout. This can be
    # used to resume the migration by setting `--start-date` to the last
    # timestamp that was migrated.
    #
    # We use the `batch_size` parameter to control the number of events to
    # batch together when sending to PostHog Cloud.
    #
    # We use the `start_date` and `end_date` parameters to control the range of
    # dates to migrate. This is useful to resume the migration if it fails.
    # This also allows us to migrate a subset of the data, which is useful for
    # testing.
    #
    # We use the `total_events` variable to keep track of the total number of
    # events that were migrated. This is useful to know how many events were
    # migrated in total.
    #
    # We use the entire sort key value to cursor the migration. The sort key may
    # not be unique if the migration is resumed, so we need to use the entire
    # sort key to ensure that we don't skip any events.

    # Stream events from ClickHouse. We use the sort key as the ordering to
    # ensure that ClickHouse is able to stream data in a memory efficient way.
    #
    # The events table in ClickHouse has the following schema:
    #
    # CREATE TABLE sharded_events (
    #     `uuid` UUID,
    #     `event` String,
    #     `properties` String CODEC(ZSTD(3)),
    #     `timestamp` DateTime64(6, 'UTC'),
    #     `team_id` Int64,
    #     `distinct_id` String,
    #     `elements_hash` String,
    #     `created_at` DateTime64(6, 'UTC'),
    #     `_timestamp` DateTime,
    #     `_offset` UInt64,
    # ) ENGINE = ReplicatedReplacingMergeTree(
    #     '/clickhouse/prod/tables/{shard}/posthog.sharded_events',
    #     '{replica}',
    #     _timestamp
    # ) PARTITION BY toYYYYMM(timestamp)
    # ORDER BY (
    #         team_id,
    #         toDate(timestamp),
    #         event,
    #         cityHash64(distinct_id),
    #         cityHash64(uuid)
    #     )
    #
    # The `events` table which we will query is a Disrtibuted table over the top
    # of the `sharded_events` table. This allows us to query all of the shards
    # in parallel.
    #
    # Periodically we call flush to ensure the data has been flushed to PostHog,
    # and only after this do we update the last cursor value.

    total_events = 0
    last_cursor = None

    async for record in clickhouse_client.iterate(
        """
        SELECT
            uuid,
            event,
            properties,
            timestamp,
            team_id,
            distinct_id,
            toDate(timestamp) as date,
            cityHash64(distinct_id) as distinct_id_hash,
            cityHash64(uuid) as uuid_hash
        FROM events
        WHERE
            team_id = {team_id}
            AND timestamp >= {start_date}
            AND timestamp < {end_date}
        ORDER BY
            team_id,
            toDate(timestamp),
            event,
            cityHash64(distinct_id),
            cityHash64(uuid)
        """,
        params={
            "team_id": team_id,
            "start_date": start_date,
            "end_date": end_date,
        },
    ):
        # Parse the event properties.
        properties = json.loads(record["properties"])

        # Send the event to PostHog Cloud.
        if not dry_run:
            posthog_client.capture(
                distinct_id=record["distinct_id"],
                event=record["event"],
                timestamp=record["timestamp"],
                properties=properties,
            )

        # Increment the total number of events.
        total_events += 1

        # Flush the events to PostHog Cloud.
        if total_events % batch_size == 0:
            if not dry_run:
                posthog_client.flush()

            # Update the last cursor value, which is the entire sort key.
            last_cursor = (
                record["team_id"],
                record["date"],
                record["event"],
                record["distinct_id_hash"],
                record["uuid_hash"],
            )

            # Output the last cursor value.
            print(last_cursor)

    # Flush the events to PostHog Cloud.
    if not dry_run:
        posthog_client.flush()

    # Return the last cursor value and the total number of events.
    return last_cursor, total_events


async def main(sys_args: Optional[List[str]] = None) -> None:
    """
    The main function.

    This function is called when the script is run from the command line.

    Args:
        sys_args: The command line arguments.
    """
    # Parse the command line arguments.
    args = parse_args(sys_args)

    # Set the logging level.
    if args.verbose:
        logger.setLevel(logging.DEBUG)

    # Get the start and end dates.
    start_date = get_start_date(args.start_date)
    end_date = get_end_date(args.end_date)

    # Get the ClickHouse and PostHog clients.
    clickhouse_client = get_clickhouse_client(
        host=args.clickhouse_host,
        user=args.clickhouse_user,
        password=args.clickhouse_password,
        database=args.clickhouse_database,
    )
    posthog_client = get_posthog_client(
        host=args.posthog_host, api_token=args.posthog_api_token
    )

    # Run the migration.
    last_cursor, total_events = await migrate_events(
        clickhouse_client=clickhouse_client,
        posthog_client=posthog_client,
        team_id=args.team_id,
        start_date=start_date,
        end_date=end_date,
        batch_size=args.batch_size,
        dry_run=args.dry_run,
    )

    # Output the last timestamp and total events to stdout.
    print(json.dumps({"last_cursor": last_cursor, "total_events": total_events}))


if __name__ == "__main__":
    # Run an asyncio event loop to run the migration.
    asyncio.run(main(sys.argv[1:]))
