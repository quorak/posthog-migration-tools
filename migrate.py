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
import base64
import contextlib
import dataclasses
import json
import logging
import re
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, List, Optional, Tuple
from uuid import UUID

import aiohttp
from aiochclient import ChClient
from posthog import Posthog

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def parse_args(sys_args: Optional[List[str]] = None) -> argparse.Namespace:
    """
    Parse command line arguments.

    :param sys_args: The arguments to parse. Defaults to `sys.argv`.
    :return: The parsed arguments.
    """
    parser = argparse.ArgumentParser(
        description="Migrate data from ClickHouse to PostHog Cloud. This tool is intended to be used for self-hosted instances of PostHog."
    )
    parser.add_argument(
        "--clickhouse-url",
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
        "--posthog-url",
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
        "--cursor",
        type=str,
        help="The cursor to start migrating from. If provided, this will override the start date.",
    )
    parser.add_argument(
        "--fetch-limit",
        type=int,
        default=1000,
        help="The number of events to fetch from ClickHouse at a time.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
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
    return parser.parse_args(sys_args or sys.argv[1:])


@contextlib.asynccontextmanager
async def get_clickhouse_client(url: str, user: str, password: str, database: str):
    """
    Get a ClickHouse client. This client is used to stream events from
    ClickHouse.

    :param url: The host of the ClickHouse instance to migrate from.
    :param user: The user to use to connect to ClickHouse.
    :param password: The password to use to connect to ClickHouse.
    :param database: The database to use to connect to ClickHouse.
    :return: A ClickHouse client.
    """
    connector = aiohttp.TCPConnector(
        # Without this I was getting the error: "ServerDisconnectedError" when
        # running without `--dry-run`. With `--dry-run` it worked fine, I'm not
        # sure why.
        force_close=True
    )
    async with aiohttp.ClientSession(connector=connector) as session:
        client = ChClient(
            session=session,
            url=url,
            user=user,
            password=password,
            database=database,
            compress_response=True,
        )
        yield client
        await client.close()


def get_posthog_client(url: str, api_token: str, debug: bool) -> Posthog:
    """
    Get a PostHog client. This client is used to send events to PostHog Cloud.

    :param url: The url of the PostHog instance to connect to.
    :param api_token: The API token to use to connect to PostHog.
    :param debug: Whether to log payloads to stdout
    :return: A PostHog client.
    """
    return Posthog(api_key=api_token, host=url, debug=debug)


def get_start_date(start_date: Optional[str]) -> datetime:
    """
    Get the start date to migrate from. If no start date is provided, we default
    to 30 days ago. We parse as UTC.

    :param start_date: The start date to migrate from.
    :return: The start date to migrate from.
    """
    if start_date:
        return datetime.fromisoformat(start_date).replace(tzinfo=None)
    return datetime.now() - timedelta(days=30)


def get_end_date(end_date: Optional[str]) -> datetime:
    """
    Get the end date to migrate to. If no end date is provided, we default to
    today.

    :param end_date: The end date to migrate to.
    :return: The end date to migrate to.
    """
    if end_date:
        return datetime.fromisoformat(end_date).replace(tzinfo=None)
    return datetime.now()


@dataclasses.dataclass
class Cursor:
    """
    A cursor is used to resume a migration. It contains the timestamp, event,
    and distinct ID hash of the last event that was migrated.

    NOTE: this looks a little overkill to have this class. It is. We originally
    had a more complex cursor that included all columns of the events table sort
    key.
    """

    timestamp: int
    uuid: UUID


def marshal_cursor(cursor: Cursor) -> str:
    """
    Convert the cursor to a string. This string can be used to resume a
    migration. We encode dates as ISO 8601 strings.

    :return: The cursor as a string.
    """
    return base64.b64encode(
        json.dumps(
            {
                "timestamp": cursor.timestamp,
                "uuid": int(cursor.uuid),
            }
        ).encode()
    ).decode()


def unmarshal_cursor(cursor: str) -> "Cursor":
    """
    Convert a cursor string to a cursor. We decode dates from ISO 8601 strings.

    :param cursor: The cursor as a string.
    :return: The cursor.
    """
    decoded = base64.b64decode(cursor.encode())
    json_data = json.loads(decoded)
    return Cursor(
        timestamp=json_data["timestamp"],
        uuid=UUID(int=json_data["uuid"]),
    )


def elements_chain_to_elements(elements_chain: str) -> list[dict]:
    """
    Parses the elements_chain string into a list of objects that will be correctly parsed
    by ingestion. The output format is built by observing how it is read by
    https://github.com/PostHog/posthog/blob/master/plugin-server/src/utils/db/elements-chain.ts
    """
    elements = []

    split_chain_regex = re.compile(r'(?:[^\s;"]|"(?:\\.|[^"])*")+')
    split_class_attributes_regex = re.compile(
        r"(.*?)($|:([a-zA-Z\-_0-9]*=.*))", flags=re.MULTILINE
    )
    parse_attributes_regex = re.compile(r'((.*?)="(.*?[^\\])")')

    elements_chain = elements_chain.replace("\n", "")

    for match in re.finditer(split_chain_regex, elements_chain):
        class_attributes = re.search(split_class_attributes_regex, match.group(0))

        attributes = {}
        if class_attributes is not None:
            try:
                attributes = {
                    m[2]: m[3]
                    for m in re.finditer(
                        parse_attributes_regex, class_attributes.group(3)
                    )
                }
            except IndexError:
                pass

        element = {}

        if class_attributes is not None:
            try:
                tag_and_class = class_attributes.group(1).split(".")
            except IndexError:
                pass
            else:
                element["tag_name"] = tag_and_class.pop(0)
                if len(tag_and_class) > 0:
                    element["attr__class"] = tag_and_class

        for key, value in attributes.items():
            match key:
                case "href":
                    element["attr__href"] = value
                case "nth-child":
                    element["nth_child"] = int(value)
                case "nth-of-type":
                    element["nth_of_type"] = int(value)
                case "text":
                    element["$el_text"] = value
                case "attr_id":
                    element["attr__id"] = value
                case k:
                    element[k] = value

        elements.append(element)

    return elements


async def migrate_events(
    clickhouse_client: ChClient,
    posthog_client: Posthog,
    team_id: int,
    start_date: datetime,
    end_date: datetime,
    cursor: Optional[Cursor],
    fetch_limit: int,
    batch_size: int,
    dry_run: bool,
) -> Tuple[Any, int]:
    """
    We use the iterator API to stream data from ClickHouse. This is the
    fastest way to get data out of ClickHouse, and it also allows us to
    resume the migration if it fails.

    We use the `timestamp` column to resume the migration. This column is
    indexed, so it's very fast to query.

    We use the `batch` method to send events to PostHog Cloud. This allows us
    to send events in batches of 1000, which is the maximum allowed by
    PostHog Cloud.

    We use the `dry_run` flag to test the migration without sending any
    events to PostHog Cloud.

    We use the `verbose` flag to output debug information.

    We output the last timestamp that was migrated to stdout. This can be
    used to resume the migration by setting `--start-date` to the last
    timestamp that was migrated.

    We use the `batch_size` parameter to control the number of events to
    batch together when sending to PostHog Cloud.

    We use the `start_date` and `end_date` parameters to control the range of
    dates to migrate. This is useful to resume the migration if it fails.
    This also allows us to migrate a subset of the data, which is useful for
    testing.

    We use the `total_events` variable to keep track of the total number of
    events that were migrated. This is useful to know how many events were
    migrated in total.

    We use the entire sort key value to cursor the migration. The sort key may
    not be unique if the migration is resumed, so we need to use the entire
    sort key to ensure that we don't skip any events.

    Stream events from ClickHouse. We use the sort key as the ordering to
    ensure that ClickHouse is able to stream data in a memory efficient way.

    The events table in ClickHouse has the following schema:

    CREATE TABLE sharded_events (
        `uuid` UUID,
        `event` String,
        `properties` String CODEC(ZSTD(3)),
        `timestamp` DateTime64(6, 'UTC'),
        `team_id` Int64,
        `distinct_id` String,
        `elements_hash` String,
        `created_at` DateTime64(6, 'UTC'),
        `_timestamp` DateTime,
        `_offset` UInt64,
    ) ENGINE = ReplicatedReplacingMergeTree(
        '/clickhouse/prod/tables/{shard}/posthog.sharded_events',
        '{replica}',
        _timestamp
    ) PARTITION BY toYYYYMM(timestamp)
    ORDER BY (
            team_id,
            toDate(timestamp),
            event,
            cityHash64(distinct_id),
            cityHash64(uuid)
        )

    The `events` table which we will query is a Disrtibuted table over the top
    of the `sharded_events` table. This allows us to query all of the shards
    in parallel.

    Periodically we call flush to ensure the data has been flushed to PostHog,
    and only after this do we update the last cursor value.

    :param clickhouse_client: The ClickHouse client to use to query events.
    :param posthog_client: The PostHog client to use to send events.
    :param team_id: The team ID to migrate events for.
    :param start_date: The start date to migrate from.
    :param end_date: The end date to migrate to.
    :param cursor: The cursor to resume the migration from.
    :param batch_size: The number of events to batch together when sending to
        PostHog Cloud.
    :param dry_run: Whether to run the migration in dry run mode.
    :return: The last cursor value and the total number of events migrated. The
        cursor value can be used to resume the migration.
    """

    total_events = 0
    records = []
    committed_cursor = cursor

    while True:
        # If we have a cursor, then we need to resume the migration from the
        # cursor value.
        results_range_query = f"""
            SELECT
                uuid,
                event,
                properties,
                toInt64(timestamp) AS timestamp,
                team_id,
                distinct_id,
                elements_chain

            FROM events

            -- Filter by team ID and date range. We use timestamp to filter by, as
            -- this is part of the sort key or at least toDate(timestamp) is it
            -- should be reasonably efficient.
            WHERE
                team_id = {team_id}
                AND timestamp >= toDateTime64({int(start_date.timestamp())}, 6)
                AND timestamp < toDateTime64({int(end_date.timestamp())}, 6)

                -- Use >= timestamp to ensure we don't miss any events.
                -- Use the (mostly probably) unique uuid property to ensure that we
                -- don't duplicate events.
                AND (timestamp, uuid) > (
                    toDateTime64({(int(cursor.timestamp) if cursor else 0)}, 6),
                    toUUID('{str(cursor.uuid) if cursor else '00000000-0000-0000-0000-000000000000'}')
                )

            -- Order by timestamp to ensure we ingest the data in timestamp order.
            -- This is important as the order of ingestion affects how person
            -- properties etc are created.
            --
            -- Ideally we should order by the sort key to ensure that ClickHouse can
            -- stream data in a memory efficient way, but this doesn't include the
            -- timestamp but instead `toDate(timestamp)`. We could reindex the table
            -- but for the purposes of this tool, we do not want to introduce too
            -- many changes to the production database. e.g. if we tried to index we
            -- might run out of disk space.
            --
            -- We also include the uuid. This isn't in the sort key either, so this
            -- _could_ also be very inefficient.
            ORDER BY
                timestamp,
                uuid
        """

        number_of_events_query = f"""
            SELECT
                count(*)

            FROM ({results_range_query})
        """

        # Query the number of events to migrate.
        logger.debug(
            "Querying number of events from ClickHouse: %s", number_of_events_query
        )
        number_of_events_result = await clickhouse_client.fetchrow(
            number_of_events_query
        )
        assert number_of_events_result is not None
        number_of_events = number_of_events_result[0]

        logger.info("Migrating %s events", number_of_events)

        results_batch_query = f"""
            SELECT *
            FROM ({results_range_query})

            -- As we cannot use the sort key to time order the data and thus take
            -- advantage of streaming, we need to limit the amount of data ClickHouse
            -- will need to store in memory. It's not the most efficient way to do
            -- this, but it's the best we can do without reindexing the table or
            -- similar.
            LIMIT {fetch_limit}
        """

        logger.debug("Querying events from ClickHouse: %s", results_batch_query)
        records = await clickhouse_client.fetch(results_batch_query)

        if not records:
            break

        for record in records:
            # Parse the event properties.
            properties = json.loads(record["properties"])

            if (
                record["event"] == "$autocapture"
                and record.get("elements_chain", None) is not None
            ):
                properties["$elements"] = elements_chain_to_elements(record["elements_chain"])

            # Send the event to PostHog Cloud.
            if not dry_run:
                posthog_client.capture(
                    distinct_id=record["distinct_id"],
                    event=record["event"],
                    timestamp=datetime.fromtimestamp(record["timestamp"], tz=timezone.utc),
                    properties=properties,
                )

            cursor = Cursor(timestamp=record["timestamp"], uuid=record["uuid"])

            # Increment the total number of events.
            total_events += 1

            # Flush the events to PostHog Cloud.
            if total_events % batch_size == 0:
                if not dry_run:
                    posthog_client.flush()

                committed_cursor = cursor
                print("Cursor: ", marshal_cursor(committed_cursor))

    # Flush the events to PostHog Cloud.
    if not dry_run:
        posthog_client.flush()

    committed_cursor = cursor
    print("Cursor: ", marshal_cursor(committed_cursor) if committed_cursor else None)

    # Return the last cursor value and the total number of events.
    return committed_cursor, total_events


async def main(sys_args: Optional[List[str]] = None) -> Tuple[Optional[str], int]:
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
    async with get_clickhouse_client(
        url=args.clickhouse_url,
        user=args.clickhouse_user,
        password=args.clickhouse_password,
        database=args.clickhouse_database,
    ) as clickhouse_client:
        posthog_client = get_posthog_client(
            url=args.posthog_url, api_token=args.posthog_api_token, debug=args.verbose
        )

        # Run the migration.
        committed_cursor, total_events = await migrate_events(
            clickhouse_client=clickhouse_client,
            posthog_client=posthog_client,
            team_id=args.team_id,
            start_date=start_date,
            end_date=end_date,
            cursor=unmarshal_cursor(args.cursor) if args.cursor else None,
            fetch_limit=args.fetch_limit,
            batch_size=args.batch_size,
            dry_run=args.dry_run,
        )

    # Output the last timestamp and total events to stdout. We want to convert
    # dates to their isoformat representation, as this is easier to parse.
    print(
        json.dumps(
            {
                "committed_cursor": dataclasses.asdict(committed_cursor)
                if committed_cursor
                else None,
                "total_events": total_events,
            },
            default=lambda o: o.isoformat() if isinstance(o, datetime) else None,
            indent=4,
        )
    )
    return marshal_cursor(committed_cursor) if committed_cursor else None, total_events


if __name__ == "__main__":
    # Run an asyncio event loop to run the migration.
    asyncio.run(main(sys.argv[1:]))
