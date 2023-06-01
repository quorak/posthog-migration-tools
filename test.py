from migrate import main
from aiochclient import ChClient
import responses
import pytest
import datetime


@pytest.mark.asyncio
async def test_can_migrate_events_to_posthog():
    """
    Here we setup ClickHouse with the sharded_events and events tables, add some
    events to the sharded_events table, and then run the migration script to
    migrate those events to PostHog. We do not actually migrate the events, but
    rather mock out the requests library with the responses library to ensure
    that the correct requests are made to PostHog.
    """
    async with ChClient() as client:
        await setup_clickhouse_schema(client)

        # Add some events to the sharded_events table
        team_id = 1
        await client.execute(
            """
            INSERT INTO test.events
            VALUES
            """,
            (
                "00000000-0000-0000-0000-000000000000",
                "test",
                '{"$browser":"Chrome","$browser_version":"90.0.4430.93","$current_url":"http://localhost:8000/","$device_id":"00000000-0000-0000-0000-000000000000","$device_type":"desktop","$initial_referrer":"http://localhost:8000/","$initial_referring_domain":"localhost","$lib":"web","$lib_version":"1.24.0","$os":"Mac OS X","$os_version":"10.15.7","$referrer":"http://localhost:8000/","$referring_domain":"localhost","$screen_height":900,"$screen_width":1440,"$user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) PostHog/1.24.0 Chrome/90.0.4430.93 Electron/12.0.7 Safari/537.36","distinct_id":"00000000-0000-0000-0000-000000000000","elements":[{"attr__class":"ant-btn","attr__href":"/signup","attr__role":"button","attr__title":"Sign up","tag_name":"a","text":"Sign up"}],"event":"$pageview","properties":{"$current_url":"http://localhost:8000/","$initial_referrer":"http://localhost:8000/","$initial_referring_domain":"localhost","$referrer":"http://localhost:8000/","$referring_domain":"localhost"},"timestamp":"2021-05-05T16:00:00.000000Z","type":"$autocapture"}',
                datetime.datetime(2021, 5, 5, 16, 0, 0),
                team_id,
                "00000000-0000-0000-0000-000000000000",
                datetime.datetime(2021, 5, 5, 16, 0, 0),
                datetime.datetime(2021, 5, 5, 16, 0, 0),
                0,
            ),
            (
                "00000000-0000-0000-0000-000000000001",
                "test",
                '{"$browser":"Chrome","$browser_version":"90.0.4430.93","$current_url":"http://localhost:8000/","$device_id":"00000000-0000-0000-0000-000000000000","$device_type":"desktop","$initial_referrer":"http://localhost:8000/","$initial_referring_domain":"localhost","$lib":"web","$lib_version":"1.24.0","$os":"Mac OS X","$os_version":"10.15.7","$referrer":"http://localhost:8000/","$referring_domain":"localhost","$screen_height":900,"$screen_width":1440,"$user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) PostHog/1.24.0 Chrome/90.0.4430.93 Electron/12.0.7 Safari/537.36","distinct_id":"00000000-0000-0000-0000-000000000000","elements":[{"attr__class":"ant-btn","attr__href":"/signup","attr__role":"button","attr__title":"Sign up","tag_name":"a","text":"Sign up"}],"event":"$pageview","properties":{"$current_url":"http://localhost:8000/","$initial_referrer":"http://localhost:8000/","$initial_referring_domain":"localhost","$referrer":"http://localhost:8000/","$referring_domain":"localhost"},"timestamp":"2021-05-05T16:00:00.000000Z","type":"$autocapture"}',
                datetime.datetime(2021, 5, 5, 16, 0, 0),
                team_id,
                "00000000-0000-0000-0000-000000000000",
                datetime.datetime(2021, 5, 5, 16, 0, 0),
                datetime.datetime(2021, 5, 5, 16, 0, 0),
                1,
            ),
        )

        # Run the migration script
        with responses.RequestsMock() as rsps:
            await main(
                sys_args=[
                    "--clickhouse-host",
                    "localhost",
                    "--clickhouse-user",
                    "default",
                    "--clickhouse-password",
                    "",
                    "--clickhouse-database",
                    "test",
                    "--posthog-host",
                    "http://localhost:8000",
                    "--posthog-api-token",
                    "test",
                    "--team-id",
                    "1",
                    "--start-date",
                    "2021-05-04T16:00:00",
                    "--end-date",
                    "2021-05-06T16:00:00",
                ]
            )

            # Check that the correct requests were made to PostHog
            assert len(rsps.calls) == 1


async def setup_clickhouse_schema(client: ChClient):
    # Setup ClickHouse. We don't actually create the distributed table but
    # rather just the MergeTree table as `events`. It's not exactly the same as
    # production but it's close enough for testing, and avoids needing
    # zookeeper.
    #
    # One thing we wouldn't be able to test is the interplay between multiple
    # shards and how that affects cursoring through.
    await client.execute("CREATE DATABASE IF NOT EXISTS test")
    await client.execute("DROP TABLE IF EXISTS events")
    await client.execute(
        """
        CREATE TABLE IF NOT EXISTS test.events (
            `uuid` UUID,
            `event` String,
            `properties` String CODEC(ZSTD(3)),
            `timestamp` DateTime64(6, 'UTC'),
            `team_id` Int64,
            `distinct_id` String,
            `created_at` DateTime64(6, 'UTC'),
            `_timestamp` DateTime,
            `_offset` UInt64,
        ) ENGINE = ReplacingMergeTree(
            _timestamp
        ) PARTITION BY toYYYYMM(timestamp)
        ORDER BY (
                team_id,
                toDate(timestamp),
                event,
                cityHash64(distinct_id),
                cityHash64(uuid)
            )
         """
    )
