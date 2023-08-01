import datetime
import json

import pytest
import responses
from aiochclient import ChClient

from migrate import elements_chain_to_elements, main


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

        # Add some events to the sharded_events table. We use the max value for
        # the team_id + 1 to ensure we don't collide with any existing events.
        rows = await client.fetchrow("SELECT max(team_id) FROM test.events")
        team_id = rows[0] if rows is not None else 0
        await client.execute(
            """
            INSERT INTO test.events
            VALUES
            """,
            (
                "00000000-0000-0000-0000-000000000000",
                "test",
                json.dumps(
                    properties1 := {
                        "$browser": "Chrome",
                        "$browser_version": "90.0.4430.93",
                        "$current_url": "http://localhost:8000/",
                        "$device_id": "00000000-0000-0000-0000-000000000000",
                        "$device_type": "desktop",
                        "$initial_referrer": "http://localhost:8000/",
                        "$initial_referring_domain": "localhost",
                        "$lib": "web",
                        "$lib_version": "1.24.0",
                        "$os": "Mac OS X",
                        "$os_version": "10.15.7",
                        "$referrer": "http://localhost:8000/",
                        "$referring_domain": "localhost",
                        "$screen_height": 900,
                        "$screen_width": 1440,
                        "$user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) PostHog/1.24.0 Chrome/90.0.4430.93 Electron/12.0.7 Safari/537.36",
                        "distinct_id": "00000000-0000-0000-0000-000000000000",
                        "elements": [
                            {
                                "attr__class": "ant-btn",
                                "attr__href": "/signup",
                                "attr__role": "button",
                                "attr__title": "Sign up",
                                "tag_name": "a",
                                "text": "Sign up",
                            }
                        ],
                        "event": "$pageview",
                        "properties": {
                            "$current_url": "http://localhost:8000/",
                            "$initial_referrer": "http://localhost:8000/",
                            "$initial_referring_domain": "localhost",
                            "$referrer": "http://localhost:8000/",
                            "$referring_domain": "localhost",
                        },
                        "timestamp": "2021-05-05T16:00:00.000000Z",
                        "type": "$autocapture",
                    }
                ),
                datetime.datetime(2021, 5, 5, 16, 0, 0),
                team_id,
                "00000000-0000-0000-0000-000000000000",
                datetime.datetime(2021, 5, 5, 16, 0, 0),
                """strong.pricingpage:attr__class="pricingpage"nth-child="1"nth-of-type="1"text="A question?";""",
                datetime.datetime(2021, 5, 5, 16, 0, 0),
                0,
            ),
            (
                "00000000-0000-0000-0000-000000000001",
                "test",
                json.dumps(
                    properties2 := {
                        "$browser": "Chrome",
                        "$browser_version": "90.0.4430.93",
                        "$current_url": "http://localhost:8000/",
                        "$device_id": "00000000-0000-0000-0000-000000000000",
                        "$device_type": "desktop",
                        "$initial_referrer": "http://localhost:8000/",
                        "$initial_referring_domain": "localhost",
                        "$lib": "web",
                        "$lib_version": "1.24.0",
                        "$os": "Mac OS X",
                        "$os_version": "10.15.7",
                        "$referrer": "http://localhost:8000/",
                        "$referring_domain": "localhost",
                        "$screen_height": 900,
                        "$screen_width": 1440,
                        "$user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) PostHog/1.24.0 Chrome/90.0.4430.93 Electron/12.0.7 Safari/537.36",
                        "distinct_id": "00000000-0000-0000-0000-000000000000",
                        "elements": [
                            {
                                "attr__class": "ant-btn",
                                "attr__href": "/signup",
                                "attr__role": "button",
                                "attr__title": "Sign up",
                                "tag_name": "a",
                                "text": "Sign up",
                            }
                        ],
                        "event": "$pageview",
                        "properties": {
                            "$current_url": "http://localhost:8000/",
                            "$initial_referrer": "http://localhost:8000/",
                            "$initial_referring_domain": "localhost",
                            "$referrer": "http://localhost:8000/",
                            "$referring_domain": "localhost",
                        },
                        "timestamp": "2021-05-05T16:00:00.000000Z",
                        "type": "$autocapture",
                    }
                ),
                datetime.datetime(2021, 5, 5, 16, 2, 0),
                team_id,
                "00000000-0000-0000-0000-000000000000",
                datetime.datetime(2021, 5, 5, 16, 2, 0),
                """strong.pricingpage:attr__class="pricingpage"nth-child="1"nth-of-type="1"text="A question?";""",
                datetime.datetime(2021, 5, 5, 16, 2, 0),
                1,
            ),
        )

        # Run the migration script
        with responses.RequestsMock() as rsps:
            rsps.add(
                responses.POST,
                "http://localhost:8000/batch/",
                json={"status": "ok"},
                status=200,
            )

            await main(
                sys_args=[
                    "--clickhouse-url",
                    "http://localhost:8123",
                    "--clickhouse-user",
                    "default",
                    "--clickhouse-password",
                    "",
                    "--clickhouse-database",
                    "test",
                    "--posthog-url",
                    "http://localhost:8000",
                    "--posthog-api-token",
                    "test",
                    "--team-id",
                    str(team_id),
                    "--start-date",
                    "2021-05-04",
                    "--end-date",
                    "2021-05-06",
                    "--verbose",
                ]
            )

            # Check that the correct requests were made to PostHog with the
            # right data
            assert len(rsps.calls) == 1
            assert rsps.calls[0].request.url == "http://localhost:8000/batch/"
            assert rsps.calls[0].request.method == responses.POST
            body_json = json.loads(rsps.calls[0].request.body)

            # Check we include the token
            assert body_json["api_key"] == "test"

            # Check we include the right events
            assert body_json["batch"][0] == {
                "distinct_id": "00000000-0000-0000-0000-000000000000",
                "event": "test",
                "properties": {
                    **properties1,
                    # These are a little annoying but not the end of the world, I'll
                    # add something to the readme about it.
                    "$lib": "posthog-python",
                    "$lib_version": "3.0.1",
                    "$geoip_disable": True,  # This makes sense, it shouldn't do geoip again
                },
                "context": {},  # I don't know what this one is but it seems to get added
                "timestamp": "2021-05-05T16:00:00+00:00",
            }

        # Check we don't include events before the start date
        with responses.RequestsMock() as rsps:
            await main(
                sys_args=[
                    "--clickhouse-url",
                    "http://localhost:8123",
                    "--clickhouse-user",
                    "default",
                    "--clickhouse-password",
                    "",
                    "--clickhouse-database",
                    "test",
                    "--posthog-url",
                    "http://localhost:8000",
                    "--posthog-api-token",
                    "test",
                    "--team-id",
                    str(team_id),
                    "--start-date",
                    "2021-05-06",
                    "--end-date",
                    "2021-05-07",
                    "--verbose",
                ]
            )

            # Check that the correct requests were made to PostHog with the
            # right data
            assert len(rsps.calls) == 0

        # Check we don't include events after the end date
        with responses.RequestsMock() as rsps:
            await main(
                sys_args=[
                    "--clickhouse-url",
                    "http://localhost:8123",
                    "--clickhouse-user",
                    "default",
                    "--clickhouse-password",
                    "",
                    "--clickhouse-database",
                    "test",
                    "--posthog-url",
                    "http://localhost:8000",
                    "--posthog-api-token",
                    "test",
                    "--team-id",
                    str(team_id),
                    "--start-date",
                    "2021-05-03",
                    "--end-date",
                    "2021-05-04",
                    "--verbose",
                ]
            )

            # Check that the correct requests were made to PostHog with the
            # right data
            assert len(rsps.calls) == 0

        # Check we don't include events from other teams
        with responses.RequestsMock() as rsps:
            await main(
                sys_args=[
                    "--clickhouse-url",
                    "http://localhost:8123",
                    "--clickhouse-user",
                    "default",
                    "--clickhouse-password",
                    "",
                    "--clickhouse-database",
                    "test",
                    "--posthog-url",
                    "http://localhost:8000",
                    "--posthog-api-token",
                    "test",
                    "--team-id",
                    str(team_id + 1),
                    "--start-date",
                    "2021-05-04",
                    "--end-date",
                    "2021-05-06",
                    "--verbose",
                ]
            )

            # Check that the correct requests were made to PostHog with the
            # right data
            assert len(rsps.calls) == 0

        # Check we get a cursor back from migrate that we can use in subsequent
        # calls to migrate
        cursor = None
        with responses.RequestsMock() as rsps:
            rsps.add(
                responses.POST,
                "http://localhost:8000/batch/",
                json={"status": "ok"},
                status=200,
            )

            cursor, _ = await main(
                sys_args=[
                    "--clickhouse-url",
                    "http://localhost:8123",
                    "--clickhouse-user",
                    "default",
                    "--clickhouse-password",
                    "",
                    "--clickhouse-database",
                    "test",
                    "--posthog-url",
                    "http://localhost:8000",
                    "--posthog-api-token",
                    "test",
                    "--team-id",
                    str(team_id),
                    "--start-date",
                    "2021-05-05T16:00:00",
                    "--end-date",
                    "2021-05-05T16:01:00",  # Before the second event
                    "--verbose",
                ]
            )

            # Check that the correct requests were made to PostHog with the
            # right data
            assert len(rsps.calls) == 1
            assert rsps.calls[0].request.url == "http://localhost:8000/batch/"
            assert rsps.calls[0].request.method == responses.POST
            body_json = json.loads(rsps.calls[0].request.body)

            # Check we include the token
            assert body_json["api_key"] == "test"

            # Check we include the first event only
            assert body_json["batch"] == [
                {
                    "distinct_id": "00000000-0000-0000-0000-000000000000",
                    "event": "test",
                    "properties": {
                        **properties1,
                        # These are a little annoying but not the end of the world, I'll
                        # add something to the readme about it.
                        "$lib": "posthog-python",
                        "$lib_version": "3.0.1",
                        "$geoip_disable": True,  # This makes sense, it shouldn't do geoip again
                    },
                    "context": {},  # I don't know what this one is but it seems to get added
                    "timestamp": "2021-05-05T16:00:00+00:00",
                }
            ]

        assert cursor

        # Now use the cursor to get the second event only
        with responses.RequestsMock() as rsps:
            rsps.add(
                responses.POST,
                "http://localhost:8000/batch/",
                json={"status": "ok"},
                status=200,
            )

            await main(
                sys_args=[
                    "--clickhouse-url",
                    "http://localhost:8123",
                    "--clickhouse-user",
                    "default",
                    "--clickhouse-password",
                    "",
                    "--clickhouse-database",
                    "test",
                    "--posthog-url",
                    "http://localhost:8000",
                    "--posthog-api-token",
                    "test",
                    "--team-id",
                    str(team_id),
                    "--cursor",
                    cursor,
                    "--start-date",
                    "2021-05-05T16:00:00",
                    "--end-date",
                    "2021-05-05T16:03:00",
                    "--verbose",
                ]
            )

            # Check that the correct requests were made to PostHog with the
            # right data
            assert len(rsps.calls) == 1
            assert rsps.calls[0].request.url == "http://localhost:8000/batch/"
            assert rsps.calls[0].request.method == responses.POST
            body_json = json.loads(rsps.calls[0].request.body)

            # Check we include the token
            assert body_json["api_key"] == "test"

            # Check we include the second event only
            assert body_json["batch"] == [
                {
                    "distinct_id": "00000000-0000-0000-0000-000000000000",
                    "event": "test",
                    "properties": {
                        **properties2,
                        # These are a little annoying but not the end of the world, I'll
                        # add something to the readme about it.
                        "$lib": "posthog-python",
                        "$lib_version": "3.0.1",
                        "$geoip_disable": True,  # This makes sense, it shouldn't do geoip again
                    },
                    "context": {},  # I don't know what this one is but it seems to get added
                    "timestamp": "2021-05-05T16:02:00+00:00",
                }
            ]


async def setup_clickhouse_schema(client: ChClient):
    """
    Setup ClickHouse. We don't actually create the distributed table but
    rather just the MergeTree table as `events`. It's not exactly the same as
    production but it's close enough for testing, and avoids needing
    zookeeper.

    One thing we wouldn't be able to test is the interplay between multiple
    shards and how that affects cursoring through.
    """
    await client.execute("CREATE DATABASE IF NOT EXISTS test")
    await client.execute("DROP TABLE IF EXISTS test.events")
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
            `elements_chain` VARCHAR,
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


@pytest.mark.parametrize(
    "elements_chain,expected",
    [
        (
            """strong.pricingpage:attr__class="pricingpage"nth-child="1"nth-of-type="1"text="A question?";""",
            [
                {
                    "tag_name": "strong",
                    "attr__class": "pricingpage",
                    "nth_child": 1,
                    "nth_of_type": 1,
                    "$el_text": "A question?",
                }
            ],
        ),
        (
            """
            div:attr__id="gatsby-focus-wrapper"attr__style="outline:none"attr__tabindex="-1"attr_id="gatsby-focus-wrapper"nth-child="1"nth-of-type="1";div:attr__id="___gatsby"attr_id="___gatsby"nth-child="3"nth-of-type="1";body.dark:attr__class="dark"nth-child="2"nth-of-type="1"
            """,
            [
                {
                    'attr__id': 'gatsby-focus-wrapper',
                    'attr__style': 'outline:none',
                    'attr__tabindex': '-1',
                    'nth_child': 1,
                    'nth_of_type': 1,
                    'tag_name': 'div'
                },
                {
                    'attr__id': '___gatsby',
                    'nth_child': 3,
                    'nth_of_type': 1,
                    'tag_name': 'div'
                },
                {
                    'attr__class': 'dark',
                    'nth_child': 2,
                    'nth_of_type': 1,
                    'tag_name': 'body'
                }
            ]
        ), (
            'button.LemonButton.LemonButton--has-icon.LemonButton--has-side-icon.LemonButton--secondary.LemonButton'
            '--small.LemonButton--status-stealth:attr__aria-disabled="false"attr__aria-haspopup="true"attr__class='
            '"LemonButton LemonButton--secondary LemonButton--status-stealth LemonButton--small LemonButton--has-icon '
            'LemonButton--has-side-icon"attr__data-attr="date-filter"attr__id="daterange_selector"attr__type="button'
            '"attr_id="daterange_selector"nth-child="2"nth-of-type="1"text="Last 7 days"',
            [
                {
                    'attr__aria-disabled': 'false',
                    'attr__aria-haspopup': 'true',
                    'attr__class': 'LemonButton LemonButton--secondary LemonButton--status-stealth '
                                   'LemonButton--small LemonButton--has-icon '
                                   'LemonButton--has-side-icon',
                    'attr__data-attr': 'date-filter',
                    'attr__id': 'daterange_selector',
                    'attr__type': 'button',
                    'nth_child': 2,
                    'nth_of_type': 1,
                    'tag_name': 'button',
                    '$el_text': 'Last 7 days'
                },
            ]
        )
    ],
)
def test_elements_chain_to_elements(elements_chain, expected):
    db_elements = elements_chain_to_elements(elements_chain)

    assert db_elements == expected
