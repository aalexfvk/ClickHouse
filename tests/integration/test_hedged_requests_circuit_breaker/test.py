import contextlib
import json
import time
import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager


cluster = ClickHouseCluster(__file__)

# Topology: two shards, two replicas each.
#
# node_1_1 is the query initiator for all tests.
#
# Shard 1 (node_1_1 / node_1_2) - used for prefer_localhost_replica=1 tests:
#   node_1_1 is the local replica; node_1_2 is the remote replica.
#   Partition node_1_1 <-> node_1_2 to simulate remote-replica failure.
#
# Shard 2 (node_2_1 / node_2_2) - used for hedged-requests circuit-breaker tests:
#   With load_balancing=in_order, node_2_1 is always tried first.
#   Partition node_1_1 <-> node_2_1 to simulate first-replica failure.

node_1_1 = cluster.add_instance(
    "node_1_1",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/users.xml"],
    with_zookeeper=True,
)
node_1_2 = cluster.add_instance(
    "node_1_2",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/users.xml"],
    with_zookeeper=True,
)
node_2_1 = cluster.add_instance(
    "node_2_1",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/users.xml"],
    with_zookeeper=True,
)
node_2_2 = cluster.add_instance(
    "node_2_2",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/users.xml"],
    with_zookeeper=True,
)

# For test to be runnable multiple times
seqno = 0


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(scope="function", autouse=True)
def reset_pool(started_cluster):
    """Reset `PoolWithFailover` state (ban list) between tests.

    This forces ClickHouse to destroy and recreate all `PoolWithFailover` instances so that
    ban state accumulated in a previous test does not leak into the next one.
    """
    reset_config_path = "/etc/clickhouse-server/config.d/zzz_reset_remote_servers.xml"

    for n in (node_1_1, node_1_2, node_2_1, node_2_2):
        n.replace_config(
            reset_config_path,
            '<clickhouse><remote_servers replace="replace"/></clickhouse>',
        )
        n.query("SYSTEM RELOAD CONFIG")
    for n in (node_1_1, node_1_2, node_2_1, node_2_2):
        n.replace_config(reset_config_path, "<clickhouse/>")
        n.query("SYSTEM RELOAD CONFIG")
    yield


@pytest.fixture(scope="function", autouse=True)
def create_tables():
    global seqno
    try:
        seqno += 1

        # Shard 1 replicas share a ReplicatedMergeTree table.

        shards = [(node_1_1, node_1_2), (node_2_1, node_2_2)]

        for shard_num, nodes in enumerate(shards, start=1):
            for n in nodes:
                n.query(
                    f"CREATE TABLE replicated_data (d Date, x UInt32)"
                    f" ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard{shard_num}/replicated_data_{seqno}', '{n.name}')"
                    f" PARTITION BY toYYYYMM(d) ORDER BY d"
                )

        # Distributed table on the initiator.
        node_1_1.query(
            "CREATE TABLE distributed (d Date, x UInt32)"
            " ENGINE = Distributed('test_cluster', 'default', 'replicated_data')"
        )

        # Seed data on both shards.
        node_1_1.query("INSERT INTO replicated_data VALUES ('2026-01-01', 1)")
        node_1_2.query("SYSTEM SYNC REPLICA replicated_data", timeout=10)

        node_2_1.query("INSERT INTO replicated_data VALUES ('2026-01-01', 2)")
        node_2_2.query("SYSTEM SYNC REPLICA replicated_data", timeout=10)

        yield

    finally:
        node_1_1.query("DROP TABLE IF EXISTS distributed")
        for n in (node_1_1, node_1_2, node_2_1, node_2_2):
            n.query("DROP TABLE IF EXISTS replicated_data")


# Circuit breaker parameters from configs/users.xml.
BAN_MIN_MS = 2000   # distributed_replica_circuit_breaker_ban_min_ms


# Per-query settings shared by all circuit-breaker tests.
CB_SETTINGS = {
    "load_balancing": "in_order",
    "prefer_localhost_replica": 1,
    "use_hedged_requests": 1,
    "connections_with_failover_max_tries": 1,
    "connect_timeout_with_failover_ms": 5_000,
    "receive_timeout": 5,
    "max_replica_delay_for_distributed_queries": 1,
    "fallback_to_stale_replicas_for_distributed_queries": 1,
}

@contextlib.contextmanager
def assert_elapsed(min_ms=None, max_ms=None):
    """Context manager that measures wall-clock time of the enclosed block and
    asserts it falls within [min_ms, max_ms).
    """
    start = time.monotonic()
    yield
    elapsed_ms = (time.monotonic() - start) * 1000
    if min_ms is not None:
        assert elapsed_ms >= min_ms, (
            f"Expected elapsed >= {min_ms:.0f} ms, got {elapsed_ms:.0f} ms"
        )
    if max_ms is not None:
        assert elapsed_ms < max_ms, (
            f"Expected elapsed < {max_ms:.0f} ms, got {elapsed_ms:.0f} ms"
        )


def _query(shard=None, extra_settings=None, query_id=None, table="distributed"):
    settings = dict(CB_SETTINGS)
    if extra_settings:
        settings.update(extra_settings)
    where = f" WHERE x = {shard}" if shard is not None else ""
    kwargs = {
        "sql": f"SELECT hostName() FROM {table}{where} LIMIT 1",
        "settings": settings
    }
    if query_id is not None:
        kwargs["query_id"] = query_id
    return node_1_1.query(**kwargs).strip()


def _get_query_profile_events(query_id):
    """Return ProfileEvents map for a completed query from system.query_log on node_1_1."""
    result = node_1_1.query(
        f"""
        SELECT ProfileEvents
        FROM system.query_log
        WHERE query_id = '{query_id}'
          AND type = 'QueryFinish'
        LIMIT 1
        FORMAT JSONEachRow
        """
    ).strip()
    if not result:
        return {}
    return json.loads(result).get("ProfileEvents", {})


def _wait_for_stale_replica(node, table="replicated_data", min_delay_s=1, timeout_s=15):
    """Wait until *node* reports an absolute replication delay >= min_delay_s seconds.

    Raises an exception if the replica does not become stale within timeout_s seconds.
    """
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        delay = node.query(
            f"SELECT absolute_delay FROM system.replicas WHERE table = '{table}'"
        ).strip()
        if delay and int(delay) >= min_delay_s:
            return
        time.sleep(0.5)
    raise Exception(
        f"{node.name} did not become stale (delay >= {min_delay_s}s) within {timeout_s}s"
    )


def test_banned_replica_is_skipped(started_cluster):
    """
    After node_2_1 fails and is banned by the circuit breaker, the next query
    must go directly to node_2_2 - without spending `connect_timeout_with_failover_ms`
    trying to reach the still-unreachable node_2_1.
    """
    connect_timeout_ms = CB_SETTINGS["connect_timeout_with_failover_ms"]
    # max_tries=1: to wait only one connect_timeout_ms instead of max_tries * connect_timeout_ms.
    # use_hedged_requests=0: forces the synchronous path so that
    # node_2_1 is guaranteed to exhaust all max_tries attempts before the query completes via node_2_2.
    no_hedged = {**CB_SETTINGS, "use_hedged_requests": 0, "connections_with_failover_max_tries": 1}

    # With both replicas up, in_order picks node_2_1 first for shard 2.
    assert _query(shard=2, extra_settings=no_hedged) == "node_2_1", "Expected node_2_1 as baseline"

    with PartitionManager() as pm:
        pm.partition_instances(node_1_1, node_2_1, port=9000)

        # First query:
        # node_2_1 is unreachable -> connect timeout (synchronous) ->
        # error_count >= max_tries -> circuit breaker bans node_2_1 ->
        # falls back to node_2_2.
        with assert_elapsed(min_ms=connect_timeout_ms):
            result = _query(shard=2, extra_settings=no_hedged)
        assert result == "node_2_2", f"Expected node_2_2 on first failure, got {result}"

        # Second query: node_2_1 is still unreachable AND banned.
        # The query must skip node_2_1 immediately.
        with assert_elapsed(max_ms=connect_timeout_ms / 2):
            result = _query(shard=2, extra_settings=no_hedged)
        assert result == "node_2_2", f"Expected node_2_2 (node_2_1 banned), got {result}"


def test_banned_replica_skipped_with_stale_fallback(started_cluster):
    """
    When node_2_1 is banned by the circuit breaker and node_2_2 is a stale replica,
    the second query must still skip node_2_1 immediately (no connect-timeout wait)
    and fall back to node_2_2 - verified via query_log ProfileEvents.
    """
    connect_timeout_ms = CB_SETTINGS["connect_timeout_with_failover_ms"]

    # Make node_2_2 stale
    node_2_2.query("SYSTEM STOP FETCHES replicated_data")
    node_2_1.query("INSERT INTO replicated_data VALUES ('2020-06-01', 100)")
    _wait_for_stale_replica(node_2_2)

    with PartitionManager() as pm:
        pm.partition_instances(node_1_1, node_2_1, port=9000)

        # First query:
        # node_2_1 unreachable -> connect timeout ->
        # circuit breaker bans node_2_1 -> falls back to stale node_2_2.
        ban_query_id = str(uuid.uuid4())
        with assert_elapsed(min_ms=connect_timeout_ms):
            result = _query(shard=2, query_id=ban_query_id)
        assert result == "node_2_2", (
            f"Expected stale node_2_2 as fallback on first failure, got {result}"
        )

        # Second query: node_2_1 is banned -> skipped immediately.
        query_id = str(uuid.uuid4())
        with assert_elapsed(max_ms=connect_timeout_ms / 2):
            result = _query(shard=2, query_id=query_id)
        assert result == "node_2_2", (
            f"Expected stale node_2_2 (node_2_1 banned), got {result}"
        )

        # Verify that the circuit breaker ban was recorded.
        node_1_1.query("SYSTEM FLUSH LOGS")

        ban_events = _get_query_profile_events(ban_query_id)
        ban_count = int(ban_events.get("DistributedConnectionCircuitBreakerBan", 0))
        assert ban_count == 1, (
            f"Expected 1 DistributedConnectionCircuitBreakerBan on first failure, got {ban_count}"
        )

        events = _get_query_profile_events(query_id)
        # With prefer_localhost_replica=1, shard 1 is executed locally on node_1_1
        # (no TCP connection -> DistributedConnectionTries not incremented for shard 1).
        # For shard 2: node_2_1 was banned and skipped, so only node_2_2 is tried.
        # Total: DistributedConnectionTries == 1 (only node_2_2).
        dist_tries = int(events.get("DistributedConnectionTries", 0))
        assert dist_tries == 1, (
            f"Expected 1 DistributedConnectionTries (shard 1 local, node_2_1 skipped), got {dist_tries}"
        )

        # No failed connection attempts - node_2_1 was not even tried.
        fail_tries = int(events.get("DistributedConnectionFailTry", 0))
        assert fail_tries == 0, (
            f"Expected 0 DistributedConnectionFailTry, got {fail_tries}"
        )

        # No new bans on the second query - node_2_1 was already banned and skipped.
        ban_count_2 = int(events.get("DistributedConnectionCircuitBreakerBan", 0))
        assert ban_count_2 == 0, (
            f"Expected 0 DistributedConnectionCircuitBreakerBan on second query, got {ban_count_2}"
        )


def test_ban_expires_and_replica_is_retried(started_cluster):
    """
    After the ban duration elapses, node_2_1 must be tried again and, when it
    is reachable, chosen first by load_balancing=in_order.
    """
    connect_timeout_ms = CB_SETTINGS["connect_timeout_with_failover_ms"]

    # Make node_2_2 stale so it is not usable as an up-to-date fallback.
    node_2_2.query("SYSTEM STOP FETCHES replicated_data")
    node_2_1.query("INSERT INTO replicated_data VALUES ('2020-06-01', 100)")
    _wait_for_stale_replica(node_2_2)

    with PartitionManager() as pm:
        pm.partition_instances(node_1_1, node_2_1, port=9000)

        # Trigger the ban: node_2_1 is unreachable and node_2_2 is stale,
        # so hedged requests must wait for node_2_1 to time out before falling
        # back to stale node_2_2. This guarantees node_2_1 is banned.
        with assert_elapsed(min_ms=connect_timeout_ms):
            result = _query(shard=2)
        assert result == "node_2_2", f"Expected node_2_2 during partition, got {result}"

    # node_2_1 is reachable again here.
    # Wait for the circuit breaker ban to expire.
    time.sleep(BAN_MIN_MS / 1000 + 1)

    with PartitionManager() as pm:
        # Make node_2_2 unreachable to avoid choosing it for hedged requests
        # due to better slowdown counter
        pm.partition_instances(node_1_1, node_2_2, port=9000)   

        # After the ban expires, node_2_1 is tried again and chosen first by in_order.
        result = _query(shard=2)
        assert result == "node_2_1", (
            f"Expected node_2_1 after ban expiry, got {result}"
        )


def test_prefer_localhost_replica_circuit_breaker(started_cluster):
    """
    When prefer_localhost_replica=1 and the local replica (node_1_1) is stale.
    This test verifies that the circuit breaker works in that path too.
    """
    connect_timeout_ms = CB_SETTINGS["connect_timeout_with_failover_ms"]

    # Make node_1_1 stale: stop replication fetches, insert new data only on node_1_2.
    node_1_1.query("SYSTEM STOP FETCHES replicated_data")
    node_1_2.query("INSERT INTO replicated_data VALUES ('2020-09-01', 200)")

    # Wait until node_1_1 replication lag >= 1 second.
    _wait_for_stale_replica(node_1_1)

    with PartitionManager() as pm:
        # Make remote replica of shard 1 is unreachable.
        pm.partition_instances(node_1_1, node_1_2, port=9000)

        # First query:
        # node_1_1 is stale -> fallback to node_1_2 ->
        # node_1_2 unreachable -> connect timeout ->
        # circuit breaker bans node_1_2 ->
        # falls back to stale node_1_1 (due to fallback_to_stale_replicas=1).
        with assert_elapsed(min_ms=connect_timeout_ms):
            result = _query(shard=1)
        assert result == "node_1_1", (
            f"Expected stale node_1_1 as fallback on first failure, got {result}"
        )

        # Second query: node_1_2 is banned -> skipped immediately.
        # Without the ban the query would spend ~connect_timeout_ms trying node_1_2.
        with assert_elapsed(max_ms=connect_timeout_ms / 2):
            result = _query(shard=1)
        assert result == "node_1_1", (
            f"Expected node_1_1 (node_1_2 banned), got {result}"
        )

