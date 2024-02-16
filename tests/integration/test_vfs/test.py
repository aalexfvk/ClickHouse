#!/usr/bin/env python3

import time
import pytest
from kazoo.client import KazooClient
from helpers.cluster import ClickHouseCluster, ClickHouseInstance
from helpers.network import PartitionManager
from .utils import get_remote_pathes


DISK_NAME = "reacquire"
GC_SLEEP_SEC = 5
GC_LOCK_PATH = f"vfs/{DISK_NAME}/gc_lock"


@pytest.fixture(scope="module")
def started_cluster(request):
    cluster = ClickHouseCluster(__file__)
    try:
        cluster.add_instance(
            "node_1",
            main_configs=["configs/config.xml"],
            with_zookeeper=True,
            with_minio=True,
        )
        cluster.add_instance(
            "node_2",
            main_configs=["configs/config.xml"],
            with_zookeeper=True,
            with_minio=True,
        )
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def test_gc_delete_unlinked_objects(started_cluster):
    node_1: ClickHouseInstance = started_cluster.instances["node_1"]
    bucket = started_cluster.minio_bucket
    minio = started_cluster.minio_client
    table = "test_gc_delete"

    # Upload some files to the object storage
    node_1.query(f"CREATE TABLE {table} (i UInt32) ENGINE=MergeTree ORDER BY i")
    node_1.query(f"INSERT INTO {table} VALUES (0)")
    node_1.query(f"OPTIMIZE TABLE {table} FINAL")

    # Get all object keys related to the table
    table_objects = get_remote_pathes(node_1, table)
    bucket_objects = {
        obj.object_name for obj in minio.list_objects(bucket, "data/", recursive=True)
    }

    # All objects related to the table are present in bucket
    assert all([obj in bucket_objects for obj in table_objects])

    # Unlink all files related to the table
    node_1.query(f"DROP TABLE {table} SYNC")
    # Wait one GC iteration
    time.sleep(GC_SLEEP_SEC)

    bucket_objects = {
        obj.object_name for obj in minio.list_objects(bucket, "data/", recursive=True)
    }
    # Check that all objects related to the table were deleted by GC
    assert not any([obj in bucket_objects for obj in table_objects])


def test_optimistic_lock(started_cluster):
    node_1: ClickHouseInstance = started_cluster.instances["node_1"]
    zk: KazooClient = started_cluster.get_kazoo_client("zoo1")
    table = "test_optimistic_lock"

    # Inject delay before releasing optimistic lock on node_1
    # that vfs log has been processed by node_2
    node_1.query("SYSTEM ENABLE FAILPOINT vfs_gc_optimistic_lock_delay")

    # Make some disk activity
    node_1.query(f"CREATE TABLE {table} (i UInt32) ENGINE=MergeTree ORDER BY i")
    node_1.query(f"INSERT INTO {table} VALUES (0)")

    # Wait one GC iteration
    time.sleep(GC_SLEEP_SEC)

    # VFS log items was processed by node_2 replica
    node_1.wait_for_log_line(
        "Skip GC transaction because optimistic lock node was already updated"
    )

    # Last VFS snapshot was created by node_2 (replica marked in snapshot name)
    assert zk.get(GC_LOCK_PATH)[0].startswith(b"snapshot_node_2")

    node_1.query(f"DROP TABLE {table} SYNC")


# TODO myrrc check possible errors on merge and move
def test_session_breaks(started_cluster):
    node: ClickHouseInstance = started_cluster.instances["node_1"]
    table = "table_session_breaks"

    # non-replicated MergeTree implies ZK data flow will be vfs-related
    node.query(f"CREATE TABLE {table} (i UInt32) ENGINE=MergeTree ORDER BY i")
    node.query(f"INSERT INTO {table} VALUES (0)")

    with PartitionManager() as pm:
        pm.drop_instance_zk_connections(node)
        node.query_and_get_error(f"INSERT INTO {table} VALUES (1)")
        time.sleep(4)
    time.sleep(2)  # Wait for CH to reconnect to ZK before next GC run

    assert (
        int(node.count_in_log("VFSGC(reacquire): GC iteration finished")) > 1
    ), "GC must run at least twice"
    assert (
        int(node.count_in_log("Trying to establish a new connection with ZooKeeper"))
        > 1
    ), "ZooKeeper session must expire"

    node.query(f"INSERT INTO {table} VALUES (2)")
    assert int(node.query(f"SELECT count() FROM {table}")) == 2
    node.query(f"DROP TABLE {table} SYNC")


def test_ch_disks(started_cluster):
    node: ClickHouseInstance = started_cluster.instances["node_1"]

    listing = node.exec_in_container(
        [
            "/usr/bin/clickhouse",
            "disks",
            "--loglevel=trace",
            "--save-logs",
            "--config-file=/etc/clickhouse-server/config.d/config.xml",
            "list-disks",
        ],
        privileged=True,
        user="root",
    )
    print(listing)
    assert listing == "default\nreacquire\nreconcile\n"

    listing = node.exec_in_container(
        [
            "/usr/bin/clickhouse",
            "disks",
            "--config-file=/etc/clickhouse-server/config.xml",
            "--loglevel=trace",
            "--save-logs",
            "--disk=reconcile",
            "list",
            "/",
        ],
        privileged=True,
        user="root",
    )

    print(listing)

    log = node.exec_in_container(
        [
            "/usr/bin/cat",
            "/var/log/clickhouse-server/clickhouse-disks.log",
        ],
        privileged=True,
        user="root",
    )
    print(log)

    assert "GC enabled: false" in log
    assert "GC started" not in log
