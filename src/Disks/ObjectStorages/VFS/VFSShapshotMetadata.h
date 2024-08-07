#pragma once

#include <Disks/ObjectStorages/VFS/JSONUtils.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <fmt/chrono.h>

#include <unordered_map>

namespace DB
{

using ProcessedLogsIndicesMap = std::unordered_map<UUID, UInt64>;

struct SnapshotMetadata
{
    UInt64 metadata_version;
    String object_storage_key;
    UInt64 total_size;
    Int32 znode_version;
    bool is_initial_snapshot;
    ProcessedLogsIndicesMap processed_logs_indices;

    SnapshotMetadata(
        UInt64 metadata_version_ = 0ull,
        const String & object_storage_key_ = "",
        UInt64 total_size_ = 0ull,
        Int32 znode_version_ = -1,
        bool is_initial_ = false)
        : metadata_version(metadata_version_)
        , object_storage_key(object_storage_key_)
        , total_size(total_size_)
        , znode_version(znode_version_)
        , is_initial_snapshot(is_initial_)
    {
    }

    SnapshotMetadata(
        const String & object_storage_key_,
        UInt64 metadata_version_ = 0ull,
        UInt64 total_size_ = 0ull,
        Int32 znode_version_ = -1,
        bool is_initial_ = false)
        : metadata_version(metadata_version_)
        , object_storage_key(object_storage_key_)
        , total_size(total_size_)
        , znode_version(znode_version_)
        , is_initial_snapshot(is_initial_)
    {
    }

    void update(const String & new_snapshot_key) { object_storage_key = new_snapshot_key; }

    String serialize() const
    {
        Poco::JSON::Object::Ptr root(new Poco::JSON::Object());

        root->set("metadata_version", metadata_version);
        root->set("object_storage_key", object_storage_key);
        root->set("total_size", total_size);
        root->set("znode_version", znode_version);

        Poco::JSON::Object::Ptr logs_indices_object(new Poco::JSON::Object());
        root->set("processed_logs_indices", logs_indices_object);

        for (const auto & [wal_id, index] : processed_logs_indices)
        {
            logs_indices_object->set(toString(wal_id), index);
        }
        return JSONUtils::to_string(*root);
    }

    static SnapshotMetadata deserialize(const String & str, Int32 znode_version)
    {
        SnapshotMetadata result;
        result.znode_version = znode_version;

        /// In case of initial snapshot, the content will be empty.
        if (str.empty())
        {
            result.is_initial_snapshot = true;
            return result;
        }
        auto json_object = JSONUtils::from_string(str);

        result.is_initial_snapshot = false;
        result.metadata_version = JSONUtils::getOrThrow<UInt64>(*json_object, "metadata_version");
        result.object_storage_key = JSONUtils::getOrThrow<String>(*json_object, "object_storage_key");
        result.total_size = JSONUtils::getOrThrow<UInt64>(*json_object, "total_size");
        auto logs_indicies_object = json_object->getObject("processed_logs_indices");
        if (!logs_indicies_object)
        {
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Key processed_log_indices is not found in JSON with metadata for snapshot");
        }
        for (auto it = logs_indicies_object->begin(); it != logs_indicies_object->end(); ++it)
        {
            result.processed_logs_indices.insert({parse<UUID>(it->first), it->second});
        }
        return result;
    }
};
}
