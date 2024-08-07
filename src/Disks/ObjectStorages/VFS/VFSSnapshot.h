#pragma once
#include <Disks/ObjectStorages/IObjectStorage.h>
#include "AppendLog.h"
#include "IO/ReadHelpers.h"
#include "VFSLog.h"
#include "VFSShapshotMetadata.h"

#include <fstream>
#include <optional>
#include <IO/Lz4DeflatingWriteBuffer.h>
#include <IO/Lz4InflatingReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <fmt/chrono.h>

#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Stringifier.h>

namespace DB
{

struct VFSSnapshotEntry
{
    VFSSnapshotEntry(Poco::JSON::Object::Ptr data_json_)
        : data_json(data_json_)
        , remote_path(data_json->getValue<String>("remote_path"))
        , link_count(data_json->getValue<Int32>("link_count"))
    {
    }

    VFSSnapshotEntry(const String & remote_path_) : data_json(new Poco::JSON::Object()), remote_path(remote_path_), link_count(0)
    {
        /// Init json required fields
        data_json->set("remote_path", remote_path);
        data_json->set("link_count", link_count);

        data_json->set("links", Poco::JSON::Object::Ptr(new Poco::JSON::Object()));
    }

    /// Json Structure:
    /// {
    ///     remote_path : String
    ///     link_count : int
    ///     links : vectors per hosts
    ///     {
    ///         WAL_ID : vector of links per_each_wal
    ///         {
    ///             local_path : Status
    ///             {
    ///                 status : {}
    ///                 timestamp : {}
    ///                 WAL_ID_index : index
    ///             }
    ///             ...
    ///         }
    ///         WAL_ID2 {}
    ///         ...
    ///     }
    /// }
    Poco::JSON::Object::Ptr data_json;
    String remote_path;
    Int32 link_count;

    bool isAlive() const { return link_count > 0; }
    void updateWithVFSItems(const VFSLogItems & items_batch);
    void updateWithVFSItem(const VFSLogItem & item);

    bool operator==(const VFSSnapshotEntry & entry) const;

    static std::optional<VFSSnapshotEntry> deserialize(ReadBuffer & buf);
    void serialize(WriteBuffer & buf) const;
};

using VFSSnapshotEntries = std::vector<VFSSnapshotEntry>;

class VFSSnapshotDataBase
{
public:
    VFSSnapshotDataBase() : log(getLogger("VFSSnapshotDataBase(unnamed)")) { }
    virtual ~VFSSnapshotDataBase() = default;

    VFSSnapshotDataBase(const VFSSnapshotDataBase &) = delete;
    VFSSnapshotDataBase(VFSSnapshotDataBase &&) = delete;

    /// Apply wal on the current snapshot and returns metadata of the updated one.
    SnapshotMetadata mergeWithWals(VFSLogItems && wal_items, const SnapshotMetadata & old_snapshot_meta);

private:
    struct VFSSnapshotMergeResult
    {
        VFSSnapshotEntries entries_to_remove;
        ProcessedLogsIndicesMap updated_log_ind_map;
    };


    void writeEntryOrAddToRemove(const VFSSnapshotEntry & entry, WriteBuffer & write_buffer, VFSSnapshotEntries & entries_to_remove);
    VFSSnapshotMergeResult mergeWithWalsImpl(
        VFSLogItems && wal_items_,
        ReadBuffer & read_buffer,
        WriteBuffer & write_buffer,
        const ProcessedLogsIndicesMap & old_vfs_wal_indices_map);

    LoggerPtr log;

protected:
    /// Methods with storage-specific logic.
    virtual void removeShapshotEntires(const VFSSnapshotEntries & entires_to_remove) = 0;
    virtual std::unique_ptr<ReadBuffer> getShapshotReadBuffer(const SnapshotMetadata & snapshot_meta) const = 0;
    virtual std::pair<std::unique_ptr<WriteBuffer>, String>
    getShapshotWriteBufferAndsnapshotObject(const SnapshotMetadata & snapshot_meta) const = 0;
};

class VFSSnapshotDataFromObjectStorage : public VFSSnapshotDataBase
{
public:
    VFSSnapshotDataFromObjectStorage(ObjectStoragePtr object_storage_, const String & name_)
        : object_storage(object_storage_), log(getLogger(fmt::format("VFSSnapshotDataFromObjectStorage({})", name_)))
    {
    }

protected:
    void removeShapshotEntires(const VFSSnapshotEntries & entires_to_remove) override;
    std::unique_ptr<ReadBuffer> getShapshotReadBuffer(const SnapshotMetadata & snapshot_meta) const override;
    std::pair<std::unique_ptr<WriteBuffer>, String>
    getShapshotWriteBufferAndsnapshotObject(const SnapshotMetadata & snapshot_meta) const override;

private:
    ObjectStoragePtr object_storage;
    LoggerPtr log;
};

}
