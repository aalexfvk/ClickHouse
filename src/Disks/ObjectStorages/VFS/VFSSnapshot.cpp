#include "VFSSnapshot.h"
#include <algorithm>
#include <iostream>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <IO/ReadBufferFromEmptyFile.h>

#include "IO/ReadBufferFromFileBase.h"
#include "IO/ReadHelpers.h"
#include "IO/WriteHelpers.h"

namespace ProfileEvents
{
extern const Event VFSGcCumulativeSnapshotBytesRead;
}

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int CORRUPTED_DATA;
}

const char VFS_SHAPSHOT_ENTRIES_DELIMITER = '\n';

bool VFSSnapshotEntry::operator==(const VFSSnapshotEntry & entry) const
{
    return remote_path == entry.remote_path && link_count == entry.link_count;
}

std::optional<VFSSnapshotEntry> VFSSnapshotEntry::deserialize(ReadBuffer & buf)
{
    if (buf.eof())
        return std::nullopt;

    String entry_string;
    readStringUntilNewlineInto(entry_string, buf);

    Poco::JSON::Parser parser;
    auto entry_json = parser.parse(entry_string).extract<Poco::JSON::Object::Ptr>();

    if (!entry_json->has("remote_path") || !entry_json->has("link_count"))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Incomplete VFS log item");

    VFSSnapshotEntry entry(entry_json);
    checkChar(VFS_SHAPSHOT_ENTRIES_DELIMITER, buf);
    return entry;
}

void VFSSnapshotEntry::serialize(WriteBuffer & buf) const
{
    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(data_json, oss);

    writeString(oss.str(), buf);
    writeChar(VFS_SHAPSHOT_ENTRIES_DELIMITER, buf);
}


void VFSSnapshotEntry::updateWithVFSItem(const VFSLogItem & item)
{
    auto links_object = data_json->getObject("links");

    if (item.event.action == VFSAction::REQUEST || item.event.action == VFSAction::LINK)
    {
        if (!links_object->has(toString(item.getDestinationWalId())))
        {
            links_object->set(toString(item.getDestinationWalId()), Poco::JSON::Object::Ptr(new Poco::JSON::Object()));
        }

        auto wal_object = links_object->getObject(toString(item.getDestinationWalId()));

        if (!wal_object->has(item.event.local_path))
        {
            Poco::JSON::Object::Ptr local_path_object = new Poco::JSON::Object();
            wal_object->set(item.event.local_path, local_path_object);
            local_path_object->set("status", static_cast<std::underlying_type_t<VFSAction>>(item.event.action));
            local_path_object->set("timestamp", item.event.timestamp.epochMicroseconds());
            local_path_object->set("vfs_log_index", item.getDestinationWalIndex());
            link_count++;
            data_json->set("link_count", link_count);
        }
        else
        {
            auto local_path_object = wal_object->getObject(item.event.local_path);
            auto status_from_entry = static_cast<VFSAction>(local_path_object->getValue<std::underlying_type_t<VFSAction>>("status"));

            if (status_from_entry == VFSAction::UNLINK || (status_from_entry == VFSAction::REQUEST && item.event.action == VFSAction::LINK))
            {
                local_path_object->set("status", static_cast<std::underlying_type_t<VFSAction>>(item.event.action));
                local_path_object->set("timestamp", item.event.timestamp.epochMicroseconds());
                local_path_object->set("vfs_log_index", item.getDestinationWalIndex());
                return;
            }

            throw DB::Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Snapshot already contains entry for remote_path: {} vfs_log_id: {}, local_path {}",
                item.event.remote_path,
                item.getDestinationWalId(),
                item.event.local_path);
        }
    }
    else if (item.event.action == VFSAction::UNLINK)
    {
        if (!links_object->has(toString(item.getDestinationWalId())))
        {
            throw DB::Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Snapshot doesn't contains vfs_log_id object in entry for remote_path: {} vfs_log_id: {}, local_path {}",
                item.event.remote_path,
                item.getDestinationWalId(),
                item.event.local_path);
        }
        auto wal_object = links_object->getObject(toString(item.getDestinationWalId()));
        if (!wal_object->has(item.event.local_path))
        {
            throw DB::Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Snapshot doesn't contains vfs_log_id object in entry for remote_path: {} vfs_log_id: {}, local_path {}",
                item.event.remote_path,
                item.getDestinationWalId(),
                item.event.local_path);
        }

        auto local_path_object = wal_object->getObject(item.event.local_path);
        local_path_object->set("status", static_cast<std::underlying_type_t<VFSAction>>(item.event.action));
        local_path_object->set("timestamp", item.event.timestamp.epochMicroseconds());
        local_path_object->set("vfs_log_index", item.getDestinationWalIndex());

        link_count--;
        data_json->set("link_count", link_count);
    }
    else
    {
        UNREACHABLE();
    }
}

void VFSSnapshotEntry::updateWithVFSItems(const VFSLogItems & items_batch)
{
    for (const auto & item : items_batch)
    {
        updateWithVFSItem(item);
    }
}

void VFSSnapshotDataBase::writeEntryOrAddToRemove(
    const VFSSnapshotEntry & entry, WriteBuffer & write_buffer, VFSSnapshotEntries & entries_to_remove)
{
    if (entry.isAlive())
    {
        entry.serialize(write_buffer);
        return;
    }
    entries_to_remove.emplace_back(entry);
}


SnapshotMetadata VFSSnapshotDataBase::mergeWithWals(VFSLogItems && wal_items, const SnapshotMetadata & old_snapshot_meta)
{
    /// For most of object stroges (like s3 or azure) we don't need the object path, it's generated randomly.
    /// But other ones reqiested to set it manually.
    std::unique_ptr<ReadBuffer> shapshot_read_buffer = getShapshotReadBuffer(old_snapshot_meta);
    auto [new_shapshot_write_buffer, new_object_key] = getShapshotWriteBufferAndsnapshotObject(old_snapshot_meta);

    LOG_DEBUG(log, "Going to merge WAL batch(size {}) with snapshot (key {})", wal_items.size(), old_snapshot_meta.object_storage_key);
    auto entires_to_remove = mergeWithWalsImpl(std::move(wal_items), *shapshot_read_buffer, *new_shapshot_write_buffer);
    SnapshotMetadata new_snapshot_meta(new_object_key);

    LOG_DEBUG(log, "Merge snapshot have finished, going to remove {} from object storage", entires_to_remove.size());
    removeShapshotEntires(entires_to_remove);
    return new_snapshot_meta;
}


VFSSnapshotEntries VFSSnapshotDataBase::mergeWithWalsImpl(VFSLogItems && wal_items_, ReadBuffer & read_buffer, WriteBuffer & write_buffer)
{
    VFSSnapshotEntries entries_to_remove;

    auto wal_items = std::move(wal_items_);
    std::ranges::sort(
        wal_items,
        [](const VFSLogItem & left, const VFSLogItem & right)
        {
            if (left.event.remote_path != right.event.remote_path)
                return left.event.remote_path < right.event.remote_path;
            return left.getDestinationWalIndex() < right.getDestinationWalIndex();
        });


    auto current_snapshot_entry = VFSSnapshotEntry::deserialize(read_buffer);

    // Implementation similar to the merge operation:
    // Iterating thought 2 sorted vectors.
    // If the links count will be == 0, then add to entries_to_remove
    // Else perform sum and append into shapshot
    for (auto wal_iterator = wal_items.begin(); wal_iterator != wal_items.end();)
    {
        // Combine all wal items with the same remote_path into single one.
        VFSLogItems items_to_merge;
        const String remote_path_to_merge = wal_iterator->event.remote_path;

        while (wal_iterator != wal_items.end() && wal_iterator->event.remote_path == remote_path_to_merge)
        {
            items_to_merge.emplace_back(std::move(*wal_iterator));
            ++wal_iterator;
        }

        // Write and skip entries from snapshot which we won't update
        while (current_snapshot_entry && current_snapshot_entry->remote_path < remote_path_to_merge)
        {
            auto next_snapshot_entry = VFSSnapshotEntry::deserialize(read_buffer);
            if (next_snapshot_entry && current_snapshot_entry->remote_path > next_snapshot_entry->remote_path)
            {
                throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "VFS snapshot is not sorted.");
            }
            current_snapshot_entry->serialize(write_buffer);
            std::swap(current_snapshot_entry, next_snapshot_entry);
        }

        if (!current_snapshot_entry || current_snapshot_entry->remote_path > remote_path_to_merge)
        {
            /// There are no entry with such remote path in the shapshot yet. Create it.
            VFSSnapshotEntry new_entry(remote_path_to_merge);
            new_entry.updateWithVFSItems(items_to_merge);
            writeEntryOrAddToRemove(new_entry, write_buffer, entries_to_remove);
            continue;
        }
        else if (current_snapshot_entry->remote_path == remote_path_to_merge)
        {
            current_snapshot_entry->updateWithVFSItems(items_to_merge);
            writeEntryOrAddToRemove(*current_snapshot_entry, write_buffer, entries_to_remove);
            current_snapshot_entry = VFSSnapshotEntry::deserialize(read_buffer);
        }
        else
        {
            UNREACHABLE();
        }
    }

    while (current_snapshot_entry)
    {
        current_snapshot_entry->serialize(write_buffer);
        auto next_snapshot_entry = VFSSnapshotEntry::deserialize(read_buffer);

        if (next_snapshot_entry && current_snapshot_entry->remote_path > next_snapshot_entry->remote_path)
        {
            throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "VFS snapshot is not sorted.");
        }
        std::swap(current_snapshot_entry, next_snapshot_entry);
    }
    write_buffer.finalize();
    return entries_to_remove;
}

std::unique_ptr<ReadBuffer> VFSSnapshotDataFromObjectStorage::getShapshotReadBuffer(const SnapshotMetadata & snapshot_meta) const
{
    if (!snapshot_meta.is_initial_snapshot)
    {
        StoredObject object(snapshot_meta.object_storage_key, "", snapshot_meta.total_size);
        // to do read settings.
        auto res = object_storage->readObject(object, {});
        return res;
    }
    return std::make_unique<ReadBufferFromEmptyFile>();
}

std::pair<std::unique_ptr<WriteBuffer>, String>
VFSSnapshotDataFromObjectStorage::getShapshotWriteBufferAndsnapshotObject(const SnapshotMetadata & snapshot_meta) const
{
    String new_object_path = fmt::format("/vfs_shapshots/shapshot_{}", snapshot_meta.znode_version + 1);
    auto new_object_key = object_storage->generateObjectKeyForPath(new_object_path);
    StoredObject new_object(new_object_key.serialize());
    std::unique_ptr<WriteBuffer> new_shapshot_write_buffer = object_storage->writeObject(new_object, WriteMode::Rewrite);

    return {std::move(new_shapshot_write_buffer), new_object_key.serialize()};
}

void VFSSnapshotDataFromObjectStorage::removeShapshotEntires(const VFSSnapshotEntries & entires_to_remove)
{
    StoredObjects objects_to_remove;
    objects_to_remove.reserve(entires_to_remove.size());

    for (const auto & entry : entires_to_remove)
    {
        objects_to_remove.emplace_back(entry.remote_path);
    }
    object_storage->removeObjectsIfExist(objects_to_remove);
}

}
