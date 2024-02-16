#include "VFSSnapshotStorageFromS3.h"

#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
extern const Event VFSGcCumulativeSnapshotBytesRead;
}

namespace DB
{

static StoredObject makeSnapshotStoredObject(const String & name, const String & prefix)
{
    return StoredObject{ObjectStorageKey::createAsAbsolute(fmt::format("{}{}", prefix, name)).serialize()};
}

VFSSnapshotReadStreamFromS3::VFSSnapshotReadStreamFromS3(ObjectStoragePtr object_storage_, StoredObject snapshot_object_)
    : object_storage(object_storage_), snapshot_object(snapshot_object_)
{
}

VFSSnapshotReadStreamFromS3::entry_type VFSSnapshotReadStreamFromS3::nextImpl()
{
    if (!read_buffer.has_value())
        read_buffer.emplace(object_storage->readObject(snapshot_object));

    if (read_buffer->eof())
    {
        ProfileEvents::increment(ProfileEvents::VFSGcCumulativeSnapshotBytesRead, read_buffer->count());
        return {};
    }
    return VFSSnapshotEntryStringSerializer::deserialize(*read_buffer);
}

VFSSnapshotWriteStreamFromS3::VFSSnapshotWriteStreamFromS3(
    ObjectStoragePtr object_storage_, StoredObject snapshot_object_, int snapshot_lz4_compression_level_)
    : object_storage(object_storage_), snapshot_object(snapshot_object_), snapshot_lz4_compression_level(snapshot_lz4_compression_level_)
{
}

WriteBuffer & VFSSnapshotWriteStreamFromS3::getWriteBuffer()
{
    if (!write_buffer.has_value())
        write_buffer.emplace(object_storage->writeObject(snapshot_object, WriteMode::Rewrite), snapshot_lz4_compression_level);

    return *write_buffer;
}

void VFSSnapshotWriteStreamFromS3::writeImpl(VFSSnapshotEntry && entry)
{
    VFSSnapshotEntryStringSerializer::serialize(std::move(entry), getWriteBuffer());
}

void VFSSnapshotWriteStreamFromS3::finalizeImpl()
{
    getWriteBuffer().finalize();
}

VFSSnapshotStorageFromS3::VFSSnapshotStorageFromS3(ObjectStoragePtr object_storage_, String prefix_, const VFSSettings & settings)
    : object_storage(object_storage_), prefix(std::move(prefix_)), snapshot_lz4_compression_level(settings.snapshot_lz4_compression_level)
{
}

VFSSnapshotReadStreamPtr VFSSnapshotStorageFromS3::readSnapshot(const String & name)
{
    return std::make_unique<VFSSnapshotReadStreamFromS3>(object_storage, makeSnapshotStoredObject(name, prefix));
}
VFSSnapshotWriteStreamPtr VFSSnapshotStorageFromS3::writeSnapshot(const String & name)
{
    return std::make_unique<VFSSnapshotWriteStreamFromS3>(
        object_storage, makeSnapshotStoredObject(name, prefix), snapshot_lz4_compression_level);
}
VFSSnapshotSortingWriteStreamPtr VFSSnapshotStorageFromS3::writeSnapshotWithSorting(const String & name)
{
    return std::make_unique<VFSSnapshotSortingWriteStream>(writeSnapshot(name));
}

Strings VFSSnapshotStorageFromS3::listSnapshots() const
{
    Strings snapshots;
    RelativePathsWithMetadata objects;
    object_storage->listObjects(prefix, objects, /* max_keys= */ 0);

    for (const auto & obj : objects)
    {
        String name = obj.relative_path.substr(prefix.length());
        snapshots.push_back(name);
    }

    return snapshots;
}

size_t VFSSnapshotStorageFromS3::removeSnapshots(Strings names)
{
    StoredObjects objects_to_remove;

    for (const auto & name : names)
        objects_to_remove.push_back(makeSnapshotStoredObject(name, prefix));
    object_storage->removeObjectsIfExist(objects_to_remove);

    return names.size();
}

}
