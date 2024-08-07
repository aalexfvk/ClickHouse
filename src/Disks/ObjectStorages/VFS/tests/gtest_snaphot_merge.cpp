#include <algorithm>
#include <iostream>
#include <Disks/ObjectStorages/VFS/VFSShapshotMetadata.h>
#include <Disks/ObjectStorages/VFS/VFSSnapshot.h>
#include <IO/WriteBufferFromString.h>

#include <gtest/gtest.h>


using namespace DB;

class VFSSnapshotDataFromString : public DB::VFSSnapshotDataBase
{
public:
    VFSSnapshotDataFromString() = default;

    void rotateData()
    {
        old_snapshot_data = new_snapshot_data;
        new_snapshot_data = "";
    }
    String getSnapshotData() const { return new_snapshot_data; }
    VFSSnapshotEntries getEnriesToRemoveAfterLastMerge() const { return latest_entires_to_remove; }

protected:
    std::unique_ptr<ReadBuffer> getShapshotReadBuffer(const SnapshotMetadata &) const override
    {
        return std::unique_ptr<ReadBuffer>(new ReadBufferFromString(old_snapshot_data));
    }
    std::pair<std::unique_ptr<WriteBuffer>, String> getShapshotWriteBufferAndsnapshotObject(const SnapshotMetadata &) const override
    {
        String key_placeholed = "test";
        auto write_buffer = std::unique_ptr<WriteBuffer>(new WriteBufferFromString(new_snapshot_data));
        return {std::move(write_buffer), key_placeholed};
    }

    void removeShapshotEntires(const VFSSnapshotEntries & entires_to_remove) override { latest_entires_to_remove = entires_to_remove; }

private:
    String old_snapshot_data;
    mutable String new_snapshot_data;
    VFSSnapshotEntries latest_entires_to_remove;
};


struct LinkDescription
{
    UUID wal;
    String local_path;
    VFSAction action;

    bool operator==(const LinkDescription & rhs) const { return wal == rhs.wal && local_path == rhs.local_path && action == rhs.action; }
    friend bool operator<(const LinkDescription & l, const LinkDescription & r);
};

struct ReableVFSEntry
{
    String remote_path;
    Int32 links_count;
    std::vector<LinkDescription> links;

    static ReableVFSEntry createFromVFSEntry(VFSSnapshotEntry & entry)
    {
        ReableVFSEntry res;
        res.remote_path = entry.remote_path;
        res.links_count = entry.link_count;
        auto links_obj = entry.data_json->getObject("links");
        for (String wal_id : links_obj->getNames())
        {
            auto wal_obj = links_obj->getObject(wal_id);
            for (String local_path : wal_obj->getNames())
            {
                auto local_path_object = wal_obj->getObject(local_path);
                if (!local_path_object)
                    continue;
                auto status_from_entry = static_cast<VFSAction>(local_path_object->getValue<std::underlying_type_t<VFSAction>>("status"));
                res.links.push_back({parse<UUID>(wal_id), local_path, status_from_entry});
            }
        }
        return res;
    }
    bool operator==(const ReableVFSEntry & rhs) const
    {
        return remote_path == rhs.remote_path && links_count == rhs.links_count && links == rhs.links;
    }
    friend bool operator<(const ReableVFSEntry & l, const ReableVFSEntry & r);
};

bool operator<(const LinkDescription & l, const LinkDescription & r)
{
    if (l.wal != r.wal)
        return l.wal < r.wal;
    if (l.local_path != r.local_path)
        return l.local_path < r.local_path;

    return l.action < r.action;
}
bool operator<(const ReableVFSEntry & l, const ReableVFSEntry & r)
{
    if (l.remote_path != r.remote_path)
        return l.remote_path < r.remote_path;

    if (l.links_count != r.links_count)
        return l.links_count < r.links_count;
    return l.links < r.links;
}

using ReableVFSEntries = std::vector<ReableVFSEntry>;

std::ostream & operator<<(std::ostream & os, const ReableVFSEntries & entries)
{
    size_t ind = 0;
    for (const auto & e : entries)
    {
        os << "Entry : " << ind++ << "\n";
        os << e.remote_path << "\n";
        os << e.links_count << "\n";
        for (const auto & link : e.links)
        {
            os << toString(link.wal) << " " << link.local_path << " " << static_cast<UInt32>(link.action) << "\n";
        }
    }
    return os;
}

void checkSnapshotState(String snapshot_data, ReableVFSEntries & entries)
{
    ReadBufferFromString rb(snapshot_data);
    ReableVFSEntries entries_from_snapshot;
    auto vfs_entry = VFSSnapshotEntry::deserialize(rb);
    while (vfs_entry)
    {
        entries_from_snapshot.push_back(ReableVFSEntry::createFromVFSEntry(*vfs_entry));
        vfs_entry = VFSSnapshotEntry::deserialize(rb);
    }
    std::sort(entries_from_snapshot.begin(), entries_from_snapshot.end());
    std::sort(entries.begin(), entries.end());

    // std::cout << "entries_from_snapshot\n" << entries_from_snapshot << "\n";
    // std::cout << "entries\n" << entries << "\n";

    EXPECT_EQ(entries_from_snapshot, entries);
}

void checkSnaphotEntries(VFSSnapshotEntries snapshot_entries, ReableVFSEntries & entries)
{
    ReableVFSEntries entries_from_snapshot;
    for (auto & entry : snapshot_entries)
    {
        entries_from_snapshot.push_back(ReableVFSEntry::createFromVFSEntry(entry));
    }

    std::sort(entries_from_snapshot.begin(), entries_from_snapshot.end());
    std::sort(entries.begin(), entries.end());

    // std::cout << "entries_from_snapshot\n" << entries_from_snapshot << "\n";
    // std::cout << "entries\n" << entries << "\n";

    EXPECT_EQ(entries_from_snapshot, entries);
}

void runMergeAndValidateResults(
    VFSLogItems & items,
    ReableVFSEntries & expected_entries_to_remove,
    ReableVFSEntries & expected_state,
    VFSSnapshotDataFromString & snapshot_data,
    SnapshotMetadata snapshot_metadata = {})
{
    snapshot_data.rotateData();
    snapshot_data.mergeWithWals(std::move(items), snapshot_metadata);

    auto to_remove = snapshot_data.getEnriesToRemoveAfterLastMerge();
    checkSnaphotEntries(to_remove, expected_entries_to_remove);

    checkSnapshotState(snapshot_data.getSnapshotData(), expected_state);
}

const UUID TEST_WAL_ID_1 = parse<UUID>("00000000-0000-0000-0000-000000000001");
const UUID TEST_WAL_ID_2 = parse<UUID>("00000000-0000-0000-0000-000000000002");


VFSLogItem createLogItem(
    String remote_path,
    String local_path,
    VFSAction action,
    UUID id = TEST_WAL_ID_1,
    Poco::Timestamp timestamp = 0,
    String replica = "a.net",
    UInt64 index = 0)
{
    return {{remote_path, local_path, {}, timestamp, action}, {replica, id, index}};
}

TEST(DiskObjectStorageVFS, CheckInternalRepresenatation)
{
    String snapshot_state = "{\"link_count\":1,\"links\":{\"00000000-0000-0000-0000-000000000001\":{\"\\/"
                            "local\":{\"status\":0,\"timestamp\":0,\"vfs_log_index\":0}}},\"remote_path\":\"\\/a\"}\n";

    ReableVFSEntries expected_state = {{"/a", 1, {{TEST_WAL_ID_1, "/local", VFSAction::LINK}}}};
    checkSnapshotState(snapshot_state, expected_state);
}

TEST(DiskObjectStorageVFS, SimpleCheckSerializationLink)
{
    VFSLogItems items = {createLogItem("/a", "/local", VFSAction::LINK)};
    ReableVFSEntries expected_entries_to_remove = {};
    ReableVFSEntries expected_state = {{"/a", 1, {{TEST_WAL_ID_1, "/local", VFSAction::LINK}}}};
    VFSSnapshotDataFromString snapshot_data;

    runMergeAndValidateResults(items, expected_entries_to_remove, expected_state, snapshot_data);
}

TEST(DiskObjectStorageVFS, ExceptionOnUnlinkNonExistingPath)
{
    VFSLogItems items = {createLogItem("/a", "/local", VFSAction::UNLINK)};

    VFSSnapshotDataFromString snapshot_data;
    SnapshotMetadata placeholder_metadata;

    snapshot_data.rotateData();
    EXPECT_THROW(snapshot_data.mergeWithWals(std::move(items), placeholder_metadata), Exception);
}

TEST(DiskObjectStorageVFS, ReplcaceLinkAfterRequest)
{
    VFSLogItems items;
    ReableVFSEntries expected_entries_to_remove;
    ReableVFSEntries expected_state;
    VFSSnapshotDataFromString snapshot_data;

    items = {{{"/a", "/local", WALInfo{"", TEST_WAL_ID_2, 0}, 0, VFSAction::REQUEST}, {"", TEST_WAL_ID_1, 0}}};
    expected_entries_to_remove = {};
    expected_state = {{"/a", 1, {{TEST_WAL_ID_2, "/local", VFSAction::REQUEST}}}};

    runMergeAndValidateResults(items, expected_entries_to_remove, expected_state, snapshot_data);
    items = {{{"/a", "/local", {}, 0, VFSAction::LINK}, {"", TEST_WAL_ID_2, 0}}};
    expected_entries_to_remove = {};
    expected_state = {{"/a", 1, {{TEST_WAL_ID_2, "/local", VFSAction::LINK}}}};

    runMergeAndValidateResults(items, expected_entries_to_remove, expected_state, snapshot_data);
}

TEST(DiskObjectStorageVFS, LinkUnlinkSameFile)
{
    // VFSLogItems items = {createLogItem("/a", "/local", VFSAction::LINK), createLogItem("/a", "/local", VFSAction::UNLINK)};

    // VFSSnapshotDataFromString snapshot_data;
    // SnapshotMetadata placeholder_metadata;
    // {
    //     snapshot_data.rotateData();
    //     snapshot_data.mergeWithWals(std::move(items), placeholder_metadata);

    //     auto to_remove = snapshot_data.getEnriesToRemoveAfterLastMerge();
    //     ReableVFSEntries expected_entries_to_remove = {{"/a", 0, {{TEST_WAL_ID_1, "/local", VFSAction::UNLINK}}}};

    //     checkSnaphotEntries(to_remove, expected_entries_to_remove);
    //     items = {};
    // }
    // ReableVFSEntries expected_state = {};
    // checkSnapshotState(snapshot_data.getSnapshotData(), expected_state);

    VFSLogItems items;
    ReableVFSEntries expected_entries_to_remove;
    ReableVFSEntries expected_state;
    VFSSnapshotDataFromString snapshot_data;

    items = {createLogItem("/a", "/local", VFSAction::LINK), createLogItem("/a", "/local", VFSAction::UNLINK)};
    expected_entries_to_remove = {{"/a", 0, {{TEST_WAL_ID_1, "/local", VFSAction::UNLINK}}}};
    expected_state = {};

    runMergeAndValidateResults(items, expected_entries_to_remove, expected_state, snapshot_data);
}


TEST(DiskObjectStorageVFS, MergeWalWithsnapshot)
{
    VFSLogItems items;
    ReableVFSEntries expected_entries_to_remove;
    ReableVFSEntries expected_state;
    VFSSnapshotDataFromString snapshot_data;

    items
        = {createLogItem("/b", "/local", VFSAction::LINK),
           createLogItem("/a", "/local", VFSAction::LINK),
           createLogItem("/a", "/local1", VFSAction::LINK),
           createLogItem("/c", "/local2", VFSAction::LINK),
           createLogItem("/c", "/local", VFSAction::LINK)};

    expected_entries_to_remove = {};
    expected_state
        = {{"/b", 1, {{TEST_WAL_ID_1, "/local", VFSAction::LINK}}},
           {"/a", 2, {{TEST_WAL_ID_1, "/local", VFSAction::LINK}, {TEST_WAL_ID_1, "/local1", VFSAction::LINK}}},
           {"/c", 2, {{TEST_WAL_ID_1, "/local", VFSAction::LINK}, {TEST_WAL_ID_1, "/local2", VFSAction::LINK}}}};

    runMergeAndValidateResults(items, expected_entries_to_remove, expected_state, snapshot_data);

    items
        = {createLogItem("/b", "/local", VFSAction::UNLINK),
           createLogItem("/a", "/local1", VFSAction::UNLINK),
           createLogItem("/c", "/local3", VFSAction::LINK)};

    expected_entries_to_remove = {{"/b", 0, {{TEST_WAL_ID_1, "/local", VFSAction::UNLINK}}}};
    expected_state
        = {{"/a", 1, {{TEST_WAL_ID_1, "/local", VFSAction::LINK}, {TEST_WAL_ID_1, "/local1", VFSAction::UNLINK}}},
           {"/c",
            3,
            {{TEST_WAL_ID_1, "/local", VFSAction::LINK},
             {TEST_WAL_ID_1, "/local2", VFSAction::LINK},
             {TEST_WAL_ID_1, "/local3", VFSAction::LINK}}}};
    runMergeAndValidateResults(items, expected_entries_to_remove, expected_state, snapshot_data);

    items
        = {createLogItem("/a", "/local", VFSAction::UNLINK),
           createLogItem("/c", "/local", VFSAction::UNLINK),
           createLogItem("/c", "/local2", VFSAction::UNLINK),
           createLogItem("/c", "/local3", VFSAction::UNLINK)};

    expected_entries_to_remove
        = {{"/a", 0, {{TEST_WAL_ID_1, "/local", VFSAction::UNLINK}, {TEST_WAL_ID_1, "/local1", VFSAction::UNLINK}}},
           {"/c",
            0,
            {{TEST_WAL_ID_1, "/local", VFSAction::UNLINK},
             {TEST_WAL_ID_1, "/local2", VFSAction::UNLINK},
             {TEST_WAL_ID_1, "/local3", VFSAction::UNLINK}}}};

    expected_state = {};

    runMergeAndValidateResults(items, expected_entries_to_remove, expected_state, snapshot_data);
}

TEST(DiskObjectStorageVFS, CheckSkipByWALIndex)
{
    VFSLogItems items;
    ReableVFSEntries expected_entries_to_remove;
    ReableVFSEntries expected_state;
    VFSSnapshotDataFromString snapshot_data;
    SnapshotMetadata snapshot_metadata;


    items = {{{"/a", "/local", {}, 0, VFSAction::LINK}, {"", TEST_WAL_ID_1, 0}}};

    expected_entries_to_remove = {};
    expected_state = {};
    snapshot_metadata.processed_logs_indices = {{TEST_WAL_ID_1, 10}};

    runMergeAndValidateResults(items, expected_entries_to_remove, expected_state, snapshot_data, snapshot_metadata);
}
