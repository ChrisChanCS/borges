#pragma once

#include "RecoveryMaintenance.h"
#include <future>

namespace SharedData
{
    // One private instance per shard rebuild. Copies cover only immutable,
    // committed bytes; appends may extend the last segment while bulk copy runs.
    class OnlineBackup
    {
        ShardResources resources_;
        RecoveredShard snapshot_;
        std::map<std::uint32_t, std::uint32_t> copied_;
        std::uint32_t read_tail_ = 0;
        std::shared_future<void> bulk_;

        void copy_delta(const RecoveredShard &state)
        {
            auto *source = shared_pointer<char>(resources_.write_stream[0]);
            auto *target = shared_pointer<char>(resources_.write_stream[1]);
            for (const auto &[key, stream] : state.streams)
                for (std::size_t i = 0; i < stream.segments.size(); ++i)
                {
                    const auto &segment = stream.segments[i];
                    const auto bytes = segment.used + (i + 1 < stream.segments.size() ? sizeof(LinkPointer) : 0);
                    auto &copied = copied_[segment.offset];
                    if (copied > bytes)
                        throw std::runtime_error("stream was trimmed during backup rebuild");
                    copy_persistent_prefix(target + segment.offset + copied,
                                           source + segment.offset + copied, bytes - copied);
                    copied = bytes;
                }
            if (read_tail_ > state.read_tail)
                throw std::runtime_error("read log moved backwards during backup rebuild");
            copy_persistent_prefix(shared_pointer<char>(resources_.read_stream[1]) + read_tail_,
                                   shared_pointer<char>(resources_.read_stream[0]) + read_tail_, state.read_tail - read_tail_);
            read_tail_ = state.read_tail;
        }

    public:
        OnlineBackup(ShardResources resources, RecoveredShard snapshot, std::uint32_t shard)
            : resources_(resources), snapshot_(std::move(snapshot))
        {
            bulk_ = std::async(std::launch::async, [this, shard]
            {
                init_shard_cxlalloc(MAX_THREAD_CNT_PER_PROCESS - 3, shard);
                copy_delta(snapshot_);
            });
        }
        ~OnlineBackup() { bulk_.wait(); }

        bool ready()
        {
            if (bulk_.wait_for(std::chrono::seconds(0)) != std::future_status::ready)
                return false;
            // Surface copy failures before the sequencer parks readers/writers.
            bulk_.get();
            return true;
        }

        // Writers and readers are parked only for this final suffix copy.
        void finish(const RecoveredShard &state)
        {
            bulk_.get();
            copy_delta(state);
            auto *catalog = shared_pointer<StreamCatalogEntry>(resources_.stream_catalog);
            std::size_t cursor = 0;
            for (const auto &[key, stream] : state.streams)
                if (initial_stream(key).initial_segments == 0 && stream.initial_segments != 0)
                    append_stream_catalog(catalog, cursor, key, stream.head,
                        stream.first_lsn ? stream.first_lsn : stream.trim_lsn);
            if (cursor)
                clflushopt(catalog, cursor * sizeof(*catalog));
            sfence();
        }
    };
}
