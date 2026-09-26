#pragma once

#include "../common/Macro.h"
#include "../common/SharedData.h"
#include <absl/container/flat_hash_map.h>
#ifndef USE_RDMA
#include <cxlalloc.h>
#endif
#include "../benchmark/counter/Replay.h"
#include "../benchmark/lock/Replay.h"
#include "../benchmark/retwis/Replay.h"
#include "../benchmark/ycsb/Replay.h"
#include "../common/io.h"
#include "../common/ClientConnections.h"
#ifndef USE_RDMA
#include "../common/MaintenanceControl.h"
#endif
#include <condition_variable>
#include <queue>
#include "Reader.h"
#include <absl/container/btree_set.h>
#include <absl/container/flat_hash_set.h>
#include <future>
#include <glog/logging.h>
#include <latch>
#include <roaring/roaring.hh>
#include <shared_mutex>
#include <thread>

std::vector<absl::Time> start_time;
std::vector<absl::Time> end_time;

struct RequestQueue {
  std::queue<std::shared_ptr<ReqHeader>> request_queue_;
  absl::Mutex mu_;
};

template <typename T, typename F> class Responser {
private:
  WorkloadType workload_type_;
  absl::Mutex mu_;
  std::uint32_t shard_id_;
  std::uint32_t committed_lsn_{0};
  SharedData::SwitchFlag **switch_flag_;
  SharedData::SwitchFlag *sequencer_switch_flag_;
#ifdef USE_RDMA
  uint64_t *switch_flag_offset_;
  uint64_t sequencer_switch_flag_offset_;
#endif

  std::uint32_t epoch_{0};

  // All shards share one hash table
  star::CCHashTable *hash_table[COMMIT_TAIL_HASH_TABLE_CANDIDATE_NUM];
#ifdef USE_RDMA
  uint64_t hash_table_offset_[COMMIT_TAIL_HASH_TABLE_CANDIDATE_NUM];
#endif

  absl::Mutex *connections_mutex_;
  absl::flat_hash_map<std::uint32_t, io_utils::ClientConnection *>
      *client_connections_;

  void send_to_client(std::uint32_t client_id, const char *buffer, std::size_t size) {
    io_utils::LockedClientConnection connection(*client_connections_, *connections_mutex_, client_id);
    if (!connection) {
      LOG(WARNING) << "Response discarded for disconnected client " << client_id;
      return;
    }
    io_utils::SendData(connection.fd(), buffer, size);
  }

  struct AckQueue {
    std::queue<std::shared_ptr<ReadContent<F>>> ack_queue;
    std::mutex ack_queue_mutex;
    std::condition_variable ack_queue_cv_;
    std::int32_t fd;
  };

  std::shared_mutex ack_send_queue_mutex;
  absl::flat_hash_map<std::uint32_t, AckQueue *>
      ack_send_queue; // acks fetched from ack_queue to be sent to each client
  GSNCollector *gsn_collector_;

  std::vector<std::thread> request_worker_threads_;

  ReaderManager<T, F> reader_manager_;
  RequestQueue *request_queue_;
  std::vector<std::thread> ack_client_threads_;

public:
#ifndef USE_RDMA
  SharedData::MaintenanceBarrier maintenance_readers;
  void prepare_lifecycle() { reader_manager_.preserve_cxl_read_results(); }
  void invalidate_stream(StateKey key) { reader_manager_.invalidate_stream(key); }
#endif
  Responser(std::uint32_t shard_id, WorkloadType workload_type)
      : workload_type_(workload_type), shard_id_(shard_id),
        reader_manager_(shard_id, workload_type) {
    gsn_collector_ = new GSNCollector(shard_id_, REPLICATOR_NUM);
    request_queue_ = new RequestQueue[REQUEST_WORKER_NUM];
#ifdef USE_RDMA
    // Under RDMA, switch_flag_ and sequencer_switch_flag_ are local dummy
    // structs. All actual reads/writes go through switch_flag_offset_ /
    // sequencer_switch_flag_offset_ via RDMA.
    switch_flag_ = new SharedData::SwitchFlag *[REQUEST_WORKER_NUM];
    for (std::uint32_t i = 0; i < REQUEST_WORKER_NUM; i++) {
      switch_flag_[i] =
          new SharedData::SwitchFlag(); // local dummy (never actually used for
                                        // CXL ops)
    }
    sequencer_switch_flag_ = new SharedData::SwitchFlag(); // local dummy
    for (std::uint32_t i = 0; i < COMMIT_TAIL_HASH_TABLE_CANDIDATE_NUM; i++) {
      hash_table[i] = nullptr; // not used under RDMA
    }
    switch_flag_offset_ = new uint64_t[REQUEST_WORKER_NUM];
    for (std::uint32_t i = 0; i < REQUEST_WORKER_NUM; i++) {
      switch_flag_offset_[i] = g_rdma.get_root(
          SWITCH_FLAG_ROOT_INDEX + shard_id_ * REQUEST_WORKER_NUM + i);
    }
    sequencer_switch_flag_offset_ = g_rdma.get_root(
        SWITCH_FLAG_ROOT_INDEX + SHARD_SERVER_NUM * REQUEST_WORKER_NUM);
    for (std::uint32_t i = 0; i < COMMIT_TAIL_HASH_TABLE_CANDIDATE_NUM; i++) {
      hash_table_offset_[i] = g_rdma.get_root(TAIL_HASH_TABLE_ROOT_INDEX + i);
    }
#else
    switch_flag_ = new SharedData::SwitchFlag *[REQUEST_WORKER_NUM];
    for (std::uint32_t i = 0; i < REQUEST_WORKER_NUM; i++) {
      switch_flag_[i] =
          SharedData::shared_pointer<SharedData::SwitchFlag>(SharedData::shard_resources(shard_id_).reader_flags) + i;
    }
    sequencer_switch_flag_ =
        reinterpret_cast<SharedData::SwitchFlag *>(cxlalloc_get_root(
            SWITCH_FLAG_ROOT_INDEX + SHARD_SERVER_NUM * REQUEST_WORKER_NUM));
    for (std::uint32_t i = 0; i < COMMIT_TAIL_HASH_TABLE_CANDIDATE_NUM; i++) {
      hash_table[i] = reinterpret_cast<star::CCHashTable *>(
          cxlalloc_get_root(TAIL_HASH_TABLE_ROOT_INDEX + i));
    }
#endif
  }
  ~Responser() {}
  std::uint32_t get_shard_id() { return shard_id_; }
  void check_and_create_ack_client_thread(std::uint32_t client_id,
                                          const int &fd) {
    bool is_new_client = false;
    {
      std::shared_lock<std::shared_mutex> lock(ack_send_queue_mutex);
      if (ack_send_queue.find(client_id) == ack_send_queue.end()) {
        is_new_client = true;
      }
    }
    if (is_new_client) {
      std::unique_lock<std::shared_mutex> lock(ack_send_queue_mutex);
      ack_send_queue[client_id] = new AckQueue();
      ack_send_queue[client_id]->fd = fd;
      ack_client_threads_.emplace_back(&Responser::ack_client_thread, this,
                                       client_id, ack_send_queue[client_id]);
    }
  }
  void ack_client_thread(std::uint32_t client_id, AckQueue *ack_queue) {
    while (true) {
      std::shared_ptr<ReadContent<F>> response_ptr;
      {
        std::unique_lock<std::mutex> lock(ack_queue->ack_queue_mutex);
        while (ack_queue->ack_queue.empty()) {
          ack_queue->ack_queue_cv_.wait(
              lock, [&ack_queue]() { return !ack_queue->ack_queue.empty(); });
        }
        response_ptr = ack_queue->ack_queue.front();
        ack_queue->ack_queue.pop();
      }
      // Because each client is closed-loop, each client has only one request in flight at any time
      io_utils::SendMessage<ReadContent<F>>(ack_queue->fd, *response_ptr);
    }
  }
  void set_client_connections(
      absl::flat_hash_map<std::uint32_t, io_utils::ClientConnection *>
          *client_connections) {
    client_connections_ = client_connections;
  }
  void set_connections_mutex(absl::Mutex *connections_mutex) {
    connections_mutex_ = connections_mutex;
  }
  void receive_request(std::uint32_t &lsn, std::uint32_t &client_id,
                       std::shared_ptr<ReqHeader> header) {
    std::uint32_t worker_id;
    if (WORKLOAD == 0) {
      worker_id = header->client_id % REQUEST_WORKER_NUM;
    } else {
      worker_id = header->state_key % REQUEST_WORKER_NUM;
    }
    {
      absl::MutexLock lock(&request_queue_[worker_id].mu_);
      request_queue_[worker_id].request_queue_.push(header);
    }
  }
  void receive_reader(std::uint32_t &lsn, std::uint32_t &client_id,
                      std::shared_ptr<ReqHeader> header) {
    reader_manager_.receive_reader(header->field_id, lsn, client_id,
                                   header->request_id, header->operation_id);
  }

  void create_view_for_new_state_key(StateKey &state_key,
                                     std::uint64_t &offset) {
    reader_manager_.create_view_for_new_state_key(state_key, offset);
  }

#ifdef USE_RDMA
  // RDMA-aware read of sequencer switch flag value (equivalent to
  // read_switch_flag_value)
  std::uint64_t rdma_read_switch_flag_value() {
    void *staging = g_rdma.get_staging_buf();
    g_rdma.read(staging, sequencer_switch_flag_offset_, sizeof(uint64_t));
    uint64_t val;
    std::memcpy(&val, staging, sizeof(uint64_t));
    return val;
  }

  // RDMA-aware reader lock (equivalent to
  // reader_lock_switch_flag_with_version_number): Read seq flag, store in own
  // reader flag, re-read to verify (retry if changed).
  std::uint64_t rdma_reader_lock_switch_flag(std::uint32_t worker_id) {
    void *staging = g_rdma.get_staging_buf();
    while (true) {
      g_rdma.read(staging, sequencer_switch_flag_offset_, sizeof(uint64_t));
      uint64_t seq_val;
      std::memcpy(&seq_val, staging, sizeof(uint64_t));
      // Claim: store seq_val in our own reader switch flag
      std::memcpy(staging, &seq_val, sizeof(uint64_t));
      g_rdma.write(staging, switch_flag_offset_[worker_id], sizeof(uint64_t));
      // Verify: re-read sequencer flag
      g_rdma.read(staging, sequencer_switch_flag_offset_, sizeof(uint64_t));
      uint64_t seq_val2;
      std::memcpy(&seq_val2, staging, sizeof(uint64_t));
      if (seq_val2 == seq_val) {
        return seq_val; // lock acquired
      }
      // Sequencer switched; release our claim and retry
      uint64_t zero = 0;
      std::memcpy(staging, &zero, sizeof(uint64_t));
      g_rdma.write(staging, switch_flag_offset_[worker_id], sizeof(uint64_t));
    }
  }

  // RDMA-aware reader unlock (equivalent to reader_unlock_switch_flag)
  void rdma_reader_unlock_switch_flag(std::uint32_t worker_id) {
    void *staging = g_rdma.get_staging_buf();
    uint64_t zero = 0;
    std::memcpy(staging, &zero, sizeof(uint64_t));
    g_rdma.write(staging, switch_flag_offset_[worker_id], sizeof(uint64_t));
  }

  // RDMA search in flat AllShardInfo table: same semantics as
  // CCHashTable::search
  void rdma_search_flat_table(std::uint32_t hash_table_id, StateKey state_key,
                              ViewOfShard view_of_shard[SHARD_SERVER_NUM]) {
    void *staging = g_rdma.get_staging_buf();
    uint64_t entry_off =
        hash_table_offset_[hash_table_id] +
        static_cast<uint64_t>(state_key) * sizeof(star::AllShardInfo);
    g_rdma.read(staging, entry_off, sizeof(star::AllShardInfo));
    star::AllShardInfo all_info;
    std::memcpy(&all_info, staging, sizeof(star::AllShardInfo));
    for (std::uint32_t i = 0; i < SHARD_SERVER_NUM; i++) {
      view_of_shard[i].last_tail_offset_ = all_info.shard_infos[i].cur_offset_;
    }
  }
#endif

  void process_entry(char *entry, View::View<T> *view) {
    std::uint32_t lsn = reinterpret_cast<std::uint64_t *>(entry)[0] >> 32;
    if constexpr (std::is_same_v<T, star::ycsb::State>) {
      star::ycsb::process_entry(entry, view);
    } else if constexpr (std::is_same_v<T, star::lock::State>) {
      if (WORKLOAD == 2) {
        star::lock::process_entry(entry, view);
      } else {
        LOG(ERROR) << "Invalid workload type: " << WORKLOAD;
        exit(-1);
      }
    } else if constexpr (std::is_same_v<T, star::counter::State>) {
      if (WORKLOAD == 3) {
        star::counter::process_entry(entry, view);
      } else {
        LOG(ERROR) << "Invalid workload type: " << WORKLOAD;
        exit(-1);
      }
    } else if constexpr (std::is_same_v<T, star::retwis::State>) {
      if (WORKLOAD == 4) {
        star::retwis::process_entry(entry, view);
      } else {
        LOG(ERROR) << "Invalid workload type: " << WORKLOAD;
        exit(-1);
      }
    } else {
      LOG(ERROR) << "Invalid workload type: " << WORKLOAD;
      exit(-1);
    }
  }

  void send_read_response(std::shared_ptr<ReqHeader> request,
                          View::View<T> *view, char *buffer,
                          std::uint32_t &lsn, GSNCollector *gsn_collector) {
    if constexpr (std::is_same_v<T, star::ycsb::State>) {
      ReadContent<F> *read_content_ptr =
          reinterpret_cast<ReadContent<F> *>(buffer);
      read_content_ptr->request_id = request->request_id;
      char *value = star::ycsb::get_field(view, request->field_id);
      std::memcpy(read_content_ptr->value, value, sizeof(F));
      send_to_client(request->client_id, buffer, sizeof(ReadContent<F>));
    } else if constexpr (std::is_same_v<T, star::lock::State>) {
      SingleWriteResHeader *response_ptr =
          reinterpret_cast<SingleWriteResHeader *>(buffer);
      bool success = star::lock::get_result(view, request->is_write,
                                            request->operation_id);
      if (success) {
        response_ptr->is_write = true;
      } else {
        response_ptr->is_write = false;
      }
      response_ptr->request_id = request->request_id;
      send_to_client(request->client_id, buffer, sizeof(SingleWriteResHeader));
    } else if constexpr (std::is_same_v<T, star::counter::State>) {
      if (WORKLOAD == 3) {
        ReadContent<F> *read_content_ptr =
            reinterpret_cast<ReadContent<F> *>(buffer);
        read_content_ptr->request_id = request->request_id;
        char *value = star::counter::get_value(view);
        std::memcpy(
            &read_content_ptr->value, value,
            sizeof(
                F)); // when compiling, value is uint32_t, so we need to use &
        send_to_client(request->client_id, buffer, sizeof(ReadContent<F>));
      } else {
        LOG(ERROR) << "Invalid workload type: " << WORKLOAD;
        exit(-1);
      }
    } else if constexpr (std::is_same_v<T, star::retwis::State>) {

      std::shared_ptr<Reader<T>> reader =
          reader_manager_.get_reader(request->operation_id);
      // Clients retry on their original logical shard. Recovery restores the
      // earliest read LSN from that shard's read log before serving requests.
      {
        absl::MutexLock lock(&reader->mu_);
        // If lsn equals reader->min_lsn_, this is the first read; fetch the value from the view,
        // and assign it to reader->value_. If lsn does not equal reader->min_lsn_, this is a duplicate read,
        // directly fetch the previously read value from reader
        if (lsn == reader->min_lsn_) {
          reader->ready_to_read_ = true;
          reader->value_ = star::retwis::get_value(view, reader->field_id_);
        } else {
          if (reader->ready_to_read_ != true) {
#ifndef USE_RDMA
            recover_read_value(request, view, *reader, gsn_collector);
#else
            LOG(ERROR) << "reader should be ready to read, lsn: " << lsn
                       << ", min_lsn: " << reader->min_lsn_;
            exit(-1);
#endif
          }
        }
      }

      if (view->state_key_ < USER_CNT) {
        // This is a user
        if (request->field_id == 0) {
          // This is a password field
          ReadContent<std::uint32_t> *read_content_ptr =
              reinterpret_cast<ReadContent<std::uint32_t> *>(buffer);
          read_content_ptr->request_id = request->request_id;
          {
            absl::MutexLock lock(&reader->mu_);
            std::memcpy(&read_content_ptr->value, reader->value_,
                        sizeof(std::uint32_t));
          }
          send_to_client(request->client_id, buffer, sizeof(ReadContent<std::uint32_t>));
        } else if (request->field_id == 1) {
          // This is a followers field
          ReadContent<std::uint32_t[FOLLOWERS_CNT]> *read_content_ptr =
              reinterpret_cast<ReadContent<std::uint32_t[FOLLOWERS_CNT]> *>(
                  buffer);
          read_content_ptr->request_id = request->request_id;
          {
            absl::MutexLock lock(&reader->mu_);
            std::memcpy(&read_content_ptr->value, reader->value_,
                        sizeof(std::uint32_t) * FOLLOWERS_CNT);
          }
          send_to_client(request->client_id, buffer, sizeof(ReadContent<std::uint32_t[FOLLOWERS_CNT]>));
        } else {
          LOG(ERROR) << "invalid field_id: " << request->field_id
                     << " for user";
          exit(-1);
        }
      } else if (view->state_key_ < 2 * USER_CNT) {
        // This is a timeline
        ReadContent<std::uint64_t[MAX_RETURN_POST_CNT]> *read_content_ptr =
            reinterpret_cast<ReadContent<std::uint64_t[MAX_RETURN_POST_CNT]> *>(
                buffer);
        read_content_ptr->request_id = request->request_id;
        {
          absl::MutexLock lock(&reader->mu_);
          std::memcpy(&read_content_ptr->value, reader->value_,
                      sizeof(std::uint64_t) * MAX_RETURN_POST_CNT);
        }
        send_to_client(request->client_id, buffer, sizeof(ReadContent<std::uint64_t[MAX_RETURN_POST_CNT]>));
      } else {
        // This is a post
        ReadContent<char[CONTENT_SIZE]> *read_content_ptr =
            reinterpret_cast<ReadContent<char[CONTENT_SIZE]> *>(buffer);
        read_content_ptr->request_id = request->request_id;
        {
          absl::MutexLock lock(&reader->mu_);
          std::memcpy(&read_content_ptr->value, reader->value_,
                      sizeof(char) * CONTENT_SIZE);
        }
        send_to_client(request->client_id, buffer, sizeof(ReadContent<char[CONTENT_SIZE]>));
      }
    } else {
      LOG(ERROR) << "Invalid workload type: " << WORKLOAD;
      exit(-1);
    }
  }

  void process_write_request(
      absl::InlinedVector<std::shared_ptr<ReqHeader>, 16> &request_batch,
      char *buffer) {
    ResHeader *response_ptr = reinterpret_cast<ResHeader *>(buffer);
    response_ptr->is_write = true;
    response_ptr->completed_req_cnt = request_batch.size();
    int index = 0;
    for (auto &request : request_batch) {
      std::memcpy(buffer + sizeof(ResHeader) + index * sizeof(std::uint32_t),
                  &request->request_id, sizeof(std::uint32_t));
      index++;
    }
    send_to_client(request_batch[0]->client_id, buffer, sizeof(ResHeader) + request_batch.size() * sizeof(std::uint32_t));
  }

  void process_read_request(
      absl::InlinedVector<std::shared_ptr<ReqHeader>, 16> &request_batch,
      GSNCollector *gsn_collector, char *buffer, bool is_new_state_key,
      std::uint64_t &round_number, std::uint32_t worker_id) {
    View::View<T> *view = reader_manager_.get_view(request_batch[0]->state_key);
    if (WORKLOAD == 1) {

#ifdef USE_RDMA
      std::uint64_t switch_flag_value = rdma_read_switch_flag_value();
#else
      std::uint64_t switch_flag_value =
          SharedData::read_switch_flag_value(sequencer_switch_flag_);
#endif
      // Drop only the index selector. Keep both epoch and publication version
      // so either kind of publication invalidates a cached view.
      std::uint64_t version_number = (switch_flag_value >> 1);
      const bool need_read = version_number > view->publication_version_;
      CHECK_GE(version_number, view->publication_version_)
          << "publication version moved backwards";
      if (need_read) {
        // 1. lock; 2. read hash table; 3. unlock; 4. k-way merge
#ifdef USE_RDMA
        std::uint64_t value = rdma_reader_lock_switch_flag(worker_id);
        std::uint8_t hash_table_id = value & 1;
        rdma_search_flat_table(hash_table_id, view->state_key_,
                               view->view_of_shard_);
        rdma_reader_unlock_switch_flag(worker_id);
#else
        std::uint64_t value =
            SharedData::reader_lock_switch_flag_with_version_number(
                sequencer_switch_flag_, switch_flag_[worker_id]);
        std::uint8_t hash_table_id = value & 1;
        view->ensure_configuration(gsn_collector->configuration_for_switch(value));
        gsn_collector->index_for_switch(value)->search_cached(view->state_key_, view->view_of_shard_.data(),
            view->shard_count(), view->index_lookup_[hash_table_id]);
        SharedData::reader_unlock_switch_flag(switch_flag_[worker_id]);
#endif
        std::uint64_t version_number = (value >> 1);
        k_way_merge_scaling_no_eos(request_batch[0], view, buffer,
                                   gsn_collector);
        view->publication_version_ = version_number;
      }
      std::shared_ptr<ReadContent<F>> response_ptr =
          std::make_shared<ReadContent<F>>();
      response_ptr->request_id = request_batch[0]->request_id;
      if constexpr (std::is_same_v<T, star::ycsb::State>) {
        std::memcpy(response_ptr->value,
                    star::ycsb::get_field(view, request_batch[0]->field_id),
                    sizeof(F));
      } else {
        LOG(ERROR) << "Invalid workload type: " << WORKLOAD;
        exit(-1);
      }
      AckQueue *ack_queue = nullptr;
      {
        std::shared_lock<std::shared_mutex> lock(ack_send_queue_mutex);
        if (ack_send_queue.find(request_batch[0]->client_id) ==
            ack_send_queue.end()) {
          LOG(ERROR) << "client_id " << request_batch[0]->client_id
                     << " not found in ack_send_queue";
          exit(-1);
        } else {
          ack_queue = ack_send_queue[request_batch[0]->client_id];
        }
      }
      {
        std::unique_lock<std::mutex> lock(ack_queue->ack_queue_mutex);
        ack_queue->ack_queue.push(response_ptr);
      }
      ack_queue->ack_queue_cv_.notify_all();
    } else {

      if (is_new_state_key) {
#ifdef USE_RDMA
        std::uint64_t switch_flag_value =
            rdma_reader_lock_switch_flag(worker_id);
        std::uint8_t hash_table_id = switch_flag_value & 1;
        rdma_search_flat_table(hash_table_id, view->state_key_,
                               view->view_of_shard_);
        rdma_reader_unlock_switch_flag(worker_id);
#else
        std::uint64_t switch_flag_value = SharedData::reader_lock_switch_flag_with_version_number(
            sequencer_switch_flag_, switch_flag_[worker_id]);
        view->read_index(gsn_collector->index_for_switch(switch_flag_value),
                         gsn_collector->configuration_for_switch(switch_flag_value));
        SharedData::reader_unlock_switch_flag(switch_flag_[worker_id]);
#endif
      }

      k_way_merge_single_read_request(request_batch[0], view, buffer,
                                      gsn_collector);
    }
  }

  void process_single_write_request(std::shared_ptr<ReqHeader> request,
                                    char *buffer) {
    SingleWriteResHeader *response_ptr =
        reinterpret_cast<SingleWriteResHeader *>(buffer);
    response_ptr->is_write = true;
    response_ptr->request_id = request->request_id;
    LOG_CLASS("Responser of shard {}", shard_id_,
              "send response request id: {}", request->request_id);
    send_to_client(request->client_id, buffer, sizeof(SingleWriteResHeader));
  }

  void RequestWorkerThread(std::uint32_t worker_id) {
    // TODO: thread id still needs to be passed in
    SharedData::SwitchFlag *switch_flag = switch_flag_[worker_id];
    SharedData::SwitchFlag *sequencer_switch_flag = sequencer_switch_flag_;
    GSNCollector gsn_collector(shard_id_, REPLICATOR_NUM + worker_id);
    char buffer[1024];
    std::uint32_t last_committed_lsn = 0;
    absl::InlinedVector<std::shared_ptr<ReqHeader>, 16> request_batch;
    roaring::Roaring state_key_bitmap;
    std::uint32_t next_process_lsn = 0;
    std::uint64_t round_number = 0;
    // Keep the ordinary loop inlined; only the epoch observer reenters it to
    // drain admitted requests before parking for maintenance.
    auto drain = [&](std::uint32_t committed_lsn) __attribute__((always_inline)) {
      StateKey state_key;
      std::uint32_t client_id;
      round_number = gsn_collector.get_global_cut_round();
      {
        state_key_bitmap = roaring::Roaring();
        last_committed_lsn = committed_lsn;
        std::shared_ptr<ReqHeader> request;
        bool is_write = false;
        while (true) {
          bool has_read_index = false;
          if (WORKLOAD == 0) {
            // append-only test means treating read/write requests as write
            // requests
            {
              absl::MutexLock lock(&request_queue_[worker_id].mu_);
              if (request_queue_[worker_id].request_queue_.empty() ||
                  request_queue_[worker_id].request_queue_.front()->lsn >
                      committed_lsn) {
                break;
              }
              request = request_queue_[worker_id].request_queue_.front();
              request_queue_[worker_id].request_queue_.pop();
            }
            LOG_CLASS("Responser of shard {}", shard_id_,
                      "receive global cut, request id: {}", committed_lsn);
            process_single_write_request(request, buffer);
          } else if (WORKLOAD == 1) {
            {
              absl::MutexLock lock(&request_queue_[worker_id].mu_);
              if (request_queue_[worker_id].request_queue_.empty() ||
                  request_queue_[worker_id].request_queue_.front()->lsn >
                      committed_lsn) {
                break;
              } else {
                request = request_queue_[worker_id].request_queue_.front();
                is_write = request->is_write;

                if (is_write) {
                  client_id = request->client_id;
                  request_queue_[worker_id].request_queue_.pop();
                } else {
                  state_key = request->state_key;
                  request_batch.push_back(request);
                  request_queue_[worker_id].request_queue_.pop();

                }
              }
            }
            if (is_write) {
              process_single_write_request(request, buffer);
            } else {
              process_read_request(request_batch, &gsn_collector, buffer, false,
                                   round_number, worker_id);
            }
            request_batch.clear();
          } else if (WORKLOAD == 2 || WORKLOAD == 3) {
            // In the lock workload, the responser treats lock requests as read
            // requests because each lock is followed by a read-lock-status request. unlock
            // requests are treated as write requests,
            // because an unlock request is received only after lock success and does not need replay
            // is_write in the request header indicates lock or unlock,
            // and does not affect responser logic
            {
              absl::MutexLock lock(&request_queue_[worker_id].mu_);
              if (request_queue_[worker_id].request_queue_.empty() ||
                  request_queue_[worker_id].request_queue_.front()->lsn >
                      committed_lsn) {
                break;
              }
              request = request_queue_[worker_id].request_queue_.front();
              request_batch.push_back(request);
              request_queue_[worker_id].request_queue_.pop();
            }
            if (!request->is_write) {
              // unlock request, handle as a write request because replay is not needed
              process_single_write_request(request, buffer);
            } else {
              if (state_key_bitmap.contains(request->state_key)) {
                // new state key here means if the key has been processed in
                // this round
                process_read_request(request_batch, &gsn_collector, buffer,
                                     false, round_number, worker_id);
              } else {
                state_key_bitmap.add(request->state_key);
                process_read_request(request_batch, &gsn_collector, buffer,
                                     true, round_number, worker_id);
              }
            }
            // process_read_request(request_batch, &gsn_collector, buffer,
            // false);
            request_batch.clear();
          } else {
            // exactly once workload, use ycsb for test first
            bool is_write = false;
            {
              absl::MutexLock lock(&request_queue_[worker_id].mu_);
              if (request_queue_[worker_id].request_queue_.empty() ||
                  request_queue_[worker_id].request_queue_.front()->lsn >
                      committed_lsn) {
                break;
              } else {
                request = request_queue_[worker_id].request_queue_.front();
                is_write = request->is_write;

                if (is_write) {
                  client_id = request->client_id;
                  request_queue_[worker_id].request_queue_.pop();

                } else {
                  state_key = request->state_key;
                  request_batch.push_back(request);
                  request_queue_[worker_id].request_queue_.pop();

                }
              }
            }
            if (is_write) {
              process_single_write_request(request, buffer);
            } else {
              if (state_key_bitmap.contains(state_key)) {
                process_read_request(request_batch, &gsn_collector, buffer,
                                     false, round_number, worker_id);
              } else {
                state_key_bitmap.add(state_key);
                process_read_request(request_batch, &gsn_collector, buffer,
                                     true, round_number, worker_id);
              }
            }
            request_batch.clear();
          }
        }
      }
    };
#ifndef USE_RDMA
    gsn_collector.configuration_observer_ = [&](GSNCollector &collector, const auto &configuration) {
      if (!maintenance_readers.requested(configuration.header.epoch))
        return;
      // Reenter after the new configuration has been installed locally. This
      // collects its sealing cut, then completes every already admitted request.
      const auto committed = collector.collect();
      drain(committed ? committed : collector.local_lsn());
      maintenance_readers.park();
    };
#endif
    while (true) {
      const auto committed = gsn_collector.collect();
#ifndef USE_RDMA
      // YCSB replay can collect newer cuts inside drain(). A subsequent empty
      // poll must retain that progress so already committed requests can finish.
      drain(committed ? committed : gsn_collector.local_lsn());
#else
      drain(committed);
#endif
    }
  }

  void run() {
    // Start request I/O worker threads
    for (int i = 0; i < REQUEST_WORKER_NUM; i++) {
      request_worker_threads_.emplace_back(&Responser::RequestWorkerThread,
                                           this, i);
    }

    LOG(INFO) << "Responser starts, " << REQUEST_WORKER_NUM
              << " request processing threads";
  }

  // Stores the current processing position of each shard
  struct ShardCursor {
    std::uint32_t shard_id;
    std::uint32_t offset;
    std::uint32_t round;
    std::uint32_t lsn;

    // Comparison function for the min-heap
    bool operator>(const ShardCursor &other) const {
      if (round != other.round)
        return round > other.round;
      if (shard_id != other.shard_id)
        return shard_id > other.shard_id;

      // Fetch lsn through the offset for comparison
      // Compare lsn values in the two buffers directly without creating temporaries
      // Read lsn_payload_size from the buffer and compare the high 32 bits directly
      return lsn > other.lsn;
    }
  };

  struct ShardCursorNew {
    std::uint32_t shard_id;
    std::uint32_t round;
    std::uint32_t lsn;

    // Comparison function for the min-heap
    bool operator>(const ShardCursorNew &other) const {
      if (round != other.round)
        return round > other.round;
      if (shard_id != other.shard_id)
        return shard_id > other.shard_id;
      return lsn > other.lsn;
    }
  };

  // Check whether the next entry is a link pointer; if so, use the link
  // pointer to return the next entry address, otherwise return the next entry address directly
  char *is_link_pointer(char *entry, std::uint32_t &payload_size,
                        std::uint32_t &cur_lsn) {
    char *next_ptr = entry + sizeof(std::uint64_t) + payload_size;
    std::uint64_t *lsn_payload_size =
        reinterpret_cast<std::uint64_t *>(next_ptr);
    if (*lsn_payload_size == LINK_POINTER_MAGIC) {
      char *next_ptr_2 = next_ptr + *reinterpret_cast<std::int64_t *>(
                                        next_ptr + sizeof(std::uint64_t));
      std::uint64_t *lsn_payload_size_2 =
          reinterpret_cast<std::uint64_t *>(next_ptr_2);

      return next_ptr + *reinterpret_cast<std::int64_t *>(
                            next_ptr + sizeof(std::uint64_t));
    } else {
      return next_ptr;
    }
  }

  // TODO: Maximize CPU cache hit rate
  // Use the k-way merge algorithm directly on CXL
  void k_way_merge_scaling_no_eos(std::shared_ptr<ReqHeader> request,
                                  View::View<T> *view, char *buffer,
                                  GSNCollector *gsn_collector) {
    std::priority_queue<
        ShardCursorNew,
        absl::InlinedVector<ShardCursorNew, SHARD_SERVER_NUM + 1>,
        std::greater<ShardCursorNew>>
        min_heap;
    // Initialize each shard cursor and add it to the min-heap
    std::uint64_t *lsn_payload_ptr;
    std::uint32_t lsn;
    std::uint32_t round;
    std::uint32_t state_key = request->state_key;
    for (std::uint32_t i = 0; i < view->shard_count(); i++) {
      // Check whether any unprocessed data remains: last_replay_offset_ < last_tail_offset_
      if (view->view_of_shard_[i].last_replay_offset_ >=
          view->view_of_shard_[i].last_tail_offset_) {
        continue;
      }

      // Read lsn directly from memory to avoid unnecessary copies
      // Note: view->view_of_shard_[i].cxl_cur_ptr_ is the last consumed position; it must be an entry, not a link
      // pointer
      lsn_payload_ptr = reinterpret_cast<std::uint64_t *>(
          view->view_of_shard_[i].cxl_cur_ptr_);

      // If the current entry is a link pointer, skip it and jump directly to the next entry
      if (*lsn_payload_ptr == LINK_POINTER_MAGIC) {
        view->view_of_shard_[i].cxl_cur_ptr_ +=
            *reinterpret_cast<std::int64_t *>(
                view->view_of_shard_[i].cxl_cur_ptr_ + sizeof(std::uint64_t));
        lsn_payload_ptr = reinterpret_cast<std::uint64_t *>(
            view->view_of_shard_[i].cxl_cur_ptr_);
      }

      lsn = static_cast<std::uint32_t>(*lsn_payload_ptr >> 32);

      // The captured index is the read boundary. If its cut has not reached
      // this collector yet, catch up before replying; do not shorten the read.
      while (!gsn_collector->get_round_by_state_key(lsn, i, round, state_key))
        gsn_collector->collect<false>();

      // Add to the min-heap
      min_heap.push({i, round, lsn});
    }

    // Process all entries in global order with k-way merge until cur_tail_offset is consumed
    while (!min_heap.empty()) {
      ShardCursorNew min_cursor = min_heap.top();
      min_heap.pop();

      std::uint32_t current_shard_id = min_cursor.shard_id;

      // Process continuously within the same round: consume as many consecutive entries in the same round as possible to reduce heap operations
      std::uint32_t current_round = min_cursor.round;
      while (true) {
        char *buffer_base = view->view_of_shard_[current_shard_id].cxl_cur_ptr_;
        std::uint64_t lsn_payload_size =
            *reinterpret_cast<std::uint64_t *>(buffer_base);
        std::uint32_t payload_size =
            static_cast<std::uint32_t>(lsn_payload_size & 0xFFFFFFFF);
        std::uint32_t current_lsn =
            static_cast<std::uint32_t>(lsn_payload_size >> 32);

        // Process one entry
        process_entry(buffer_base, view);

        // Update last_replay_offset_ instead of valid_size_
        std::uint32_t entry_size = 2 * sizeof(std::uint32_t) + payload_size;
        view->view_of_shard_[current_shard_id].last_replay_offset_ +=
            entry_size;
        view->view_of_shard_[current_shard_id].cxl_cur_ptr_ =
            is_link_pointer(buffer_base, payload_size, current_lsn);

        // If cur_tail_offset has been consumed, finish this processing round
        if (view->view_of_shard_[current_shard_id].last_replay_offset_ >=
            view->view_of_shard_[current_shard_id].last_tail_offset_) {
          break;
        }

        // Read the next entry and check whether it is still in the same round
        lsn_payload_ptr = reinterpret_cast<std::uint64_t *>(
            view->view_of_shard_[current_shard_id].cxl_cur_ptr_);
        if (*lsn_payload_ptr == LINK_POINTER_MAGIC) {
          view->view_of_shard_[current_shard_id].cxl_cur_ptr_ +=
              *reinterpret_cast<std::int64_t *>(
                  view->view_of_shard_[current_shard_id].cxl_cur_ptr_ +
                  sizeof(std::uint64_t));
          lsn_payload_ptr = reinterpret_cast<std::uint64_t *>(
              view->view_of_shard_[current_shard_id].cxl_cur_ptr_);
        }
        std::uint32_t next_lsn =
            static_cast<std::uint32_t>(*lsn_payload_ptr >> 32);
        std::uint32_t next_round;
        while (!gsn_collector->get_round_by_state_key(next_lsn, current_shard_id,
                                                     next_round, state_key))
          gsn_collector->collect<false>();
        if (next_round != current_round) {
          // If round changes, insert into the heap and finish this round
          min_heap.push({current_shard_id, next_round, next_lsn});
          break;
        }

        // Otherwise continue processing the next entry in the same round
      }
    }
  }

  template <bool SendResponse = true>
  void k_way_merge_single_read_request(std::shared_ptr<ReqHeader> request,
                                       View::View<T> *view, char *buffer,
                                       GSNCollector *gsn_collector) {
    std::priority_queue<
        ShardCursorNew,
        absl::InlinedVector<ShardCursorNew, SHARD_SERVER_NUM + 1>,
        std::greater<ShardCursorNew>>
        min_heap;
    // Initialize each shard cursor and add it to the min-heap
    std::uint64_t *lsn_payload_ptr;
    std::uint32_t lsn;
    std::uint32_t round;

    for (std::uint32_t i = 0; i < view->shard_count(); i++) {
      if (view->view_of_shard_[i].last_replay_offset_ >=
          view->view_of_shard_[i].last_tail_offset_) {
        continue;
      }
      clflushopt(view->view_of_shard_[i].cxl_cur_ptr_,
                 view->view_of_shard_[i].last_tail_offset_ -
                     view->view_of_shard_[i].last_replay_offset_);
    }
    sfence();

    for (std::uint32_t i = 0; i < view->shard_count(); i++) {
      if (view->view_of_shard_[i].last_replay_offset_ >=
          view->view_of_shard_[i].last_tail_offset_) {
        continue;
      }
      prefetch(view->view_of_shard_[i].cxl_cur_ptr_, 3,
               view->view_of_shard_[i].last_tail_offset_ -
                   view->view_of_shard_[i].last_replay_offset_);
    }

    for (std::uint32_t i = 0; i < view->shard_count(); i++) {
      // Skip if there is no replayable data
      if (view->view_of_shard_[i].last_replay_offset_ >=
          view->view_of_shard_[i].last_tail_offset_) {
        continue;
      }

      // Read lsn directly from memory to avoid unnecessary copies
      // Note: view->view_of_shard_[i].cxl_cur_ptr_ is the last consumed position; it must be an entry, not a link
      // pointer
      lsn_payload_ptr = reinterpret_cast<std::uint64_t *>(
          view->view_of_shard_[i].cxl_cur_ptr_);

      // If the current entry is a link pointer, skip it and jump directly to the next entry
      if (*lsn_payload_ptr == LINK_POINTER_MAGIC) {
        view->view_of_shard_[i].cxl_cur_ptr_ +=
            *reinterpret_cast<std::int64_t *>(
                view->view_of_shard_[i].cxl_cur_ptr_ + sizeof(std::uint64_t));
        lsn_payload_ptr = reinterpret_cast<std::uint64_t *>(
            view->view_of_shard_[i].cxl_cur_ptr_);
      }

      lsn = static_cast<std::uint32_t>(*lsn_payload_ptr >> 32);

      // Look up the round; defer this shard if the entry is not committed yet.
      if (!gsn_collector->get_round(lsn, i, round)) {
        continue;
      }

      // Add to the min-heap
      min_heap.push({i, round, lsn});
    }

    // Global order of the target request
    std::uint32_t target_lsn = request->lsn;
    std::uint32_t target_round = 0;
    if (!gsn_collector->get_round(target_lsn, shard_id_, target_round)) {
      LOG(ERROR) << "get_round failed, target_lsn: " << target_lsn
                 << ", shard_id: " << shard_id_;
      exit(-1);
    }

    bool should_stop = false;

    // Process entries in global order with k-way merge
    while (!min_heap.empty()) {
      const ShardCursorNew &min_cursor = min_heap.top();

      // If the next entry global order is greater than the target request, stop
      bool is_entry_greater =
          (min_cursor.round > target_round) ||
          (min_cursor.round == target_round &&
           min_cursor.shard_id > shard_id_) ||
          (min_cursor.round == target_round &&
           min_cursor.shard_id == shard_id_ && min_cursor.lsn > target_lsn);
      if (is_entry_greater) {
        break;
      }

      // Process consecutive entries in the current shard until the round changes or a stop condition is triggered
      std::uint32_t current_round = min_cursor.round;
      std::uint32_t current_shard_id = min_cursor.shard_id;
      char *buffer_base;
      min_heap.pop();
      while (true) {
        buffer_base = view->view_of_shard_[current_shard_id].cxl_cur_ptr_;
        // Get the current entry lsn and payload_size
        std::uint64_t lsn_payload_size =
            *reinterpret_cast<std::uint64_t *>(buffer_base);
        std::uint32_t payload_size =
            static_cast<std::uint32_t>(lsn_payload_size & 0xFFFFFFFF);
        std::uint32_t current_lsn =
            static_cast<std::uint32_t>(lsn_payload_size >> 32);

        // Check whether this entry is beyond the target request's global order.
        bool greater_than_target =
            (current_round > target_round) ||
            (current_round == target_round && current_shard_id > shard_id_) ||
            (current_round == target_round && current_shard_id == shard_id_ &&
             current_lsn > target_lsn);
        if (greater_than_target) {
          should_stop = true;
          break;
        }

        // Replay this entry
        process_entry(buffer_base, view);
        // Only accumulate the logical entry size, excluding link pointers
        view->view_of_shard_[current_shard_id].last_replay_offset_ +=
            (2 * sizeof(std::uint32_t) + payload_size);
        // Advance the cursor and handle cross-segment movement
        view->view_of_shard_[current_shard_id].cxl_cur_ptr_ =
            is_link_pointer(buffer_base, payload_size, current_lsn);

        // Lock/counter reserve an adjacent logical read in the same batch.
        // Retwis reads are separate appends: a remote write can commit between
        // the previous local write and this read, so use the global bound there.
        if constexpr (std::is_same_v<T, star::lock::State> ||
                      std::is_same_v<T, star::counter::State>) {
          if (current_shard_id == shard_id_ && (current_lsn + 1 == target_lsn)) {
            should_stop = true;
            break;
          }
        }

        // Reach EOS: last_replay_offset_ ==
        // last_tail_offset_(or greater, as defensive handling)
        if (view->view_of_shard_[current_shard_id].last_replay_offset_ >=
            view->view_of_shard_[current_shard_id].last_tail_offset_) {
          break;
        }

        // Read the next entry as a candidate
        lsn_payload_size = *reinterpret_cast<std::uint64_t *>(
            view->view_of_shard_[current_shard_id].cxl_cur_ptr_);
        std::uint32_t next_lsn =
            static_cast<std::uint32_t>(lsn_payload_size >> 32);
        std::uint32_t next_round;
        if (!gsn_collector->get_round(next_lsn, current_shard_id, next_round)) {
          // Not committed; end current shard processing and wait for the next round
          break;
        }

        // If round changes, insert into the heap and switch to another shard
        if (next_round != current_round) {
          min_heap.push({current_shard_id, next_round, next_lsn});
          break;
        }

        // Otherwise continue processing the next entry for this shard in the current round
      }

      if (should_stop) {
        break;
      }
    }
    // Single request: send read response
    if constexpr (SendResponse)
      send_read_response(request, view, buffer, request->lsn, gsn_collector);
  }

#ifndef USE_RDMA
  // Called only for a retried operation restored from the read log. A fresh
  // view is necessary because ordinary reads may already have replayed beyond
  // the original read's position. No response or shared write occurs here.
  void recover_read_value(const std::shared_ptr<ReqHeader> &request,
                          View::View<T> *current, Reader<T> &reader,
                          GSNCollector *collector) {
    if constexpr (std::is_same_v<T, star::retwis::State>) {
      CHECK(SharedData::recovered_shard.has_value());
      CHECK_LT(reader.min_lsn_, request->lsn);
      View::View<T> replay(request->state_key, shard_id_);
      replay.value_ = std::make_shared<T>();
      star::retwis::Post post{};
      star::retwis::Timeline timeline;
      char initial_post[CONTENT_SIZE];
      std::size_t bytes;
      if (request->state_key < USER_CNT) {
        replay.value_->user = current->value_->user;
        bytes = reader.field_id_ == 0 ? sizeof(std::uint32_t) : sizeof(std::uint32_t) * FOLLOWERS_CNT;
      } else if (request->state_key < 2 * USER_CNT) {
        // Initial followers/timestamps retain the benchmark's existing setup.
        // Timeline writes append; the initial entries stay at the front.
        timeline.username = request->state_key - USER_CNT;
        timeline.posts.assign(current->value_->timeline->posts.begin(),
                              current->value_->timeline->posts.begin() + MAX_RETURN_POST_CNT);
        replay.value_->timeline = &timeline;
        bytes = sizeof(std::uint64_t) * MAX_RETURN_POST_CNT;
      } else {
        std::memset(initial_post, '1', sizeof(initial_post));
        post.post_id = request->state_key - 2 * USER_CNT;
        post.content = initial_post;
        replay.value_->post = &post;
        bytes = CONTENT_SIZE;
      }
      const auto worker = collector->thread_id_ - REPLICATOR_NUM;
      const auto version = SharedData::reader_lock_switch_flag_with_version_number(
          sequencer_switch_flag_, switch_flag_[worker]);
      replay.read_index(collector->index_for_switch(version), collector->configuration_for_switch(version));
      SharedData::reader_unlock_switch_flag(switch_flag_[worker]);
      auto first = std::make_shared<ReqHeader>(*request);
      first->lsn = reader.min_lsn_;
      k_way_merge_single_read_request<false>(first, &replay, nullptr, collector);
      reader.recovered_value_ = std::make_unique<char[]>(bytes);
      std::memcpy(reader.recovered_value_.get(), star::retwis::get_value(&replay, reader.field_id_), bytes);
      reader.value_ = reader.recovered_value_.get();
      reader.ready_to_read_ = true;
    }
  }
#endif

  //         // Check whether the current entry global order is greater than the target lsn
  //         bool is_entry_greater = (min_cursor.round > target_round) ||
  //                                 (min_cursor.round == target_round &&
  //                                 min_cursor.shard_id > shard_id_) ||
  //                                 (min_cursor.round == target_round &&
  //                                 min_cursor.shard_id == shard_id_ &&
  //                                 min_cursor.lsn > target_lsn);

  //         // Check whether the current entry global order is greater than the target lsn
  //         bool is_entry_greater = (min_cursor.round > target_round) ||
  //                                 (min_cursor.round == target_round &&
  //                                 min_cursor.shard_id > shard_id_) ||
  //                                 (min_cursor.round == target_round &&
  //                                 min_cursor.shard_id == shard_id_ &&
  //                                 min_cursor.lsn > target_lsn);

};
