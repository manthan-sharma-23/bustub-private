//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <iterator>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <utility>
#include "common/config.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  for (size_t i = 0; i < replacer_size_; i++) {
    node_store_[i].fid_ = i;
    node_store_[i].is_evictable_ = false;
  }
}

auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  std::lock_guard<std::mutex> lock(latch_);

  current_timestamp_++;

  if (node_store_.empty()) {
    return std::nullopt;
  }

  std::unordered_map<frame_id_t, LRUKNode> infinitely_distanced_frames;
  std::unordered_map<frame_id_t, LRUKNode> evictable_frames;

  for (const auto &[_fid, node] : node_store_) {
    if (node.is_evictable_) {
      const bool is_infinite_k = node.history_.size() < k_;

      if (is_infinite_k) {
        infinitely_distanced_frames[node.fid_] = node;
        evictable_frames.erase(node.fid_);
      } else {
        evictable_frames[node.fid_] = node;
        infinitely_distanced_frames.erase(node.fid_);
      }
    }
  }

  if (!infinitely_distanced_frames.empty()) {
    auto evicted_frame_id = infinitely_distanced_frames.begin()->first;

    for (const auto &[frame_id, node] : infinitely_distanced_frames) {
      auto last_access_timestamp = node.history_.back();
      if (last_access_timestamp < node_store_[evicted_frame_id].history_.back()) {
        evicted_frame_id = frame_id;
      }
    }

    node_store_[evicted_frame_id].history_.clear();
    node_store_[evicted_frame_id].is_evictable_ = false;
    curr_size_--;
    return evicted_frame_id;
  }

  if (!evictable_frames.empty()) {
    size_t max_kth_distance = 0;
    frame_id_t frame_id{-1};

    for (const auto &[fid, node] : evictable_frames) {
      if (node.history_.size() >= k_) {
        auto history_itr = node.history_.begin();
        std::advance(history_itr, node.history_.size() - k_);
        auto stamp = *history_itr;
        size_t kth_distance = current_timestamp_ - stamp;

        if (kth_distance > max_kth_distance || frame_id == -1) {
          max_kth_distance = kth_distance;
          frame_id = fid;
        }
      }
    }

    if (frame_id != -1) {
      node_store_[frame_id].history_.clear();
      node_store_[frame_id].is_evictable_ = false;
      curr_size_--;
      return frame_id;
    }
  }

  return std::nullopt;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);

  current_timestamp_++;

  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw Exception("Frame id is greater than the replacer size");
  }

  if (node_store_.find(frame_id) == node_store_.end()) {
    node_store_[frame_id] = LRUKNode(k_, frame_id);
  }

  node_store_[frame_id].history_.push_back(current_timestamp_);
  return;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  if (node_store_.find(frame_id) == node_store_.end()) {
    throw Exception("Frame id not found in the replacer");
  }

  if (node_store_[frame_id].is_evictable_ && !set_evictable) {
    curr_size_--;
  }
  if (!node_store_[frame_id].is_evictable_ && set_evictable) {
    curr_size_++;
  }

  node_store_[frame_id].is_evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  if (node_store_.find(frame_id) == node_store_.end()) {
    return;
  }

  if (node_store_[frame_id].is_evictable_) {
    curr_size_--;
  }

  node_store_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
