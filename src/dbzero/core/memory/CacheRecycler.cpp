// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "CacheRecycler.hpp"
#include "config.hpp"
#include <cassert>
#include <iostream>

namespace db0

{
    
    // Calculate target capacity for specific priority
    std::size_t getCapacity(std::size_t total_capacity, int priority)
    {
        auto result = total_capacity;
        auto low_result = total_capacity >> 3; // 12.5% for low priority
        if (priority == 0) {
            result -= low_result;
        } else {
            result = low_result;
        }
        return result;
    }

    std::size_t getMaxSize(std::size_t capacity) {
        return (capacity > 0) ? ((capacity - 1) / MIN_PAGE_SIZE + 1) : 0;
    }
    
    CacheRecycler::CacheRecycler(std::size_t capacity, const std::atomic<std::size_t> &dirty_meter,
        std::optional<std::size_t> flush_size,
        std::function<void(std::size_t limit)> flush_dirty,
        std::function<bool(bool threshold_reached)> flush_callback)
        : m_capacity(capacity)
        // NOTE: buffers are overprovisioned
        , m_res_bufs { getMaxSize(m_capacity), getMaxSize(m_capacity) }
        , m_dirty_meter(dirty_meter)
        // assign default flush size
        , m_flush_size(flush_size.value_or(DEFAULT_FLUSH_SIZE))
        , m_flush_dirty(flush_dirty)
        , m_flush_callback(flush_callback)
    {
    }

    std::size_t CacheRecycler::adjustSize(std::unique_lock<std::mutex> &, list_t &res_buf,
        std::size_t requested_release_size)
    {                  
        // calculate size to be released from the dirty locks
        // so that they occupy <50% of the cache
        // NOTE: this has to be done before actual size adjustment
        if (m_flush_dirty && m_dirty_meter > ((getCurrentSize() - requested_release_size) >> 1)) {
            std::int64_t limit = m_dirty_meter - ((getCurrentSize() - requested_release_size) >> 1);
            // request flushing (and releasing) specific volume of dirty locks
            m_flush_dirty(limit);
        }
        
        std::size_t released_size = 0;
        // try flushing 'requested_release_size' number of excess elements
        auto it = res_buf.begin(), end = res_buf.end();
        while (it != end && released_size < requested_release_size) {
            // only release locks with no active external references (other than the CacheRecycler itself)
            // NOTE: dirty locks are relased by m_flush_dirty callback
            if ((*it).use_count() == 1 && !(*it)->isDirty()) {
                released_size += (*it)->usedMem();
                it = res_buf.erase(it);
            } else {
                ++it;
            }
        }
        
        return released_size;
    }
    
    void CacheRecycler::adjustSize(std::unique_lock<std::mutex> &lock, std::size_t release_size)
    {
        // release from low-priority cache first
        auto released_size = adjustSize(lock, m_res_bufs[1], release_size);
        // update current size
        m_current_size[1] -= released_size;
        release_size -= released_size;
        if (release_size > 0) {
            released_size = adjustSize(lock, m_res_bufs[0], release_size);
            m_current_size[0] -= released_size;
        }
    }

    void CacheRecycler::updateSize(std::unique_lock<std::mutex> &lock, int priority, std::size_t expected_size)
    {
        assert(priority == 0 || priority == 1); 
        // we make 2 iterations because dependent locks (i.e. owned by the boundary lock)
        // will be released only during the second pass
        for (int i = 0; i < 2; ++i) {
            if (m_current_size[priority] <= expected_size) {
                break;
            }

            // release excess locks plus flush size
            auto released_size = adjustSize(lock, m_res_bufs[priority], m_current_size[priority] - expected_size);
            m_current_size[priority] -= released_size;
        }
    }
    
    void CacheRecycler::setFlushSize(unsigned int flush_size)
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        if (flush_size > 0) {
            m_flush_size = flush_size;
        }
    }
    
    void CacheRecycler::update(std::shared_ptr<ResourceLock> res_lock)
    {        
        bool flushed = false, flush_result = false;
		if (res_lock) {
			// access existing resource
			std::unique_lock<std::mutex> lock(m_mutex);
            int priority = res_lock->isCached() ? 0 : 1;
			if (res_lock->isRecycled()) {
				// resource already in cache, just bring to back (lowest priority for removal)
                m_res_bufs[priority].splice(m_res_bufs[priority].end(), res_lock->m_recycle_it);
			} else {
                // add new resource (if to be cached)
                auto lock_size = res_lock->usedMem();
                auto &res_buf = m_res_bufs[priority];                
                if (lock_size > m_capacity) {
                    // Cache size is too small to keep this resource
                    // (or is uninitialized)
                    return;
                }
                m_current_size[priority] += lock_size;
                if (getCurrentSize() > m_capacity) {
                    auto flush_returned_values = _flush(lock, priority);
                    flushed = flush_returned_values.first;
                    flush_result = flush_returned_values.second;
                }
                // resize is a costly operation but cannot be avoided if the number of locked
                // resources exceeds the assumed limit
                // note that this operation does not change the configured cache capacity
                if (res_buf.size() == res_buf.max_size()) {
                    // After resize, all iterators to cached elements will be invalidated!!
                    res_buf.resize(res_buf.size() * 2);
                    // Update self-iterators in all cached locks
                    for (auto it = res_buf.begin(), end = res_buf.end(); it != end; ++it) {
                        (*it)->m_recycle_it = it;
                    }
                }
                res_buf.push_back(res_lock);
                res_lock->m_recycle_it = std::prev(res_buf.end());
                res_lock->setRecycled(true);
			}
		}
        // NOTE: flush-callback will be repeated if unable to handle the previous time
        if (m_flush_callback && (flushed || !m_last_flush_callback_result.first)) {
            m_last_flush_callback_result.second = flush_result;
            m_last_flush_callback_result.first = m_flush_callback(flush_result);
        }
	}
    
	void CacheRecycler::clear()
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        // try releasing all locks without changing capacity
        updateSize(lock, 0, 0);
        updateSize(lock, 1, 0);
	}
    
    void CacheRecycler::resize(std::size_t new_capacity)
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        bool resize = (new_capacity < m_capacity);
        m_capacity = new_capacity;
        if (resize) {
            // try reducing cache utilization to new capacity
            updateSize(lock, new_capacity);
        }
    }
    
    void CacheRecycler::updateSize(std::unique_lock<std::mutex> &_lock, std::size_t expected_size)
    {
        // try keeping priority = 1 below its target capacity
        auto new_size_1 = std::min(db0::getCapacity(expected_size, 1), m_current_size[1]);
        resize(_lock, new_size_1, 1);
        // priority = 0 may excteed its target capacity when there's sufficient free space
        resize(_lock, std::min(expected_size - new_size_1, m_current_size[0]), 0);
    }

    void CacheRecycler::resize(std::unique_lock<std::mutex> &_lock, std::size_t new_size, int priority)
    {        
        if (m_current_size[priority] <= new_size) {
            // target size already satisfied
            return;
        }
        
        // try releasing excess locks
        updateSize(_lock, priority, new_size);
        auto &res_buf = m_res_bufs[priority];
        // new capacity of the fixed list should allow storing existing locks
        auto new_max_size = std::max((m_capacity - 1) / MIN_PAGE_SIZE + 1, res_buf.size());
        if (new_max_size > res_buf.max_size()) {
            // After resize, all iterators to cached elements will be invalidated!!
            res_buf.resize(new_max_size);
            
            // Update self-iterators in all cached locks
            for (auto it = res_buf.begin(), end = res_buf.end(); it != end; ++it) {
                (*it)->m_recycle_it = it;
            }
        }
    }
    
    void CacheRecycler::release(ResourceLock &res, std::unique_lock<std::mutex> &)
    {
        if (res.isRecycled()) {
            res.setRecycled(false);
            int priority = res.isCached() ? 0 : 1;
            m_current_size[priority] -= res.size();
            m_res_bufs[priority].erase(res.m_recycle_it);
        }
    }
    
	std::size_t CacheRecycler::size() const
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        return getCurrentSize();
    }

    void CacheRecycler::forEach(std::function<void(std::shared_ptr<ResourceLock>)> f) const
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        for (const auto &p: m_res_bufs[0]) {
            f(p);
        }
        for (const auto &p: m_res_bufs[1]) {
            f(p);
        }
    }
    
    std::size_t CacheRecycler::getCapacity() const
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        return m_capacity;
    }

    std::vector<std::size_t> CacheRecycler::getDetailedSize() const
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        return { m_current_size[0], m_current_size[1] };
    }

    std::pair<bool, bool> CacheRecycler::_flush(std::unique_lock<std::mutex> &lock, int priority)
    {
        auto now = std::chrono::high_resolution_clock::now();
        if (now >= m_next_flush_time) {
            // try reducing cache utilization to capacity minus flush size
            auto flush_size = std::min(m_capacity >> 1, m_flush_size);
            auto size_before_flush = getCurrentSize();
            updateSize(lock, m_capacity - flush_size);
            // Update backoff state based on flush result(need to flush more than 10 % of flush size)
            if ((size_before_flush - getCurrentSize()) > (flush_size/10)) {
                // Success: reset delay
                m_current_flush_delay = std::chrono::nanoseconds{0};
                m_next_flush_time = std::chrono::high_resolution_clock::time_point{};
            } else {
                // Failure: apply exponential backoff
                // adding +1 to avoid condition for zero delay
                auto new_delay = std::min(m_current_flush_delay.count() * 2 + 1 , MAX_FLUSH_DELAY_NS);
                m_current_flush_delay = std::chrono::nanoseconds{new_delay};
                now = std::chrono::high_resolution_clock::now();
                m_next_flush_time = now + m_current_flush_delay;
            }
            return { true, m_current_size[priority] <= (m_capacity - flush_size) };
        }
        return { false, false };
    }
}