#include "CacheRecycler.hpp"
#include "config.hpp"
#include <cassert>
#include <iostream>

namespace db0

{
    
    CacheRecycler::CacheRecycler(std::size_t size, std::optional<std::size_t> flush_size,
        std::function<void(bool threshold_reached)> flush_callback)
        : m_res_buf((size > 0)?((size - 1) / MIN_PAGE_SIZE + 1):0)
        , m_capacity(size)
        // assign default flush size
        , m_flush_size(flush_size.value_or(DEFAULT_FLUSH_SIZE))
        , m_flush_callback(flush_callback)
    {
    }
    
    void CacheRecycler::adjustSize(std::unique_lock<std::mutex> &, 
        std::vector<std::shared_ptr<ResourceLock> > &released_locks, std::size_t requested_release_size)
    {
        std::size_t released_size = 0;
        // try flushing 'requested_release_size' number of excess elements
        auto it = m_res_buf.begin(), end = m_res_buf.end();
        while (it != end && released_size < requested_release_size) {
            // only release locks with no active external references (other than the CacheRecycler itself)
            if ((*it).use_count() == 1) {
                std::shared_ptr<ResourceLock> temp_lock = *it;
                auto lock_size = temp_lock->size();
                released_size += lock_size;
                temp_lock->setRecycled(false);
                released_locks.push_back(std::move(temp_lock));
                it = m_res_buf.erase(it);
            } else {
                ++it;
            }       
        }
        
        // update current size
        m_current_size -= released_size;
    }
    
    void CacheRecycler::updateSize(std::unique_lock<std::mutex> &lock, std::size_t expected_size)
    {
        std::vector<std::shared_ptr<ResourceLock> > released_locks;
        // we make 2 iterations because dependent locks (i.e. owned by the boundary lock)
        // will be released only during the second pass
        for (int i = 0; i < 2; ++i) {
            if (m_current_size <= expected_size) {
                break;
            }

            // release excess locks plus flush size
            adjustSize(lock, released_locks, m_current_size - expected_size);
            lock.unlock();
            // FIXME: we can do this in a dedicated thread not to block this one
            // expired locks need to be flushed
            for (auto &lock: released_locks) {
                lock->flush();
            }
            released_locks.clear();
            lock.lock();            
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
        bool flushed, flush_result = false;
		if (res_lock) {
			// access existing resource
			std::unique_lock<std::mutex> lock(m_mutex);
			if (res_lock->isRecycled()) {
				// resource already in cache, just bring to back (lowest priority for removal)
                m_res_buf.splice(m_res_buf.end(), res_lock->m_recycle_it);
			} else if (res_lock->isCached()) {
                // add new resource (if to be cached)
                auto lock_size = res_lock->size();
                if (lock_size > m_capacity) {
                    // Cache size is too small to keep this resource
                    // (or is uninitialized)
                    return;
                }
                m_current_size += lock_size;
                if (m_current_size > m_capacity) {
                    // try reducing cache utilization to capacity minus flush size
                    updateSize(lock, m_capacity - m_flush_size);
                    flushed = true;
                    flush_result = m_current_size <= (m_capacity - m_flush_size);
                }
                // resize is a costly operation but cannot be avoided if the number of locked
                // resources exceeds the assumed limit
                // note that this operation does not change the configured cache capacity
                if (m_res_buf.size() == m_res_buf.max_size()) {
                    // After resize, all iterators to cached elements will be invalidated!!
                    m_res_buf.resize(m_res_buf.size() * 2);
                    // Update self-iterators in all cached locks
                    for (auto it = m_res_buf.begin(), end = m_res_buf.end(); it != end; ++it) {
                        (*it)->m_recycle_it = it;
                    }                    
                }
                m_res_buf.push_back(res_lock);
                res_lock->m_recycle_it = std::prev(m_res_buf.end());
                res_lock->setRecycled(true);
			}
		}
        if (flushed && m_flush_callback) {
            m_flush_callback(flush_result);
        }
	}
    
	void CacheRecycler::clear()
    {
        std::unique_lock<std::mutex> lock(m_mutex);            
        // try releasing all locks
        updateSize(lock, 0);
	}

    void CacheRecycler::resize(std::size_t new_size)
    {        
        std::unique_lock<std::mutex> lock(m_mutex);
        if (new_size == m_capacity) {
            return;
        }
        m_capacity = new_size;
        // try releasing excess locks
        updateSize(lock, m_capacity);
        // new capacity of the fixed list should allow storing existing locks
        auto new_max_size = std::max((m_capacity - 1) / MIN_PAGE_SIZE + 1, m_res_buf.size());                
        if (new_max_size != m_res_buf.max_size()) {
            // After resize, all iterators to cached elements will be invalidated!!
            m_res_buf.resize(new_max_size);

            // Update self-iterators in all cached locks
            for (auto it = m_res_buf.begin(), end = m_res_buf.end(); it != end; ++it) {
                (*it)->m_recycle_it = it;
            }
        }
    }
    
    void CacheRecycler::release(ResourceLock &res, std::unique_lock<std::mutex> &)
    {
        if (res.isRecycled()) {
            res.setRecycled(false);
            m_current_size -= res.size();
            m_res_buf.erase(res.m_recycle_it);
        }
    }
    
	std::size_t CacheRecycler::size() const {
        return m_current_size;
    }

	std::size_t CacheRecycler::getCapacity() const {
        return m_capacity;
    }
    
    void CacheRecycler::forEach(std::function<void(std::shared_ptr<ResourceLock>)> f) const
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        for (const auto &p: m_res_buf) {
            f(p);
        }
    }

}