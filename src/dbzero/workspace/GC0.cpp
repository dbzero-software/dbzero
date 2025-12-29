// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "GC0.hpp"

namespace db0

{
    std::shared_ptr<GC0_SharedState>& GC0::getGlobalSharedState()
    {
        // Enforce singleton-like behavior. Single initialization is guaranteed by C++.
        static std::shared_ptr<GC0_SharedState> global_shared_state = std::make_shared<GC0_SharedState>();
        return global_shared_state;
    }

    template <typename T> void dropByAddr(Memspace &memspace, Address addr, const std::vector<GC_Ops> &ops)
    {
        assert(ops.size() > T::m_gc_ops_id);
        ops[T::m_gc_ops_id].dropByAddr(memspace, addr);
    }

    GC0::GC0(db0::swine_ptr<Fixture> &fixture)
        : super_t(fixture)
        , m_shared_state(getGlobalSharedState())    
        , m_read_only(false)
    {
    }
    
    GC0::GC0(db0::swine_ptr<Fixture> &fixture, Address address, bool read_only)
        : super_t(tag_from_address(), fixture, address)
        , m_shared_state(getGlobalSharedState())    
        , m_read_only(read_only)
    {
    }
    
    GC0::~GC0()
    {
    }

    GC0_SharedState& GC0::getSharedState()
    {
        return *m_shared_state;
    }
    
    bool GC0::tryRemove(void *vptr, bool is_volatile)
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        auto it = m_vptr_map.find(vptr);
        if (it == m_vptr_map.end()) {
            return false;
        }
        
        NoArgsFunction drop_op = nullptr;
        auto &ops = getSharedState().m_ops[it->second];
        // if type implements flush then remove it from flush map as well
        if (ops.flush) {
            m_flush_map.erase(vptr);
        }

        // do not drop when in read-only mode (e.g. snapshot owned)
        // NOTE: drop not allowed when commit pending
        // do not drop volatile instances
        if (!m_read_only && ops.hasRefs && ops.drop && !is_volatile
            && !ops.hasRefs(it->first))
        {
            if (m_commit_pending) {
                // must schedule for deletion since unable to drop while save is pending
                auto addr_pair = ops.address(it->first);
                m_scheduled_for_deletion[addr_pair.first] = addr_pair.second;
            } else {
                // at this stage just collect the ops and remove the entry
                drop_op = ops.drop;
            }
        }
        // NOTE: we erase by vptr because hasRefs may have side effects and invalidate the iterator
        m_vptr_map.erase(vptr);
        lock.unlock();
        
        // drop object after erasing from map due to possible recursion
        if (drop_op) {
            auto fixture = this->getFixture();
            fixture->onUpdated();
            // lock to synchronize with the auto-commit thread            
            FixtureLock lock(fixture);
            drop_op(vptr);
            return true;
        }

        return false;
    }
    
    void GC0::detachAll()
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        auto &ops_list = getSharedState().m_ops;
        for (auto &vptr_item : m_vptr_map) {
            ops_list[vptr_item.second].detach(vptr_item.first);
        }
    }
    
    void GC0::commitAllOf(const std::vector<vtypeless*> &vptrs, ProcessTimer *timer_ptr)
    {
        std::unique_ptr<ProcessTimer> timer;
        if (timer_ptr) {
            timer = std::make_unique<ProcessTimer>("GC0::commitAllOf", timer_ptr);
        }
        
        // Commit & collect unreferenced instances
        // Important ! Collect instance addresses first because push_back can trigger "remove" calls        
        std::unique_lock<std::mutex> lock(m_mutex);
        std::unordered_set<TypedAddress> addresses;
        std::size_t count = 0;
        auto &ops_list = getSharedState().m_ops;
        for (auto vptr : vptrs) {
            auto it = m_vptr_map.find(vptr);
            if (it != m_vptr_map.end()) {
                auto &ops = ops_list[it->second];
                ops.commit(vptr);
                if (ops.hasRefs && !ops.hasRefs(vptr)) {
                    addresses.insert(toTypedAddress(ops.address(vptr)));
                }
                ++count;
            }
        }

        lock.unlock();
                
        super_t::clear();
        for (auto addr: addresses) {
            super_t::push_back(addr);
        }
        // also registered instances scheduled for deletion
        for (auto &addr_pair: m_scheduled_for_deletion) {
            super_t::push_back(toTypedAddress(addr_pair));
        }
        m_scheduled_for_deletion.clear();
        super_t::commit();
    }
    
    void GC0::commitAll()
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        auto &ops_list = getSharedState().m_ops;
        for (auto &vptr_item : m_vptr_map) {
            ops_list[vptr_item.second].commit(vptr_item.first);
        }
    }

    std::size_t GC0::size() const 
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        return m_vptr_map.size();
    }
    
    void GC0::flushAllOf(const std::vector<vtypeless*> &vptrs, ProcessTimer *timer_ptr)
    {
        std::unique_ptr<ProcessTimer> timer;
        if (timer_ptr) {
            timer = std::make_unique<ProcessTimer>("GC0::flushAllOf", timer_ptr);
        }

        std::unique_lock<std::mutex> lock(m_mutex);
        // collect ops first (this is necessary because flush can trigger "remove" calls)
        std::vector<std::pair<void*, unsigned int>> flush_ops;
        for (auto vptr : vptrs) {
            auto it = m_flush_map.find(vptr);
            if (it != m_flush_map.end()) {
                flush_ops.push_back(*it);
            }
        }
        lock.unlock();
        
        // call flush where it's provided
        auto &ops_list = getSharedState().m_ops;
        for (auto &item : flush_ops) {
            ops_list[item.second].flush(item.first, false);
        }
    }
    
    void GC0::collect()
    {        
        assert(!m_read_only);
        if (!m_vptr_map.empty()) {
            THROWF(db0::InternalException) << "GC0::collect: cannot collect while there are still live instances";
        }
        auto fixture = this->tryGetFixture();
        if (!fixture) {
            THROWF(db0::InternalException) << "GC0::collect: cannot collect without a valid fixture";
        }

        auto &ops_list = getSharedState().m_ops;
        auto &ops_map = getSharedState().m_ops_map;
        
        // drop scheduled for deletion
        for (auto &addr_pair: m_scheduled_for_deletion) {
            auto ops_id = ops_map[addr_pair.second];
            assert(ops_id < ops_list.size());
            ops_list[ops_id].dropByAddr(fixture, addr_pair.first.getAddress());
        }
        m_scheduled_for_deletion.clear();
        
        for (auto addr: *this) {
            auto ops_id = ops_map[addr.getType()];
            assert(ops_id < ops_list.size());
            // object will be dropped only if it has no references
            ops_list[ops_id].dropByAddr(fixture, addr.getAddress());
        }
        super_t::clear();
    }
    
    void GC0::beginAtomic()
    {
        assert(!m_atomic);
        // commmit all active v_object instances so that the underlying locks can be re-created (CoW)        
        commitAll();
        m_atomic = true;
    }

    void GC0::endAtomic()
    {
        assert(m_atomic);
        m_volatile.clear();
        m_atomic = false;
    }
    
    void GC0::cancelAtomic()
    {
        assert(m_atomic);
        for (auto vptr : m_volatile) {
            if (vptr) {
                tryRemove(vptr, true);
            }
        }
        // call reverse flush where it's provided (use revert=true)
        auto &ops_list = getSharedState().m_ops;
        for (auto &item : m_flush_map) {
            ops_list[item.second].flush(item.first, true);
        }
        m_volatile.clear();
        m_atomic = false;
    }
    
    std::optional<unsigned int> GC0::erase(void *vptr)
    {
        std::optional<unsigned int> flush_op;
        std::unique_lock<std::mutex> lock(m_mutex);
        assert(m_vptr_map.find(vptr) != m_vptr_map.end());
        m_vptr_map.erase(vptr);
        auto it = m_flush_map.find(vptr);
        if (it != m_flush_map.end()) {
            flush_op = it->second;
            m_flush_map.erase(it);                    
        }

        if (m_atomic) {
            for (auto &volatile_ptr: m_volatile) {
                if (volatile_ptr == vptr) {
                    volatile_ptr = nullptr;
                }
            }
        }
        return flush_op;
    }

    GC0::CommitContext::CommitContext(GC0 &gc0)
        : m_gc0(gc0)
    {        
        assert(!m_gc0.m_commit_pending);
        m_gc0.m_commit_pending = true;
    }

    GC0::CommitContext::~CommitContext()
    {
        assert(m_gc0.m_commit_pending);
        m_gc0.m_commit_pending = false;
    }
    
    void GC0::CommitContext::commitAllOf(const std::vector<vtypeless*> &vec, ProcessTimer *timer)
    {
        assert(m_gc0.m_commit_pending);
        m_gc0.commitAllOf(vec, timer);
    } 
    
    std::unique_ptr<GC0::CommitContext> GC0::beginCommit() {
        return std::make_unique<CommitContext>(*this);
    }

}