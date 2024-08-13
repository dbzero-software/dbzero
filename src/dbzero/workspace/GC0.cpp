#include "GC0.hpp"

namespace db0

{
    
    std::vector<GC_Ops> GC0::m_ops;
    std::unordered_map<StorageClass, GCOps_ID> GC0::m_ops_map;
    bool GC0::m_initialized = false;

    template <typename T> void dropByAddr(Memspace &memspace, uint64_t addr, const std::vector<GC_Ops> &ops)
    {
        assert(ops.size() > T::m_gc_ops_id);
        ops[T::m_gc_ops_id].dropByAddr(memspace, addr);
    }

    GC0::GC0(db0::swine_ptr<Fixture> &fixture)
        : super_t(fixture)        
    {
    }
    
    GC0::GC0(db0::swine_ptr<Fixture> &fixture, std::uint64_t address)
        : super_t(tag_from_address(), fixture, address)        
    {
    }
    
    bool GC0::remove(void *vptr)
    {
        auto it = m_vptr_map.find(vptr);
        NoArgsFunction drop_op = nullptr;
        if (it != m_vptr_map.end()) {
            auto ops = m_ops[it->second];
            // if type implements preCommit then remove it from pre-commit map as well
            if (ops.preCommit) {
                m_pre_commit_map.erase(vptr);                
            }            
            if (ops.hasRefs && ops.drop && !ops.hasRefs(it->first)) {
                // at this stage just collect the ops and remove the entry
                drop_op = ops.drop;
            }
            m_vptr_map.erase(it);
        }

        // drop object after erasing from map due to possible recursion
        if (drop_op) {
            // lock to synchronize with the auto-commit thread
            auto fixture = this->getFixture();
            fixture->onUpdated();
            FixtureLock lock(fixture);
            drop_op(vptr);
            return true;
        }

        return false;
    }
    
    void GC0::unregister(void *vptr)
    {
        auto it = m_vptr_map.find(vptr);
        if (it != m_vptr_map.end()) {
            auto ops = m_ops[it->second];
            m_vptr_map.erase(it);
        }
    }

    void GC0::detachAll()
    {
        for (auto &vptr_item : m_vptr_map) {
            m_ops[vptr_item.second].detach(vptr_item.first);
        }
    }

    void GC0::commitAll()
    {
        for (auto &vptr_item : m_vptr_map) {
            m_ops[vptr_item.second].commit(vptr_item.first);
        }
    }

    std::size_t GC0::size() const {
        return m_vptr_map.size();
    }
    
    void GC0::commit()
    {
        // call pre-commit where it's provided
        for (auto &item : m_pre_commit_map) {
            m_ops[item.second].preCommit(item.first, false);
        }

        super_t::clear();
        for (auto &vptr_item : m_vptr_map) {
            auto ops = m_ops[vptr_item.second];
            if (ops.hasRefs && !ops.hasRefs(vptr_item.first)) {
                super_t::push_back(ops.address(vptr_item.first));
            }
        }
    }
    
    void GC0::collect()
    {
        if (!m_vptr_map.empty()) {
            THROWF(db0::InternalException) << "GC0::collect: cannot collect while there are still live instances";
        }
        auto fixture = this->tryGetFixture();
        if (!fixture) {
            THROWF(db0::InternalException) << "GC0::collect: cannot collect without a valid fixture";
        }

        for (auto addr: *this) {
            auto ops_id = m_ops_map[addr.getType()];
            assert(ops_id < m_ops.size());
            // object will be dropped only if it has no references
            m_ops[ops_id].dropByAddr(fixture, addr.getAddress());
        }
        super_t::clear();
    }
    
    void GC0::beginAtomic()
    {
        assert(!m_atomic);
        // commmit all active v_object instances so that the underlying locks can be re-created (CoW)        
        commitAll();
        // call pre-commit where it's provided (i.e. to flush internal buffers)
        for (auto &item : m_pre_commit_map) {
            m_ops[item.second].preCommit(item.first, false);
        }
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
            remove(vptr);
        }
        // call reverse pre-commit where it's provided (use revert=true)
        for (auto &item : m_pre_commit_map) {
            m_ops[item.second].preCommit(item.first, true);
        }
        m_volatile.clear();
        m_atomic = false;
    }

}