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
        , m_read_only(false)
    {
    }
    
    GC0::GC0(db0::swine_ptr<Fixture> &fixture, std::uint64_t address, bool read_only)
        : super_t(tag_from_address(), fixture, address)
        , m_read_only(read_only)
    {
    }
    
    GC0::~GC0()
    {
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
    
    void GC0::CommitContext::commit()
    {
        assert(m_gc0.m_commit_pending);
        m_gc0.commit();
    }
    
    bool GC0::tryRemove(void *vptr, bool is_volatile)
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        auto it = m_vptr_map.find(vptr);
        if (it == m_vptr_map.end()) {
            return false;
        }

        NoArgsFunction drop_op = nullptr;
        auto &ops = m_ops[it->second];
        // if type implements preCommit then remove it from pre-commit map as well
        if (ops.preCommit) {
            m_pre_commit_map.erase(vptr);
        }
        // do not drop when in read-only mode (e.g. snapshot owned)
        // NOTE: drop not allowed when commit pending
        // do not drop volatile instances
        if (!m_read_only && ops.hasRefs && ops.drop && !m_commit_pending && !is_volatile
            && !ops.hasRefs(it->first))
        {
            // at this stage just collect the ops and remove the entry
            drop_op = ops.drop;
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
        for (auto &vptr_item : m_vptr_map) {
            m_ops[vptr_item.second].detach(vptr_item.first);
        }
    }
    
    void GC0::commitAll()
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        for (auto &vptr_item : m_vptr_map) {
            m_ops[vptr_item.second].commit(vptr_item.first);
        }
    }

    std::size_t GC0::size() const 
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        return m_vptr_map.size();
    }
    
    void GC0::preCommit()
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        // collect ops first (this is necessary because preCommit can trigger "remove" calls)
        std::vector<std::pair<void*, unsigned int>> pre_commit_ops;
        std::copy(m_pre_commit_map.begin(), m_pre_commit_map.end(), std::back_inserter(pre_commit_ops));
        lock.unlock();

        // call pre-commit where it's provided
        for (auto &item : pre_commit_ops) {
            m_ops[item.second].preCommit(item.first, false);
        }
    }
    
    void GC0::commit()
    {
        // Important ! Collect instance addresses first because push_back can trigger "remove" calls
        std::vector<TypedAddress> addresses;
        std::unique_lock<std::mutex> lock(m_mutex);
        for (auto &vptr_item : m_vptr_map) {
            auto &ops = m_ops[vptr_item.second];
            if (ops.hasRefs && !ops.hasRefs(vptr_item.first)) {
                addresses.push_back(ops.address(vptr_item.first));
            }
        }
        lock.unlock();
        
        super_t::clear();
        for (auto addr: addresses) {
            super_t::push_back(addr);
        }
        super_t::commit();
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
        // call reverse pre-commit where it's provided (use revert=true)
        for (auto &item : m_pre_commit_map) {
            m_ops[item.second].preCommit(item.first, true);
        }
        m_volatile.clear();
        m_atomic = false;
    }

    std::unique_ptr<GC0::CommitContext> GC0::beginCommit() {
        return std::make_unique<CommitContext>(*this);
    }

    std::optional<unsigned int> GC0::erase(void *vptr)
    {
        std::optional<unsigned int> pre_commit_op;
        std::unique_lock<std::mutex> lock(m_mutex);
        assert(m_vptr_map.find(vptr) != m_vptr_map.end());
        m_vptr_map.erase(vptr);
        auto it = m_pre_commit_map.find(vptr);
        if (it != m_pre_commit_map.end()) {
            pre_commit_op = it->second;
            m_pre_commit_map.erase(it);                    
        }

        if (m_atomic) {
            for (auto &volatile_ptr: m_volatile) {
                if (volatile_ptr == vptr) {
                    volatile_ptr = nullptr;
                }
            }
        }
        return pre_commit_op;
    }

}