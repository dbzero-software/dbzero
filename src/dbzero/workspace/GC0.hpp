// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <vector>
#include <unordered_map>
#include <mutex>
#include <optional>
#include <dbzero/core/vspace/vtypeless.hpp>
#include <dbzero/core/vspace/v_object.hpp>
#include <dbzero/core/collections/vector/v_bvector.hpp>
#include <dbzero/object_model/value/TypedAddress.hpp>
#include <dbzero/object_model/value/StorageClass.hpp>
#include <dbzero/core/memory/swine_ptr.hpp>
#include <dbzero/object_model/has_fixture.hpp>

namespace db0

{
        
    class Fixture;
    class ProcessTimer;
        
    using TypedAddress = db0::object_model::TypedAddress;
    using StorageClass = db0::object_model::StorageClass;

    // C-style hasrefs / drop / detatch functions
    using HasRefsFunction = bool (*)(const void *);
    using NoArgsFunction = void (*)(void *);
    using GetAddress = std::pair<UniqueAddress, StorageClass> (*)(const void *);
    using StorageClass = db0::object_model::StorageClass;
    using DropByAddrFunction = void (*)(db0::swine_ptr<Fixture> &, Address);
    using FlushFunction = void (*)(void *, bool revert);
    
    struct GC_Ops
    {
        HasRefsFunction hasRefs = nullptr;
        NoArgsFunction drop = nullptr;
        NoArgsFunction detach = nullptr;
        // commit is a lightweight version of "detach" for a writer process
        NoArgsFunction commit = nullptr;
        GetAddress address = nullptr;
        DropByAddrFunction dropByAddr = nullptr;        
        // null allowed, flush handler is called just before fixture.commit
        FlushFunction flush = nullptr;
    };
    
    struct GCOps_ID
    {
        unsigned int m_value;

        GCOps_ID() = default;
        explicit inline GCOps_ID(unsigned int id)
            : m_value(id)
        {
        }
        
        inline operator unsigned int() const {
            return m_value;
        }
    };

    struct GC0_SharedState
    {
        std::vector<GC_Ops> m_ops;
        // GC-ops by storage class
        std::unordered_map<StorageClass, GCOps_ID> m_ops_map;
        // flag indicating if static bindings were initialized
        bool m_initialized;

        GC0_SharedState() = default;
        // We don't want accidental copy
        GC0_SharedState(const GC0_SharedState&) = delete;
        GC0_SharedState(GC0_SharedState&&) = delete;
        GC0_SharedState& operator=(const GC0_SharedState&) = delete;
        GC0_SharedState& operator=(GC0_SharedState&) = delete;
    };
    
    
#define GC0_Declare protected: \
    friend class db0::GC0; \
    static GCOps_ID m_gc_ops_id;

#define GC0_Define(T) GCOps_ID T::m_gc_ops_id;
    
    /**
     * GC0 keeps track of all "live" v_object instances.
     * and drops associated dbzero instances once they are no longer referenced from Python
     * GC0 has also a persistence layer to keep track of unreferenced instances as long as
     * the corresponding Python objects are still alive.
    */
    class GC0: public db0::has_fixture<v_bvector<TypedAddress> >
    {
    public:
        using super_t = has_fixture<v_bvector<TypedAddress> >;
        GC0(db0::swine_ptr<Fixture> &);
        GC0(db0::swine_ptr<Fixture> &, Address address, bool read_only);
        ~GC0();
        
        // register instance with type specific ops, must be a known / registered type
        template <typename T> void add(void *vptr);
        // move instance from another GC0
        template <typename T> void moveFrom(GC0 &other, void *vptr);
        
        /**
         * Unregister instance (i.e. when reference from Python was removed)
         * @return true if object was also dropped
         */
        bool tryRemove(void *vptr, bool is_volatile = false);
        
        // flush calls the operation on objects which implement it
        void flushAllOf(const std::vector<vtypeless*> &, ProcessTimer * = nullptr);

        // Detach all instances held by this registry
        void detachAll();

        std::size_t size() const;

        template <typename... T> static void registerTypes();

        /**
         * The collect operation visits all stored references and drops
         * instances with a zero ref-count.
        */
        void collect();

        void beginAtomic();
        void endAtomic();
        void cancelAtomic();
        
        struct CommitContext
        {
            GC0 &m_gc0;

            CommitContext(GC0 &gc0);
            ~CommitContext();

            void commitAllOf(const std::vector<vtypeless*> &, ProcessTimer * = nullptr);
        };
        
        std::unique_ptr<CommitContext> beginCommit();

    protected:
        bool m_commit_pending = false;
        
        // Commit specific (e.g. modified) instances held by this registry
        void commitAllOf(const std::vector<vtypeless*> &, ProcessTimer * = nullptr);        
        // @return flush ops-id if element was assigned it
        std::optional<unsigned int> erase(void *vptr);
        
    private:
        // Keep shared state 'alive' until it isn't needed anymore 
        std::shared_ptr<GC0_SharedState> m_shared_state;
        static std::shared_ptr<GC0_SharedState>& getGlobalSharedState();
        GC0_SharedState& getSharedState();

        const bool m_read_only;
        // type / ops_id
        std::unordered_map<void*, unsigned int> m_vptr_map;
        // the map dedicated to instances which implement flush
        // it's assumed that it's much smaller than m_vptr_map (it duplicates some of its entries)
        std::unordered_map<void*, unsigned int> m_flush_map;
        // objects irrevocably scheduled for deletion
        std::unordered_map<UniqueAddress, StorageClass> m_scheduled_for_deletion;
        // flag indicating atomic operation in progress
        bool m_atomic = false;
        // the list of volatile instances - i.e. created during atomic operation
        std::vector<void*> m_volatile;
        mutable std::mutex m_mutex;
        
        void commitAll();

        template <typename T> static void registerSingleType()
        {            
            auto &state = getGlobalSharedState();
            T::m_gc_ops_id = GCOps_ID(state->m_ops.size());
            state->m_ops.push_back(T::getGC_Ops());
            state->m_ops_map[T::storageClass()] = T::m_gc_ops_id;
        }
    };
    
    template <typename T> void GC0::add(void *vptr)
    {
        // vptr must not be null
        assert(vptr);
        std::unique_lock<std::mutex> lock(m_mutex);
        // detach function must always be provided
        auto &ops_list = getSharedState().m_ops;
        assert(ops_list[T::m_gc_ops_id].detach);
        assert(ops_list[T::m_gc_ops_id].address);
        m_vptr_map[vptr] = T::m_gc_ops_id;
        // if the type implements flush then also add it to the flush map
        if (ops_list[T::m_gc_ops_id].flush) {
            m_flush_map[vptr] = T::m_gc_ops_id;
        }
        if (m_atomic) {
            m_volatile.push_back(vptr);
        }
    }
    
    template <typename T> void GC0::moveFrom(GC0 &other, void *vptr)
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        auto flush_op = other.erase(vptr);
        m_vptr_map[vptr] = T::m_gc_ops_id;
        // also move between flush maps
        if (flush_op) {
            m_flush_map[vptr] = *flush_op;
        }
        if (m_atomic) {
            m_volatile.push_back(vptr);
        }
    }
    
    template <typename... T> void GC0::registerTypes()
    {        
        auto &state = getGlobalSharedState();
        if (state->m_initialized) {
            return;
        }
        
        (registerSingleType<T>(), ...);
        state->m_initialized = true;
    }

}
