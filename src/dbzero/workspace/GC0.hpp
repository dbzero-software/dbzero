#pragma once

#include <vector>
#include <unordered_map>
#include <mutex>
#include <optional>
#include <dbzero/core/vspace/v_ptr.hpp>
#include <dbzero/core/collections/vector/v_bvector.hpp>
#include <dbzero/object_model/value/TypedAddress.hpp>
#include <dbzero/object_model/value/StorageClass.hpp>
#include <dbzero/core/memory/swine_ptr.hpp>
#include <dbzero/object_model/has_fixture.hpp>

namespace db0

{
        
    class Fixture;
    
    // C-style hasrefs / drop / detatch functions
    using HasRefsFunction = bool (*)(const void *);
    using NoArgsFunction = void (*)(void *);
    using TypedAddress = db0::object_model::TypedAddress;
    using GetTypedAddress = TypedAddress (*)(const void *);
    using StorageClass = db0::object_model::StorageClass;
    using DropByAddrFunction = void (*)(db0::swine_ptr<Fixture> &, std::uint64_t);
    using PreCommitFunction = void (*)(void *, bool revert);
    
    struct GC_Ops
    {
        HasRefsFunction hasRefs = nullptr;
        NoArgsFunction drop = nullptr;
        NoArgsFunction detach = nullptr;
        // commit is a lightweight version of "detach" for a writer process
        NoArgsFunction commit = nullptr;
        GetTypedAddress address = nullptr;
        DropByAddrFunction dropByAddr = nullptr;        
        // null allowed, preCommit handler is called just before fixture.commit
        PreCommitFunction preCommit = nullptr;
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
    
#define GC0_Declare protected: \
    friend class db0::GC0; \
    static GCOps_ID m_gc_ops_id;

#define GC0_Define(T) GCOps_ID T::m_gc_ops_id;

    /**
     * GC0 keeps track of all "live" v_ptr instances.
     * and drops associated DBZero instances once they are no longer referenced from Python
     * GC0 has also a persistence layer to keep track of unreferenced instances as long as
     * the corresponding Python objects are still alive.
    */
    class GC0: public db0::has_fixture<v_bvector<TypedAddress> >
    {
    public:
        using super_t = has_fixture<v_bvector<TypedAddress> >;
        GC0(db0::swine_ptr<Fixture> &);
        GC0(db0::swine_ptr<Fixture> &, std::uint64_t address, bool read_only);
        ~GC0();
        
        // register instance with type specific ops, must be a known / registered type
        template <typename T> void add(void *vptr);
        // move instance from another GC0
        template <typename T> void moveFrom(GC0 &other, void *vptr);
        
        // preCommit calls the operation on objects which implement it
        void preCommit();

        /**
         * Unregister instance (i.e. when reference from Python was removed)
         * @return true if object was also dropped
         */
        bool tryRemove(void *vptr, bool is_volatile = false);
        
        /**
         * Detach all instances held by this registry.
        */
        void detachAll();
        void commitAll();
        
        std::size_t size() const;

        struct CommitContext
        {
            GC0 &m_gc0;

            CommitContext(GC0 &gc0);
            ~CommitContext();

            void commit();
        };

        /**
         * Commit serializes the list of unreferenced instances to the persistence layer
         * this is to be able to drop those instances once the corresponding references from Python expire
        */
        std::unique_ptr<CommitContext> beginCommit();

        template <typename... T> static void registerTypes();

        /**
         * The collect operation visits all stored references and drops
         * instances with a zero ref-count.
        */
        void collect();

        void beginAtomic();
        void endAtomic();
        void cancelAtomic();

    protected:
        friend CommitContext;
        bool m_commit_pending = false;

        void commit();
        // @return pre-commit ops-id if element was assigned it
        std::optional<unsigned int> erase(void *vptr);
        
    private:
        static std::vector<GC_Ops> m_ops;
        // GC-ops by storage class
        static std::unordered_map<StorageClass, GCOps_ID> m_ops_map;
        // flag indicating if static bindings were initialized
        static bool m_initialized;
        const bool m_read_only;
        // type / ops_id
        std::unordered_map<void*, unsigned int> m_vptr_map;
        // the map dedicated to instances which implement preCommit
        // it's assumed that it's much smaller than m_vptr_map (it duplicates some of its entries)
        std::unordered_map<void*, unsigned int> m_pre_commit_map;
        // flag indicating atomic operation in progress
        bool m_atomic = false;        
        // the list of volatile instances - i.e. created during atomic operation
        std::vector<void*> m_volatile;
        mutable std::mutex m_mutex;
        
        template <typename T> static void registerSingleType()
        {
            // auto storage_class = T::storageClass();
            T::m_gc_ops_id = GCOps_ID(m_ops.size());
            m_ops.push_back(T::getGC_Ops());
            m_ops_map[T::storageClass()] = T::m_gc_ops_id;
        }
    };
    
    template <typename T> void GC0::add(void *vptr)
    {
        assert(vptr);
        std::unique_lock<std::mutex> lock(m_mutex);
        // detach function must always be provided
        assert(m_ops[T::m_gc_ops_id].detach);
        assert(m_ops[T::m_gc_ops_id].address);
        m_vptr_map[vptr] = T::m_gc_ops_id;
        // if the type implements preCommit then also add it to the preCommit map
        if (m_ops[T::m_gc_ops_id].preCommit) {
            m_pre_commit_map[vptr] = T::m_gc_ops_id;
        }
        if (m_atomic) {
            m_volatile.push_back(vptr);
        }
    }
    
    template <typename T> void GC0::moveFrom(GC0 &other, void *vptr)
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        auto pre_commit_op = other.erase(vptr);
        m_vptr_map[vptr] = T::m_gc_ops_id;
        // also move between pre-commit maps
        if (pre_commit_op) {
            m_pre_commit_map[vptr] = *pre_commit_op;
        }
        if (m_atomic) {
            m_volatile.push_back(vptr);
        }
    }
    
    template <typename... T> void GC0::registerTypes()
    {        
        if (m_initialized) {
            return;
        }
        
        (registerSingleType<T>(), ...);
        m_initialized = true;
    }

}
