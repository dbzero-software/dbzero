// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "has_fixture.hpp"
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/GC0.hpp>
#include <dbzero/object_model/value/StorageClass.hpp>
#include <dbzero/object_model/LangConfig.hpp>
#include <dbzero/workspace/AtomicContext.hpp>

namespace db0

{
    
    using StorageClass = db0::object_model::StorageClass;    
    
    template <typename T> void addToGC0(Fixture &fixture, void *vptr) {
        fixture.getGC0().add<T>(vptr);
    }

    template <typename T> bool tryAddToGC0(Fixture &fixture, void *vptr) 
    {
        auto gc0_ptr = fixture.tryGetGC0();
        if (!gc0_ptr) {
            return false;
        }
        gc0_ptr->add<T>(vptr);
        return true;
    }

    /**
     * The base class for all Fixture based v_objects
     * @tparam BaseT must be some v_object or derived class
     * @tparam _CLS the storage class (for GC0 integration)
     * @tparam T the actual type of the object. T must implement the following optional operations (or specialzations): destroy, detach
     * @tparam unique if true, the object will be created with a unique address
     * and must contain the m_header as the first overlaid member and define static GCOps_ID m_gc_ops_id as its member (see GC0_Declare macro)
    */
    template <typename T, typename BaseT, StorageClass _CLS, bool Unique = true>
    class ObjectBase: public has_fixture<BaseT>
    {
    public:
        using self_t = ObjectBase<T, BaseT, _CLS, Unique>;
        using LangToolkit = db0::object_model::LangConfig::LangToolkit;
        using ObjectPtr = LangToolkit::ObjectPtr;
        
        // Constructs a "null" placeholder instance
        ObjectBase() = default;
        
        // Create a new instance
        template <typename... Args> ObjectBase(db0::swine_ptr<Fixture> &fixture, Args &&... args)
            : has_fixture<BaseT>()
        {
            initNew(fixture, std::forward<Args>(args)...);            
            addToGC0<T>(*fixture, this);
            m_gc_registered = true;
        }
        
        // Create a new instance (no garbage collection)
        struct tag_no_gc {};
        template <typename... Args> ObjectBase(tag_no_gc, db0::swine_ptr<Fixture> &fixture, Args &&... args)
            : has_fixture<BaseT>()            
        {
            initNew(fixture, std::forward<Args>(args)...);
        }
        
        // Fetch an existing instance
        struct tag_from_address {};
        ObjectBase(tag_from_address, db0::swine_ptr<Fixture> &fixture, Address address, AccessFlags access_mode = {})
            : has_fixture<BaseT>(typename has_fixture<BaseT>::tag_from_address(), fixture, address, 0, access_mode)
        {
            m_gc_registered = tryAddToGC0<T>(*fixture, this);
        }

        // Move existing instance / stem
        struct tag_from_stem {};
        ObjectBase(tag_from_stem, db0::swine_ptr<Fixture> &fixture, BaseT &&stem)
            : has_fixture<BaseT>(typename has_fixture<BaseT>::tag_from_stem(), fixture, std::move(stem))        
        {
            m_gc_registered = tryAddToGC0<T>(*fixture, this);
        }
        
        ~ObjectBase()
        {      
        }
        
        // Unregister must be called pre-destruction
        void unregister() const
        {
            // remove from the registry (on condition the underlying instance & fixture still exists)
            if (m_gc_registered && hasInstance()) {
                auto fixture = this->tryGetFixture();
                if (fixture) {
                    fixture->getGC0().tryRemove((void*)this);
                }
                m_gc_registered = false;
            }
        }
        
        /**
         * Initialize the instance in place with a unique address
        */
        template <typename... Args> void init(db0::swine_ptr<Fixture> &fixture, Args &&... args)
        {
            unregister();
            initNew(fixture, std::forward<Args>(args)...);
            m_gc_registered = tryAddToGC0<T>(*fixture, this);
        }
        
        inline bool hasInstance() const {
            return !has_fixture<BaseT>::isNull();
        }

        void operator=(const ObjectBase &other) = delete;

        // GC0 associated members
        static StorageClass storageClass() {
            return _CLS;
        }
        
        void incRef(bool is_tag)
        {
            assert(hasInstance());
            this->modify().m_header.incRef(is_tag);
        }
        
        // @return true if reference count was decremented to zero
        bool decRef(bool is_tag)
        {
            assert(hasInstance());
            return this->modify().m_header.decRef(is_tag);
        }
        
        // tags / objects reference counts
        std::pair<std::uint32_t, std::uint32_t> getRefCounts() const
        {
            assert(hasInstance());
            return (*this)->m_header.m_ref_counter.get();
        }

        bool hasRefs() const 
        {
            assert(hasInstance());
            return (*this)->m_header.hasRefs();
        }
        
        // The implementation registers the underlying language specific instance
        // for detach (on rollback) but only if atomic operation is in progress
        void beginModify(ObjectPtr);

        void moveTo(db0::swine_ptr<Fixture> &);
        
        template <bool C = Unique, typename = std::enable_if_t<C> >
        UniqueAddress getUniqueAddress() const
        {
            assert(hasInstance());
            return UniqueAddress(this->getAddress(), (*this)->m_header.m_instance_id);
        }

        // Get unique object's instance ID
        template <bool C = Unique, typename = std::enable_if_t<C> >
        std::uint16_t getInstanceId() const
        {
            assert(hasInstance());
            return (*this)->m_header.m_instance_id;
        }
        
        static bool checkUnload(db0::swine_ptr<Fixture> &fixture, Address address) {
            // NOTE: check address within a type-specific realm
            return fixture->isAddressValid(address, BaseT::getRealmID());
        }
        
        // Check if unload operation would be successful without actually performing it
        // @param check_has_refs flag indicating if the refs-count should also be checked
        static bool checkUnload(db0::swine_ptr<Fixture> &fixture, Address address, std::uint16_t instance_id, 
            bool check_has_refs)
        {
            std::size_t size_of = 0;
            if (!fixture->isAddressValid(address, BaseT::getRealmID(), &size_of)) {
                return false;
            }
            // validate instance ID only if provided
            // NOTE: in this case we also validate if the refs-count is not zero (otherwise it's pending deletion)
            if (instance_id || check_has_refs) {
                // NOTE: here we use trusted size_of retrieved from the allocator
                auto stem = BaseT(db0::tag_verified(), fixture->myPtr(address), size_of);
                if (instance_id && stem->m_header.m_instance_id == instance_id) {
                    return false;
                }
                if (check_has_refs && !stem->hasRefs()) {
                    return false;
                }
                return true;
            }
            return true;
        }
        
        // Destroys an existing instance and constructs a "null" placeholder
        // this operation is required for destroying dbzero instance while still preserving the language wrapper object
        void dropInstance(FixtureLock &)
        {
            reinterpret_cast<T*>(this)->~T();
            // construct a new (placeholder) instance in place of the existing one            
            new ((void*)this) T();
        }
        
        // Get access flags to propagate to members (e.g. no_cache)
        AccessFlags getMemberFlags() const {
            return this->getAccessMode() & AccessOptions::no_cache;
        }

    protected:
        friend class db0::GC0;

        template <typename... Args> void initNew(db0::swine_ptr<Fixture> &fixture, Args &&... args)
        {        
            assert(fixture);
            if constexpr (Unique) {
               auto instance_id = has_fixture<BaseT>::initUnique(fixture, std::forward<Args>(args)...);
               this->modify().m_header.m_instance_id = instance_id;
            } else {
               has_fixture<BaseT>::init(fixture, std::forward<Args>(args)...);
            }
        }
        
        // member should be overridden for derived types which need flush
        using FlushFunction = void (*)(void *, bool revert);
        static FlushFunction getFlushFunction() {
            return nullptr;
        }
        
        // called from GC0 to bind GC_Ops for this type
        static GC_Ops getGC_Ops() {
            return { hasRefsOp, dropOp, detachOp, commitOp, getTypedAddress, dropByAddr, T::getFlushFunction() };
        }
        
        void operator=(ObjectBase &&other)
        {            
            has_fixture<BaseT>::operator=(std::move(other));
            assert(!other.hasInstance());
        }
                
    private:
        // Flag indicating if the instance is registered in GC0
        mutable bool m_gc_registered = false;

        static bool hasRefsOp(const void *vptr) {
            return static_cast<const T*>(vptr)->hasRefs();
        }
        
        static void detachOp(void *vptr) {
            static_cast<T*>(vptr)->detach();
        }

        static void commitOp(void *vptr) {
            static_cast<T*>(vptr)->commit();
        }
        
        static void dropOp(void *vptr) {
            static_cast<T*>(vptr)->destroy();
        }
        
        static std::pair<UniqueAddress, StorageClass> getTypedAddress(const void *vptr) {
            return { static_cast<const T*>(vptr)->getUniqueAddress(), _CLS };
        }
        
        static void dropByAddr(db0::swine_ptr<Fixture> &fixture, Address addr)
        {
            // this code creates an instance which will be registered in GC0
            // and immediately unregistered, if its ref-count is 0 then it will get dropped
            T instance(fixture, addr);
        }
    };
    
    template <typename T, typename BaseT, StorageClass _CLS, bool Unique>
    void ObjectBase<T, BaseT, _CLS, Unique>::beginModify(ObjectPtr ptr)
    {
        if (hasInstance()) {
            auto fixture = this->tryGetFixture();
            if (fixture) {
                auto atomic_context_ptr = fixture->tryGetAtomicContext();
                if (atomic_context_ptr) {
                    atomic_context_ptr->add(this->getAddress(), ptr);
                }
            }
        }
    }
    
    template <typename T, typename BaseT, StorageClass _CLS, bool Unique>
    void ObjectBase<T, BaseT, _CLS, Unique>::moveTo(db0::swine_ptr<Fixture> &fixture)
    {
        // NOTE: newly created instance is not registered in GC0 because existing wrapper object will be reused
        T new_instance(tag_no_gc(), fixture, *static_cast<T*>(this));
        // move instance to a different cache (changing its address)
        fixture->getLangCache().moveFrom(this->getFixture()->getLangCache(), this->getAddress(), 
            new_instance.getAddress());
        // move instance to a different GC0 (preserving the same wrapper object)
        fixture->getGC0().moveFrom<T>(this->getFixture()->getGC0(), this);
        new_instance.m_gc_registered = true;
        auto atomic_ctx_ptr = fixture->tryGetAtomicContext();
        if (atomic_ctx_ptr) {
            // move instance to a different atomic context (changing its address)
            assert(this->getFixture()->tryGetAtomicContext());
            atomic_ctx_ptr->moveFrom(*this->getFixture()->tryGetAtomicContext(), this->getAddress(),
                new_instance.getAddress());
        }
        
        this->destroy();
        *this = std::move(new_instance);
    }
    
}