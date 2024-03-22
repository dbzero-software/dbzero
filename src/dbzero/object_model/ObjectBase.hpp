#pragma once

#include "has_fixture.hpp"
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/GC0.hpp>
#include <dbzero/object_model/value/StorageClass.hpp>

namespace db0

{
    
    using StorageClass = db0::object_model::StorageClass;
    
    /**
     * The base class for all Fixture based v_objects
     * @tparam BaseT must be some v_object or derived class
     * @tparam _CLS the storage class
     * @tparam T the actual type of the object. T must implement the following optional operations (or specialzations): destroy, detach
     * and must contain the m_header as the first overlaid member and define static GCOps_ID m_gc_ops_id as its member (see GC0_Declare macro)
    */
    template <typename T, typename BaseT, StorageClass _CLS>
    class ObjectBase: public has_fixture<BaseT>
    {
    public:        
        ObjectBase() = default;
        
        // create a new instance
        template <typename... Args> ObjectBase(db0::swine_ptr<Fixture> &fixture, Args &&... args)
            : has_fixture<BaseT>(fixture, std::forward<Args>(args)...)
        {
            fixture->getGC0().add<T>(this);
        }
        
        // open existing instance
        struct tag_from_address {};
        ObjectBase(tag_from_address, db0::swine_ptr<Fixture> &fixture, std::uint64_t address)
            : has_fixture<BaseT>(typename has_fixture<BaseT>::tag_from_address(), fixture, address)
        {
            fixture->getGC0().add<T>(this);
        }
        
        // move existing instance / stem
        struct tag_from_stem {};
        ObjectBase(tag_from_stem, db0::swine_ptr<Fixture> &fixture, BaseT &&stem)
            : has_fixture<BaseT>(typename has_fixture<BaseT>::tag_from_stem(), fixture, std::move(stem))
        {            
            fixture->getGC0().add<T>(this);
        }
        
        ~ObjectBase()
        {
            unregister();
        }
        
        /**
         * Initialize the instance in place
        */
        template <typename... Args> void init(db0::swine_ptr<Fixture> &fixture, Args &&... args)
        {            
            unregister();
            has_fixture<BaseT>::init(fixture, std::forward<Args>(args)...);
            fixture->getGC0().add<T>(this);
        }
        
        inline bool hasInstance() const {
            return !has_fixture<BaseT>::isNull();
        }
                        
        void operator=(ObjectBase &&other) = delete;
        void operator=(const ObjectBase &other) = delete;

        // GC0 associated members
        static StorageClass storageClass() {
            return _CLS;
        }
        
        void incRef()
        {
            assert(hasInstance());
            this->modify().m_header.incRef();
        }
        
        void decRef()
        {
            assert(hasInstance());
            this->modify().m_header.decRef();
        }
        
        auto getRefCount()
        {
            assert(hasInstance());
            return (*this)->m_header.m_ref_count;
        }

        bool hasRefs() const
        {
            assert(hasInstance());
            return (*this)->m_header.hasRefs();
        }
        
    protected:
        friend class db0::GC0;

        // member should be overridden for derived types which need pre-commit
        using PreCommitFunction = void (*)(void *);
        static PreCommitFunction getPreCommitFunction() {
            return nullptr;
        }

        // called from GC0 to bind GC_Ops for this type
        static GC_Ops getGC_Ops() {
            return { hasRefsOp, dropOp, detachOp, getTypedAddress, dropByAddr, T::getPreCommitFunction() };
        }

    private:

        void unregister()
        {
            // remove from the registry (on condition the underlying instance & fixture still exists)
            if (hasInstance()) {
                auto fixture = this->tryGetFixture();
                if (fixture) {
                    fixture->getGC0().remove(this);
                }
            }
        }
        
        static bool hasRefsOp(const void *vptr) {
            return (*static_cast<const T*>(vptr))->m_header.hasRefs();
        }

        static void detachOp(void *vptr) {
            static_cast<T*>(vptr)->detach();
        }        

        static void dropOp(void *vptr) {
            static_cast<T*>(vptr)->destroy();
        }
        
        static TypedAddress getTypedAddress(const void *vptr) {
            return { _CLS, static_cast<const T*>(vptr)->getAddress() };
        }

        static void dropByAddr(db0::swine_ptr<Fixture> &fixture, std::uint64_t addr)
        {
            // this code creates an instance which will be registered in GC0
            // and immediately unregistered, if its ref-count is 0 then it will get dropped
            T instance(fixture, addr);
        }
    };

}