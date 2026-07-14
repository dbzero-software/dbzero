// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once
    
#include <unordered_map>
#include <vector>
#include <functional>
#include <cstdint>
#include <memory>
#include <typeindex>
#include <dbzero/core/threading/ProgressiveMutex.hpp>

namespace db0

{

    /**
     * Resource type ID + pointer (and type name)
     */
    struct ResourcePtr
    {
        std::type_index m_type_id;
        void *m_ptr;

        ResourcePtr(std::type_index type_id, void *ptr);

        template <typename T> T &upcast();

        template <typename T> const T &upcast() const;

        /**
         * Compare to other instance (simply match pointers)
         */
        template <typename T> bool equals(const T &other) const {
            return &other==m_ptr;
        }

        bool operator==(const ResourcePtr&) const;
    };

} 

namespace std

{

    template <> struct hash<db0::ResourcePtr>
    {
    public :
        std::size_t operator()(const db0::ResourcePtr &ptr) const
        {
            return m_hash_type_index(ptr.m_type_id) + m_hash_void_ptr(ptr.m_ptr);
        }

    private:
        hash<std::type_index> m_hash_type_index;
        hash<void*> m_hash_void_ptr;
    };

} // std namespace {

namespace db0

{

    class ResourceManager;

    class LockObject
    {
    public :
        LockObject(const ResourceManager &, ResourcePtr,
            std::unique_ptr<db0::progressive_mutex::scoped_lock> &&lock, bool is_unique);
        ~LockObject();

        /**
         * Access resource object
         */
        template <typename T> const T &getResource() const
        {
            return m_resource_ptr.upcast<T>();
        }

    protected :
        const ResourceManager &m_ref;
        ResourcePtr m_resource_ptr;
        std::unique_ptr<db0::progressive_mutex::scoped_lock> m_scoped_lock;
        const bool m_is_unique;
    };

    class UniqueLockObject : public LockObject
    {
    public :
        UniqueLockObject(const ResourceManager &, ResourcePtr, 
            std::unique_ptr<db0::progressive_mutex::scoped_lock> &&lock);

        /**
         * Access resource object
         */
        template <typename T> T &getResource() 
        {
            return m_resource_ptr.upcast<T>();
        }
    };

    /**
     * Resource Manager supports thread access to specific process resources
     * specific resource can be accessed either in shared (read only) or exclusive (read / write) mode
     */
    class ResourceManager
    {
    public :
        ResourceManager() = default;

        virtual ~ResourceManager();

        /**
         * Lock specific resource (identified by &resource) for read only
         * thread blocks until read permissions have been granted
         * @param resource reference to resource object
         * @return scoped lock object
         */
        template <typename T> std::unique_ptr<LockObject> lockForRead(const T &resource) const {
            return lockShared(getResourcePtr(resource));
        }

        /**
         * Lock specific resource (identified by &resource) in exclusive mode (for read / write)
         * thread blocks until read permissions have been granted
         * @param resource reference to resource object
         * @return scoped lock object
         */
        template <typename T> std::unique_ptr<UniqueLockObject> lockUnique(T &resource) {
            return lockExclusive(getResourcePtr(resource));
        }

        /**
         * Pre-register resource
         */
        template <typename T> void addResource(const T &resource) {
            addNewResource(getResourcePtr(resource));
        }

        /**
         * This should be called to notify ResourceManager instance that resource instance has been deleted
         * no resource locks can exist when this is called
         */
        template <typename T> void onResourceDeleted(const T &resource) {
            deleteResource(getResourcePtr(resource));
        }

        /**
         * @param type_id
         * @return number of resources of type "type_id" available to resource manager
         */
        std::uint32_t count(std::type_index type_id) const;

        /**
         * @param throw_if_nothing_matched if true will throw (default), otherwise will return null pointer
         */
        std::unique_ptr<UniqueLockObject> selectUnique(std::type_index type_id,
            std::function<bool(ResourcePtr)> match_criteria, bool throw_if_nothing_matched) const;
        
        /**
         * Select resource of specific type (meeting match_criteria) and lock as unique
         * will throw exception if none of existing resources match criteria
         * NOTICE: resource should be added with "addResource" prior to calling this routine
         * @param throw_if_nothing_matched if true will throw (default), otherwise will return null pointer
         */
        template <typename T> std::unique_ptr<UniqueLockObject> selectUnique(std::function<bool(ResourcePtr)> match_criteria,
            bool throw_if_nothing_matched = true) const
        {
            return selectUnique(std::type_index(typeid(T)), match_criteria, throw_if_nothing_matched);
        }

        /**
         * Try select resource in exclusive mode (unique), throws if no resource available
         * @return
         */
        template <typename T> std::unique_ptr<UniqueLockObject> selectUnique() {
            return selectUnique(std::type_index(typeid(T)), [](ResourcePtr) { return true; }, true);
        }

        /**
         * Select resource of type T, and return reference to it
         * NOTICE: this method will pick arbitrary resource using selectUnique, then scrap reference and unlock
         * @return reference or throw
         */
        template <typename T> T &select() 
        {
            auto unique_lock = selectUnique<T>([](ResourcePtr ) { return true; }, true);
            return unique_lock->template getResource<T>();
        }

        template <typename T> const T &select() const 
        {
            auto unique_lock = selectUnique<T>([](ResourcePtr ) { return true; }, true);
            return unique_lock->template getResource<T>();
        }

        /**
         * Select resource matching specific criteria, throws if nothing matched
         * @tparam T
         * @return resource reference
         */
        template <typename T> T &select(std::function<bool(T&)> condition)
        {
            auto unique_lock = selectUnique<T>([&](ResourcePtr ptr) { return condition(ptr.upcast<T>()); }, true);
            return unique_lock->template getResource<T>();
        }

        /**
         * Attempt select resource of type T, return nullptr if no such resource is available
         * @return pointer to resource or nullptr
         */
        template <typename T> T *trySelect() 
        {
            auto unique_lock = selectUnique<T>([](ResourcePtr ) { return true; }, false);
            if (!unique_lock.get()) {
                return nullptr;
            }
            return &unique_lock->template getResource<T>();
        }

        /**
         * Count number of resources of type T available to resource manager
         * @return number of available resources
         */
        template <typename T> std::uint32_t count()
        {
            return count(std::type_index(typeid(T)));
        }

        /**
         * List all resources of specific kind
         * @tparam T
         */
        template <typename T> void forAll(std::function<void(const T &instance)> f) const
        {
            auto type_id = std::type_index(typeid(T));
            db0::progressive_mutex::scoped_read_lock read_lock(m_mutex);
            auto it = m_resources.find(type_id);
            if ( it!=m_resources.end() ) {
                for (auto &item: it->second) {
                    // run function f for each resource
                    f(item.template upcast<T>());
                }
            }
        }
        
    protected :
        friend class LockObject;
        friend class UniqueLockObject;

        /**
         * Called back by LockObject when destroyed
         */
        void onLockReleased(ResourcePtr resource_ptr, bool is_unique) const;

    private:
        static constexpr std::uint64_t UNIQUE_LOCK_ID = 0xffffffff;

        mutable db0::progressive_mutex m_mutex;
        // this vector will only hold resources explicitly added with addResource (grouped by type)
        mutable std::unordered_map<std::type_index, std::vector<ResourcePtr> > m_resources;
        mutable std::unordered_map<ResourcePtr, std::unique_ptr<db0::progressive_mutex> > m_resource_mutexes;

        // either number of shared locks or m_unique_lock_id (0xffffffff) for unique lock
        mutable std::unordered_map<ResourcePtr, std::uint32_t> m_locks;

        db0::progressive_mutex &getResourceMutex(ResourcePtr) const;

        void deleteResource(ResourcePtr);

        std::uint32_t getLocks(ResourcePtr, db0::progressive_mutex::scoped_lock &) const;

        template <typename T> ResourcePtr getResourcePtr(const T &instance) const 
        {
            return ResourcePtr(std::type_index(typeid(T)), (void*)(&instance));
        }

        std::unique_ptr<LockObject> lockShared(ResourcePtr resource_ptr) const;
        std::unique_ptr<UniqueLockObject> lockExclusive(ResourcePtr resource_ptr) const;
        void addNewResource(ResourcePtr resource_ptr);
    };
    
    template <typename T> inline T& db0::ResourcePtr::upcast()
    {
        return *reinterpret_cast<T*>(m_ptr);
    }

    template <typename T> inline const T& db0::ResourcePtr::upcast() const
    {
        return *reinterpret_cast<const T*>(m_ptr);
    }

}