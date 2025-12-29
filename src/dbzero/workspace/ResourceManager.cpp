// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "ResourceManager.hpp"
#include <dbzero/core/exception/Exceptions.hpp>

#include <unordered_set>
#include <cassert>

using namespace std;

namespace db0

{

    ResourceManager::~ResourceManager() 
    {
    }

    db0::progressive_mutex &db0::ResourceManager::getResourceMutex(ResourcePtr ptr) const
    {
        db0::progressive_mutex::scoped_lock lock(m_mutex);
        for (;;) {
            lock.lock();
            auto it = m_resource_mutexes.find(ptr);
            if (it == m_resource_mutexes.end()) {
                if (!lock.upgradeToUniqueLock()) {
                    continue;
                }
                std::unique_ptr<db0::progressive_mutex> mutex(new db0::progressive_mutex());
                it = m_resource_mutexes.emplace(ptr, std::move(mutex)).first;
            }
            return *(it->second);
        }
    }

    std::unique_ptr<db0::LockObject> db0::ResourceManager::lockShared(ResourcePtr ptr) const
    {
        std::unique_ptr<db0::progressive_mutex::scoped_lock> lock(new db0::progressive_mutex::scoped_read_lock(
                getResourceMutex(ptr))
        );
        // Register lock as shared (lock has been granted)
        {
            db0::progressive_mutex::scoped_unique_lock lock(m_mutex);
            auto it = m_locks.find(ptr);
            if (it == m_locks.end()) {
                it = m_locks.emplace(ptr, 0).first;
            }
            ++(it->second);
        }
        return std::unique_ptr<LockObject>(new LockObject(*this, ptr, std::move(lock), false));
    }

    std::unique_ptr<db0::UniqueLockObject> db0::ResourceManager::lockExclusive(ResourcePtr ptr) const
    {
        std::unique_ptr<db0::progressive_mutex::scoped_lock> lock(
                new db0::progressive_mutex::scoped_unique_lock(getResourceMutex(ptr))
        );
        // Register lock
        {
            db0::progressive_mutex::scoped_unique_lock lock(m_mutex);
            auto it = m_locks.find(ptr);
            if (it == m_locks.end()) {
                it = m_locks.emplace(ptr, 0).first;
            }
            assert(it->second == 0);
            it->second = UNIQUE_LOCK_ID;
        }
        return std::unique_ptr<UniqueLockObject>(
                new UniqueLockObject(*this, ptr, std::move(lock))
        );
    }

    void db0::ResourceManager::deleteResource(ResourcePtr ptr)
    {
        db0::progressive_mutex::scoped_unique_lock lock(m_mutex);
        {
            auto it = m_resource_mutexes.find(ptr);
            if (it != m_resource_mutexes.end()) {
                m_resource_mutexes.erase(it);
            }
        }
        // Unregister from m_resources data vector
        {
            auto it = m_resources.find(ptr.m_type_id);
            if (it != m_resources.end()) {
                auto it_res = it->second.begin(), end = it->second.end();
                for (; it_res != end; ++it_res) {
                    if (*it_res == ptr) {
                        it->second.erase(it_res);
                        break;
                    }
                }
            }
        }
    }

    db0::LockObject::LockObject(const ResourceManager &ref, ResourcePtr ptr,
        std::unique_ptr<db0::progressive_mutex::scoped_lock> &&lock, bool is_unique)
        : m_ref(ref)
        , m_resource_ptr(ptr)
        , m_scoped_lock(std::move(lock))
        , m_is_unique(is_unique)
    {
    }

    db0::LockObject::~LockObject() {
        m_ref.onLockReleased(m_resource_ptr, m_is_unique);
    }

    void db0::ResourceManager::onLockReleased(ResourcePtr ptr, bool is_unique) const
    {
        db0::progressive_mutex::scoped_unique_lock lock(m_mutex);
        auto it = m_locks.find(ptr);
        assert(it != m_locks.end());
        if (is_unique) {
            it->second = 0;
        } else {
            --(it->second);
        }
    }

    void db0::ResourceManager::addNewResource(ResourcePtr ptr) 
    {
        db0::progressive_mutex::scoped_unique_lock lock(m_mutex);
        // create mutex and also add to m_resources vector (avoiding duplicates)
        auto it = m_resource_mutexes.find(ptr);
        if (it == m_resource_mutexes.end()) {
            std::unique_ptr<db0::progressive_mutex> mutex(new db0::progressive_mutex());
            m_resource_mutexes.emplace(ptr, std::move(mutex)).first;
            m_resources[ptr.m_type_id].emplace_back(ptr);
        }
    }

    db0::UniqueLockObject::UniqueLockObject(const ResourceManager &ref, ResourcePtr ptr,
        std::unique_ptr<db0::progressive_mutex::scoped_lock> &&lock)
        : LockObject(ref, ptr, std::move(lock), true) 
    {
    }

    std::uint32_t db0::ResourceManager::count(std::type_index type_id) const
    {
        db0::progressive_mutex::scoped_read_lock lock(m_mutex);
        auto it = m_resources.find(type_id);
        if (it != m_resources.end()) {
            return it->second.size();
        }
        return 0;
    }

    std::unique_ptr<db0::UniqueLockObject> db0::ResourceManager::selectUnique(std::type_index type_id,
        std::function<bool(ResourcePtr)> match_criteria, bool throw_if_nothing_matched) const
    {
        for (;;) {
            int matched = 0;
            std::unordered_set<std::uint32_t> used_items;
            db0::progressive_mutex::scoped_unique_lock lock(m_mutex);
            auto it = m_resources.find(type_id);
            if (it == m_resources.end()) {
                if (throw_if_nothing_matched) {
                    THROWF(db0::InternalException)
                        << "Match criteria did not match any available resource for type: "
                        << type_id.name();
                } else {
                    // break (and then return null)
                    break;
                }
            }
            // we add some randomness to resource search operation
            auto &resources = it->second;
            if (resources.empty()) {
                if (throw_if_nothing_matched) {
                    THROWF(db0::InternalException)
                            << "Match criteria did not match any available resource for type: "
                            << type_id.name();
                } else {
                    // break (and then return null)
                    break;
                }
            }
            std::uint32_t end_index = resources.size();
            std::uint32_t max_step = end_index;
            std::uint32_t index = (std::uint32_t) rand() % end_index;
            std::uint32_t half_index = end_index / 2;
            while (used_items.size() < end_index) {
                auto it = used_items.find(index);
                if (it == used_items.end()) {
                    used_items.insert(index);
                    ResourcePtr resource_ptr = resources[index];
                    if (match_criteria(resource_ptr)) {
                        ++matched;
                        if (getLocks(resource_ptr, lock) == 0) {
                            lock.release();
                            return lockExclusive(resource_ptr);
                        }
                    }
                }
                if (max_step > 1) {
                    index = (index + (rand() % max_step) + 1) % end_index;
                    // adjust max_step if >50% elements have been checked randomly
                    if (used_items.size() >= half_index) {
                        max_step = 1;
                    }
                } else {
                    ++index;
                    if (index == end_index) {
                        index = 0;
                    }
                }
            }
            if (matched == 0) {
                if (throw_if_nothing_matched) {
                    THROWF(db0::InternalException)
                            << "Match criteria did not match any available resource for type: "
                            << type_id.name();
                } else {
                    // break (and then return null)
                    break;
                }
            }
        }
        return std::unique_ptr<db0::UniqueLockObject>();
    }

    std::uint32_t db0::ResourceManager::getLocks(ResourcePtr ptr, db0::progressive_mutex::scoped_lock &lock) const
    {
        // must be at least read locked
        assert(lock.isLocked());
        auto it = m_locks.find(ptr);
        if (it == m_locks.end()) {
            return 0;
        }
        return it->second;
    }

    db0::ResourcePtr::ResourcePtr(std::type_index type_id, void *ptr)
        : m_type_id(type_id)
        , m_ptr(ptr) 
    {
    }

    bool db0::ResourcePtr::operator==(const ResourcePtr &ptr) const {
        return (this->m_type_id == ptr.m_type_id && this->m_ptr == ptr.m_ptr);
    }
    
}
