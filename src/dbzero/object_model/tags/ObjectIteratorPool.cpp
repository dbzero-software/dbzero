// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 DBZero Software sp. z o.o.

#include "ObjectIteratorPool.hpp"
#include "ObjectIterator.hpp"

namespace db0::object_model
{

    std::shared_ptr<ObjectIterator> ObjectIteratorPool::getIterator(const ObjectWeakPtr &object)
    {
        return object.lock();
    }

    void ObjectIteratorPool::add(const std::shared_ptr<ObjectIterator> &object)
    {
        if (m_closed) {
            return;
        }
        if (!object) {
            return;
        }
        m_iterators.insert_or_assign(object.get(), object);
    }

    void ObjectIteratorPool::remove(ObjectIterator *object_ptr)
    {
        if (!object_ptr) {
            return;
        }
        m_iterators.erase(object_ptr);
    }

    std::size_t ObjectIteratorPool::detach()
    {
        std::size_t detached_count = 0;
        for (auto it = m_iterators.begin(); it != m_iterators.end();) {
            auto iterator = getIterator(it->second);
            if (!iterator) {
                it = m_iterators.erase(it);
                continue;
            }
            iterator->detach();
            ++detached_count;
            ++it;
        }
        return detached_count;
    }

    std::size_t ObjectIteratorPool::detach(std::uint64_t generation)
    {
        if (m_detach_generation == generation) {
            return 0;
        }
        m_detach_generation = generation;
        return detach();
    }

    std::size_t ObjectIteratorPool::cleanup()
    {
        auto old_size = m_iterators.size();
        for (auto it = m_iterators.begin(); it != m_iterators.end();) {
            if (!getIterator(it->second)) {
                it = m_iterators.erase(it);
            } else {
                ++it;
            }
        }
        return old_size - m_iterators.size();
    }

    void ObjectIteratorPool::close()
    {
        m_closed = true;
        m_iterators.clear();
    }

    std::size_t ObjectIteratorPool::size() const
    {
        return m_iterators.size();
    }

    bool ObjectIteratorPool::isClosed() const
    {
        return m_closed;
    }

}
