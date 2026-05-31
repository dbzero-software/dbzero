// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 DBZero Software sp. z o.o.

#include "ObjectIteratorPool.hpp"
#include "ObjectIterator.hpp"

namespace db0::object_model
{

    ObjectIterator *ObjectIteratorPool::getIterator(ObjectSharedExtPtr const &object)
    {
        auto *py_iterator = object.get();
        if (!py_iterator) {
            return nullptr;
        }
        auto iterator_ptr = py_iterator->getSharedPtr();
        return iterator_ptr.get();
    }

    void ObjectIteratorPool::add(ObjectSharedExtPtr object)
    {
        if (m_closed) {
            return;
        }
        if (object.get() != nullptr) {
            m_iterators.push_back(std::move(object));
        }
    }

    std::size_t ObjectIteratorPool::detach()
    {
        std::size_t detached_count = 0;
        auto out = m_iterators.begin();
        for (auto it = m_iterators.begin(); it != m_iterators.end(); ++it) {
            auto *iterator = getIterator(*it);
            if (!iterator) {
                continue;
            }
            iterator->detach();
            ++detached_count;
            if (out != it) {
                *out = std::move(*it);
            }
            ++out;
        }
        m_iterators.erase(out, m_iterators.end());
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
        auto out = m_iterators.begin();
        for (auto it = m_iterators.begin(); it != m_iterators.end(); ++it) {
            if (!getIterator(*it)) {
                continue;
            }
            if (out != it) {
                *out = std::move(*it);
            }
            ++out;
        }
        m_iterators.erase(out, m_iterators.end());
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
