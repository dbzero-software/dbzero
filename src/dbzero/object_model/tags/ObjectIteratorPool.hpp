// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <unordered_map>

namespace db0::object_model
{

    class ObjectIterator;

    class ObjectIteratorPool
    {
    public:
        using ObjectWeakPtr = std::weak_ptr<ObjectIterator>;

        void add(const std::shared_ptr<ObjectIterator> &object);
        void remove(ObjectIterator *object_ptr);
        std::size_t detach();
        std::size_t detach(std::uint64_t generation);
        std::size_t cleanup();
        void close();

        std::size_t size() const;
        bool isClosed() const;

    private:
        std::unordered_map<ObjectIterator *, ObjectWeakPtr> m_iterators;
        std::uint64_t m_detach_generation = 0;
        bool m_closed = false;

        static std::shared_ptr<ObjectIterator> getIterator(const ObjectWeakPtr &object);
    };

}
