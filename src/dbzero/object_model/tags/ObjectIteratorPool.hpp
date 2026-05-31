// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 DBZero Software sp. z o.o.

#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>
#include <dbzero/bindings/python/PyWrapper.hpp>
#include <dbzero/bindings/python/shared_py_object.hpp>

namespace db0::object_model
{

    class ObjectIterator;
    using PyObjectIterator = db0::python::PySharedWrapper<ObjectIterator, false>;

    class ObjectIteratorPool
    {
    public:
        using ObjectSharedExtPtr = db0::python::shared_py_object<PyObjectIterator *, true>;

        void add(ObjectSharedExtPtr object);
        std::size_t detach();
        std::size_t detach(std::uint64_t generation);
        std::size_t cleanup();
        void close();

        std::size_t size() const;
        bool isClosed() const;

    private:
        std::vector<ObjectSharedExtPtr> m_iterators;
        std::uint64_t m_detach_generation = 0;
        bool m_closed = false;

        static ObjectIterator *getIterator(ObjectSharedExtPtr const &object);
    };

}
