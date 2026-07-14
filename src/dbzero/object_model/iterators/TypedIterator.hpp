// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

namespace db0::object_model

{

    /**
     * Iterator performing user defined type conversion (from language specific object)
     * conforms with the ForwardIterator concept
    */
    template <typename T, typename IteratorT> class TypedIterator
    {
    public:
        using CastFunction = std::function<T(typename IteratorT::ObjectSharedPtr)>;

        TypedIterator(CastFunction cast_func, IteratorT const &iter)
            : m_cast_func(cast_func)
            , m_iter(iter)
        {
        }
        
        bool operator!=(TypedIterator const &other) const
        {
            return m_iter != other.m_iter;
        }

        T operator*() const
        {
            return m_cast_func(*m_iter);
        }

        TypedIterator &operator++()
        {
            ++m_iter;
            return *this;
        }
        
        TypedIterator end() const
        {
            return { CastFunction(), m_iter.end() };
        }

    private:
        CastFunction m_cast_func;
        IteratorT m_iter;

        // as null / end
        TypedIterator() = default;
    };

}   
