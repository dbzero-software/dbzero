// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "FT_IteratorBase.hpp"
#include "FT_Iterator.hpp"
#include <dbzero/core/serialization/Serializable.hpp>

namespace db0

{

    // Sorted iterator implementations should register here (for serialization)
    enum class SortedIteratorType: std::uint16_t
    {
        Invalid = 0,
        RT_Sort = 1
    };

    /**
     * A base for sorted full-text iterators
    */
    template <typename ValueT> class SortedIterator: public FT_IteratorBase, public Serializable
    {
    public:
        using QueryIterator = FT_Iterator<ValueT>;

        /**
         * Check if the underlying full-text query has been defined
        */
        virtual bool hasFTQuery() const = 0;

        /**
         * Retrieve the underlying full-text query (unsorted)
        */
        virtual std::unique_ptr<QueryIterator> beginFTQuery() const = 0;

        /**
         * Clone the iterator for starting over preserving the sorting order
         * or sort a specific inner query (ft_query) if provided       
        */
        virtual std::unique_ptr<SortedIterator<ValueT> > beginSorted(std::unique_ptr<QueryIterator> ft_query = nullptr) const = 0;

        virtual SortedIteratorType getSerialTypeId() const = 0;

        virtual void serialize(std::vector<std::byte> &) const;

    protected:
        virtual void serializeImpl(std::vector<std::byte> &) const = 0;

        SortedIterator(std::uint64_t uid);
    };

    template <typename ValueT> void SortedIterator<ValueT>::serialize(std::vector<std::byte> &v) const
    {
        db0::serial::write(v, this->getSerialTypeId());
        this->serializeImpl(v);
    }

    template <typename ValueT> SortedIterator<ValueT>::SortedIterator(std::uint64_t uid)
        : FT_IteratorBase(uid)
    {
    }
    
}
