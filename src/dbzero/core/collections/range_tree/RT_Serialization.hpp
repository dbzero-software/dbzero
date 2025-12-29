// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "RT_SortIterator.hpp"
#include "IndexBase.hpp"
#include <dbzero/core/serialization/Serializable.hpp>
#include <dbzero/workspace/Snapshot.hpp>
#include <dbzero/core/collections/full_text/FT_Serialization.hpp>

namespace db0

{

    using TypeIdType = decltype(db0::serial::typeId<void>());

    template <typename KeyT> std::unique_ptr<db0::SortedIterator<KeyT> > deserializeSortedIterator(
        db0::Snapshot &, std::vector<std::byte>::const_iterator &iter,
        std::vector<std::byte>::const_iterator end);

    template <typename KeyT, typename ValueT> std::unique_ptr<RT_SortIterator<KeyT, ValueT> > 
    deserializeRT_SortIterator(Snapshot &, std::vector<std::byte>::const_iterator &iter, 
        std::vector<std::byte>::const_iterator end);

    template <typename KeyT, typename ValueT> std::unique_ptr<RT_SortIterator<KeyT, ValueT> >
    deserializeRT_SortIterator(Snapshot &snapshot, std::vector<std::byte>::const_iterator &iter,
        std::vector<std::byte>::const_iterator end)
    {
        using RT_TreeT = RangeTree<KeyT, ValueT>;
        if (db0::serial::read<TypeIdType>(iter, end) != db0::serial::typeId<KeyT>()) {
            THROWF(db0::InputException) << "Deserialize: invalid key type";
        }
        if (db0::serial::read<TypeIdType>(iter, end) != db0::serial::typeId<ValueT>()) {
            THROWF(db0::InputException) << "Deserialize: invalid value type";
        }
        
        auto fixture = snapshot.getFixture(db0::serial::read<std::uint64_t>(iter, end));
        auto addr = db0::serial::read<Address>(iter, end);
        bool asc = db0::serial::read<bool>(iter, end);
        bool null_first = db0::serial::read<bool>(iter, end);
        bool has_inner = db0::serial::read<bool>(iter, end);
        if (has_inner) {
            auto inner_it = deserializeSortedIterator<ValueT>(snapshot, iter, end);
            IndexBase index(fixture->myPtr(addr));
            return std::make_unique<RT_SortIterator<KeyT, ValueT>>(index, db0::tryGetRangeTree<RT_TreeT>(index),
                std::move(inner_it), asc, null_first);
        } else {
            bool has_query = db0::serial::read<bool>(iter, end);
            if (has_query) {
                auto query_it = deserializeFT_Iterator<ValueT>(snapshot, iter, end);
                IndexBase index(fixture->myPtr(addr));
                return std::make_unique<RT_SortIterator<KeyT, ValueT>>(index, db0::tryGetRangeTree<RT_TreeT>(index),
                    std::move(query_it), asc, null_first);
            } else {
                IndexBase index(fixture->myPtr(addr));
                return std::make_unique<RT_SortIterator<KeyT, ValueT>>(index, db0::tryGetRangeTree<RT_TreeT>(index), 
                    asc, null_first);
            }
        }
    }
    
    template <typename KeyT> std::unique_ptr<db0::SortedIterator<KeyT> > deserializeSortedIterator(
        db0::Snapshot &snapshot, std::vector<std::byte>::const_iterator &iter,
        std::vector<std::byte>::const_iterator end)
    {
        auto type_id = db0::serial::read<SortedIteratorType>(iter, end);
        if (type_id == SortedIteratorType::RT_Sort) {
            auto _iter = iter;
            auto key_type_id = db0::serial::read<TypeIdType>(_iter, end);
            auto value_type_id = db0::serial::read<TypeIdType>(_iter, end);
            if (value_type_id != db0::serial::typeId<KeyT>()) {
                THROWF(db0::InternalException) << "Key type mismatch: " << value_type_id << THROWF_END;
            }
            // key of the value sorted by
            if (key_type_id == db0::serial::typeId<std::uint64_t>()) {
                return deserializeRT_SortIterator<std::uint64_t, KeyT>(snapshot, iter, end);
            } else if (key_type_id == db0::serial::typeId<std::int64_t>()) {
                return deserializeRT_SortIterator<std::int64_t, KeyT>(snapshot, iter, end);
            } else {
                THROWF(db0::InternalException) << "RT_SortIterator unsupported key type ID: " << key_type_id << THROWF_END;
            }
        } else {
            THROWF(db0::InternalException) << "Unsupported SortedIterator type: " << static_cast<std::uint16_t>(type_id) 
                << THROWF_END;
        }
    }
    
}