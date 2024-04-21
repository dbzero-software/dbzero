#pragma once

#include <vector>
#include "FT_Iterator.hpp"
#include "SortedIterator.hpp"
#include "FT_IndexIterator.hpp"
#include <dbzero/workspace/Snapshot.hpp>
#include <dbzero/core/serialization/Serializable.hpp>
#include <dbzero/core/collections/b_index/mb_index.hpp>

namespace db0

{

    using TypeIdType = decltype(db0::serial::typeId<void>());

    template <typename KeyT> std::unique_ptr<db0::FT_Iterator<KeyT> > deserializeFT_Iterator(
        db0::Snapshot &, std::vector<std::byte>::const_iterator &iter,
        std::vector<std::byte>::const_iterator end);
    
    template <typename bindex_t, typename KeyT> std::unique_ptr<db0::FT_IndexIterator<bindex_t, KeyT> > deserializeFT_IndexIterator(
        db0::Snapshot &, std::vector<std::byte>::const_iterator &iter,
        std::vector<std::byte>::const_iterator end);
    
    template <typename KeyT> std::unique_ptr<db0::FT_Iterator<KeyT> > deserializeFT_Iterator(
        db0::Snapshot &snapshot, std::vector<std::byte>::const_iterator &iter,
        std::vector<std::byte>::const_iterator end)
    {
        auto type_id = db0::serial::read<FTIteratorType>(iter, end);
        if (type_id == FTIteratorType::Index) {
            // detect underlying index type (complex type)
            auto index_type_id = db0::serial::read<TypeIdType>(iter, end);
            if (index_type_id == db0::MorphingBIndex<std::uint64_t, std::uint64_t>::getSerialTypeId()) {
                throw std::runtime_error("Not implemented");
            } else {
                THROWF(db0::InternalException) << "Unsupported index type ID: " << index_type_id
                    << THROWF_END;
            }
        } else {
            THROWF(db0::InternalException) << "Unsupported FT_Iterator type: " << static_cast<std::uint16_t>(type_id) 
                << THROWF_END;
        }
    }
    
}
