#pragma once

#include <vector>
#include "FT_Iterator.hpp"
#include "SortedIterator.hpp"
#include "FT_IndexIterator.hpp"
#include "FT_BaseIndex.hpp"
#include "IteratorFactory.hpp"
#include "FT_ANDIterator.hpp"
#include "FT_ORXIterator.hpp"
#include <dbzero/workspace/Snapshot.hpp>
#include <dbzero/core/serialization/Serializable.hpp>
#include <dbzero/core/collections/b_index/mb_index.hpp>
#include <dbzero/core/collections/range_tree/RangeIteratorFactory.hpp>

namespace db0

{

    using TypeIdType = decltype(db0::serial::typeId<void>());

    template <typename KeyT> std::unique_ptr<db0::FT_Iterator<KeyT> > deserializeFT_Iterator(
        db0::Snapshot &, std::vector<std::byte>::const_iterator &iter,
        std::vector<std::byte>::const_iterator end);

    template <typename bindex_t, typename KeyT> std::unique_ptr<db0::FT_Iterator<KeyT> > deserializeFT_IndexIterator(
        db0::Snapshot &, std::vector<std::byte>::const_iterator &iter,
        std::vector<std::byte>::const_iterator end);
    
    template <typename KeyT> std::unique_ptr<db0::FT_Iterator<KeyT> > deserializeFT_Iterator(
        db0::Snapshot &workspace, std::vector<std::byte>::const_iterator &iter,
        std::vector<std::byte>::const_iterator end)
    {
        auto type_id = db0::serial::read<FTIteratorType>(iter, end);
        if (type_id == FTIteratorType::Index) {
            // detect underlying index type (complex type)
            auto _iter = iter;
            auto index_type_id = db0::serial::read<TypeIdType>(_iter, end);
            if (index_type_id == db0::MorphingBIndex<std::uint64_t, std::uint64_t>::getSerialTypeId()) {
                return deserializeFT_IndexIterator<db0::MorphingBIndex<std::uint64_t, std::uint64_t>, KeyT>(workspace, iter, end);
            } else {
                THROWF(db0::InternalException) << "Unsupported index type ID: " << index_type_id
                    << THROWF_END;
            }
        } else if (type_id == FTIteratorType::JoinAnd) {
            auto _iter = iter;
            auto key_type_id = db0::serial::read<TypeIdType>(_iter, end);
            if (key_type_id == db0::serial::typeId<std::uint64_t>()) {
                return db0::FT_JoinANDIterator<std::uint64_t>::deserialize(workspace, iter, end);
            } else {
                THROWF(db0::InternalException) << "Unsupported key type ID: " << key_type_id
                    << THROWF_END;
            }
        } else if (type_id == FTIteratorType::JoinOr) {
            auto _iter = iter;
            auto key_type_id = db0::serial::read<TypeIdType>(_iter, end);
            if (key_type_id == db0::serial::typeId<std::uint64_t>()) {
                return db0::FT_JoinORXIterator<std::uint64_t>::deserialize(workspace, iter, end);
            } else {
                THROWF(db0::InternalException) << "Unsupported key type ID: " << key_type_id
                    << THROWF_END;
            }
        } else {
            THROWF(db0::InternalException) << "Unsupported FT_Iterator type: " << static_cast<std::uint16_t>(type_id) 
                << THROWF_END;
        }
    }
    
    template <typename bindex_t, typename KeyT> std::unique_ptr<db0::FT_Iterator<KeyT> > deserializeFT_IndexIterator(
        db0::Snapshot &snapshot, std::vector<std::byte>::const_iterator &iter,
        std::vector<std::byte>::const_iterator end)
    {
        auto index_type_id = db0::serial::read<TypeIdType>(iter, end);
        if (index_type_id != db0::serial::typeId<bindex_t>()) {
            THROWF(db0::InternalException) << "Index type mismatch: " << index_type_id << " != " << bindex_t::getSerialTypeId()
                << THROWF_END;
        }

        auto key_type_id = db0::serial::read<TypeIdType>(iter, end);
        if (key_type_id != db0::serial::typeId<KeyT>()) {
            THROWF(db0::InternalException) << "Key type mismatch: " << key_type_id << " != " << db0::serial::typeId<KeyT>()
                << THROWF_END;
        }

        // get fixture by UUID
        auto fixture = snapshot.getFixture(db0::serial::read<std::uint64_t>(iter, end));        
        int direction = db0::serial::read<std::int8_t>(iter, end);
        auto index_key = db0::serial::read<std::uint64_t>(iter, end);        
        // use FT_Base index as the factory
        return fixture->get<db0::FT_BaseIndex>().makeIterator(index_key, direction);
    }
    
    template <typename KeyT> std::unique_ptr<db0::IteratorFactory<KeyT> > deserializeIteratorFactory(
        db0::Snapshot &, std::vector<std::byte>::const_iterator &iter,
        std::vector<std::byte>::const_iterator end);
    
    template <typename KeyT> std::unique_ptr<db0::IteratorFactory<KeyT> > deserializeIteratorFactory(
        db0::Snapshot &workspace, std::vector<std::byte>::const_iterator &iter,
        std::vector<std::byte>::const_iterator end)
    {
        auto type_id = db0::serial::read<IteratorFactoryTypeId>(iter, end);
        if (type_id == IteratorFactoryTypeId::Range) {
            auto iter_ = iter;
            auto key_type_id = db0::serial::read<TypeIdType>(iter_, end);
            db0::serial::read<TypeIdType>(iter_, end); // value type ID
            if (key_type_id == db0::serial::typeId<std::uint64_t>()) {
                return deserializeRangeIteratorFactory<std::uint64_t, KeyT>(workspace, iter, end);
            } else if (key_type_id == db0::serial::typeId<std::int64_t>()) {
                return deserializeRangeIteratorFactory<std::int64_t, KeyT>(workspace, iter, end);
            } else {
                THROWF(db0::InternalException) << "Unsupported key type ID: " << key_type_id
                    << THROWF_END;
            }            
        }
        THROWF(db0::InternalException) << "Unsupported IteratorFactory type: " << static_cast<std::uint16_t>(type_id) 
            << THROWF_END;        
    }

}
