// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <vector>
#include "FT_Iterator.hpp"
#include "SortedIterator.hpp"
#include "FT_IndexIterator.hpp"
#include "FT_BaseIndex.hpp"
#include "IteratorFactory.hpp"
#include "FT_ANDIterator.hpp"
#include "FT_ORXIterator.hpp"
#include "FT_ANDNOTIterator.hpp"
#include <dbzero/workspace/Snapshot.hpp>
#include <dbzero/core/serialization/Serializable.hpp>
#include <dbzero/core/collections/b_index/mb_index.hpp>
#include <dbzero/core/collections/range_tree/RangeIteratorFactory.hpp>
#include <dbzero/object_model/tags/TagIndex.hpp>

namespace db0

{

    using TypeIdType = decltype(db0::serial::typeId<void>());

    template <typename KeyT, typename KeyStorageT = KeyT> 
    std::unique_ptr<db0::FT_Iterator<KeyT, KeyStorageT> > deserializeFT_Iterator(
        db0::Snapshot &, std::vector<std::byte>::const_iterator &iter,
        std::vector<std::byte>::const_iterator end
    );

    template <typename bindex_t, typename KeyT> std::unique_ptr<db0::FT_Iterator<KeyT> > deserializeFT_IndexIterator(
        db0::Snapshot &, std::vector<std::byte>::const_iterator &iter,
        std::vector<std::byte>::const_iterator end);
    
    template <typename KeyT, typename KeyStorageT> 
    std::unique_ptr<db0::FT_Iterator<KeyT, KeyStorageT> > deserializeFT_Iterator(
        db0::Snapshot &workspace, std::vector<std::byte>::const_iterator &iter,
        std::vector<std::byte>::const_iterator end)
    {
        auto type_id = db0::serial::read<FTIteratorType>(iter, end);
        if (type_id == FTIteratorType::Index) {
            // detect underlying index type (complex type)
            auto _iter = iter;
            auto index_type_id = db0::serial::read<TypeIdType>(_iter, end);
            if (index_type_id == db0::MorphingBIndex<UniqueAddress>::getSerialTypeId()) {
                auto key_type_id = db0::serial::read<TypeIdType>(_iter, end);
                auto index_key_type_id = db0::serial::read<TypeIdType>(_iter, end);
                if (key_type_id == db0::serial::typeId<std::uint64_t>()) {
                    if constexpr (std::is_same_v<KeyT, std::uint64_t>) {
                        if (index_key_type_id == db0::serial::typeId<std::uint64_t>()) {
                            return deserializeFT_IndexIterator<db0::MorphingBIndex<UniqueAddress>, KeyT, std::uint64_t>(workspace, iter, end);
                        } else {
                            THROWF(db0::InternalException) << "Unsupported index key type ID: " << index_key_type_id
                                << THROWF_END;
                        }
                    }
                }
                
                if (key_type_id == db0::serial::typeId<UniqueAddress>()) {
                    if constexpr (std::is_same_v<KeyT, UniqueAddress>) {
                        if (index_key_type_id == db0::serial::typeId<std::uint64_t>()) {
                            return deserializeFT_IndexIterator<db0::MorphingBIndex<UniqueAddress>, KeyT, std::uint64_t>(workspace, iter, end);
                        } else {
                            THROWF(db0::InternalException) << "Unsupported index key type ID: " << index_key_type_id
                                << THROWF_END;
                        }
                    }
                }
                
                THROWF(db0::InternalException) << "Unsupported key type ID: " << key_type_id << THROWF_END;                
            }
            THROWF(db0::InternalException) << "Unsupported index type ID: " << index_type_id << THROWF_END;
        } else if (type_id == FTIteratorType::JoinAnd) {
            auto _iter = iter;
            auto key_type_id = db0::serial::read<TypeIdType>(_iter, end);
            if (key_type_id == db0::serial::typeId<UniqueAddress>()) {
                if constexpr (std::is_same_v<KeyT, UniqueAddress>) {
                    return db0::FT_JoinANDIterator<UniqueAddress>::deserialize(workspace, iter, end);
                }
            } 
            THROWF(db0::InternalException) << "Unsupported key type ID: " << key_type_id << THROWF_END;
        } else if (type_id == FTIteratorType::JoinOr) {
            auto _iter = iter;
            auto key_type_id = db0::serial::read<TypeIdType>(_iter, end);
            if (key_type_id == db0::serial::typeId<UniqueAddress>()) {
                if constexpr (std::is_same_v<KeyT, UniqueAddress>) {
                    return db0::FT_JoinORXIterator<UniqueAddress>::deserialize(workspace, iter, end);
                }                
            }
            THROWF(db0::InternalException) << "Unsupported key type ID: " << key_type_id << THROWF_END;            
        } else if (type_id == FTIteratorType::JoinAndNot) {
            auto _iter = iter;
            auto key_type_id = db0::serial::read<TypeIdType>(_iter, end);
            if (key_type_id == db0::serial::typeId<UniqueAddress>()) {
                if constexpr (std::is_same_v<KeyT, UniqueAddress>) {
                    return db0::FT_ANDNOTIterator<UniqueAddress>::deserialize(workspace, iter, end);
                }                
            }
            THROWF(db0::InternalException) << "Unsupported key type ID: " << key_type_id << THROWF_END;            
        } else {
            THROWF(db0::InternalException) << "Unsupported FT_Iterator type: " << static_cast<std::uint16_t>(type_id) 
                << THROWF_END;
        }
    }
    
    template <typename bindex_t, typename KeyT, typename IndexKeyT>
    std::unique_ptr<db0::FT_Iterator<KeyT> > deserializeFT_IndexIterator(
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
        
        auto index_key_type_id = db0::serial::read<TypeIdType>(iter, end);
        if (index_key_type_id != db0::serial::typeId<IndexKeyT>()) {
            THROWF(db0::InternalException) << "Index key type mismatch: " << index_key_type_id << " != " << db0::serial::typeId<IndexKeyT>()
                << THROWF_END;
        }
        
        // get fixture by UUID
        auto fixture = snapshot.getFixture(db0::serial::read<std::uint64_t>(iter, end));        
        int direction = db0::serial::read<std::int8_t>(iter, end);
        if (index_key_type_id == db0::serial::typeId<std::uint64_t>()) {
            auto index_key = db0::serial::read<std::uint64_t>(iter, end);
            // use FT_Base index as the factory
            // NOTE: TagIndex only supports UniqueAddress key type
            if constexpr (std::is_same_v<KeyT, UniqueAddress>) {
                auto &tag_index = fixture->get<db0::object_model::TagIndex>();
                return tag_index.getBaseIndexShort().makeIterator(index_key, direction);
            }
        }
        THROWF(db0::InternalException) << "Unsupported key type ID: " << key_type_id << THROWF_END;        
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
