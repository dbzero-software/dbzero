// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "FT_IteratorBase.hpp"
#include "FT_Iterator.hpp"
#include <dbzero/core/serialization/Serializable.hpp>

namespace db0

{
    
    enum class IteratorFactoryTypeId: std::uint16_t 
    {
        Invalid = 0,
        Range = 1
    };

    /**
     * The ItertorFactory interface combines FT_IteratorBase and FT_Iterator) properties
     * It can be used to construct either of the types depending on the usage context
    */
    template <typename KeyT> class IteratorFactory: public Serializable
    {
    public:
        virtual ~IteratorFactory() = default;

        virtual std::unique_ptr<FT_IteratorBase> createBaseIterator() = 0;

        virtual std::unique_ptr<FT_Iterator<KeyT> > createFTIterator() = 0;

        virtual IteratorFactoryTypeId getSerialTypeId() const = 0;

        virtual void serialize(std::vector<std::byte> &) const override;

    protected:
        virtual void serializeImpl(std::vector<std::byte> &) const = 0;
    };
    
    template <typename KeyT> void IteratorFactory<KeyT>::serialize(std::vector<std::byte> &data) const
    {
        db0::serial::write(data, getSerialTypeId());
        serializeImpl(data);
    }
    
}
