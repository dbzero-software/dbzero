// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <dbzero/core/serialization/Serializable.hpp>
#include <dbzero/core/collections/b_index/mb_index.hpp>

namespace tests

{
    
    using TypeOfType = decltype(db0::serial::typeId<void>());

    TEST( SerializationTest , testSerialWriteAndReadTypeIds )
    {
        std::vector<std::byte> buf;
        db0::serial::write(buf, db0::serial::typeId<int>());
        db0::serial::write(buf, db0::serial::typeId<std::string>());
        // complex type
        db0::serial::write(buf, db0::MorphingBIndex<std::uint64_t>::getSerialTypeId());

        auto iter = buf.cbegin(), end = buf.cend();
        ASSERT_EQ(db0::serial::read<TypeOfType>(iter, end), db0::serial::typeId<int>());
        ASSERT_EQ(db0::serial::read<TypeOfType>(iter, end), db0::serial::typeId<std::string>());
        ASSERT_EQ(db0::serial::read<TypeOfType>(iter, end), db0::MorphingBIndex<std::uint64_t>::getSerialTypeId());
        // reading past the end of the buffer should throw
        ASSERT_ANY_THROW(db0::serial::read<TypeOfType>(iter, end));
    }
    
}