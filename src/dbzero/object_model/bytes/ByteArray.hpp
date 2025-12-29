// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once
#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/core/collections/vector/v_bvector.hpp>
#include <dbzero/object_model/value/StorageClass.hpp>
#include <dbzero/object_model/ObjectBase.hpp>

namespace db0
{

    class Fixture;

}

namespace db0::object_model

{

    using Fixture = db0::Fixture;
    
    class ByteArray: public db0::ObjectBase<ByteArray, v_bvector<std::byte>, StorageClass::DB0_BYTES_ARRAY>
    {
        GC0_Declare
    public:
        using super_t = db0::ObjectBase<ByteArray, v_bvector<std::byte>, StorageClass::DB0_BYTES_ARRAY>;        
        using LangToolkit = db0::python::PyToolkit;
        using ObjectPtr = typename LangToolkit::ObjectPtr;
        using ObjectSharedPtr = typename LangToolkit::ObjectSharedPtr;
        friend super_t;
        
        // as null placeholder
        ByteArray() = default;
        ByteArray(db0::swine_ptr<Fixture> &, std::byte *, std::size_t, AccessFlags = {});
        ByteArray(tag_no_gc, db0::swine_ptr<Fixture> &, const ByteArray &);
        ByteArray(db0::swine_ptr<Fixture> &, Address, AccessFlags = {});
        ~ByteArray();
        
        ObjectSharedPtr getItem(std::size_t i) const;
        std::byte getByte(std::size_t i) const;
        void setItem(FixtureLock &fixture, std::size_t i, ObjectPtr lang_value);
        void append(FixtureLock &, ObjectSharedPtr lang_value);

        std::size_t count(std::byte value) const;
        std::size_t count(const std::byte *value, std::size_t size) const;
        std::size_t count(const ByteArray& value, std::size_t size) const;

        // operators
        bool operator==(const ByteArray &) const;
        bool operator!=(const ByteArray &) const;                
    };
    
}