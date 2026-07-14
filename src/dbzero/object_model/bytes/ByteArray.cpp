// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "ByteArray.hpp"
#include <dbzero/object_model/value/TypeUtils.hpp>
#include <dbzero/object_model/value/Member.hpp>

namespace db0::object_model

{

    GC0_Define(ByteArray)
    
    ByteArray::ByteArray(db0::swine_ptr<Fixture> &fixture, std::byte *bytes, std::size_t size, AccessFlags access_mode)
        : super_t(fixture, access_mode)
    {
        for (auto *bytes_ptr = bytes, *end = bytes + size; bytes_ptr != end; ++bytes_ptr) {            
            v_bvector::push_back(*bytes_ptr);
        }
    }
    
    ByteArray::ByteArray(db0::swine_ptr<Fixture> &fixture, Address address, AccessFlags access_mode)
        : super_t(super_t::tag_from_address(), fixture, address, access_mode)
    {
    }
    
    ByteArray::ByteArray(tag_no_gc, db0::swine_ptr<Fixture> &fixture, const ByteArray &byte_array)
        : super_t(tag_no_gc(), fixture, byte_array)
    {
    }    
    
    ByteArray::~ByteArray()
    {
        // unregister needs to be called before destruction of members
        unregister();
    }

    ByteArray::ObjectSharedPtr ByteArray::getItem(std::size_t i) const
    {
        if (i >= size()) {
            THROWF(db0::InputException) << "Index out of range: " << i;
        }
        auto fixture = this->getFixture();
        return unloadMember<LangToolkit>(
            fixture, StorageClass::INT64, (long)(*this)[i], 0, this->getMemberFlags()
        );
    }
    
    std::byte ByteArray::getByte(std::size_t i) const {
        return (*this)[i];        
    }

    void ByteArray::setItem(FixtureLock &fixture, std::size_t i, ObjectPtr lang_value)
    {
        if (i >= size()) {
            THROWF(db0::InputException) << "Index out of range: " << i;
        }
        auto &type_manager = LangToolkit::getTypeManager();
        v_bvector::setItem(i, std::byte(type_manager.extractInt64(lang_value)));
    }

    void ByteArray::append(FixtureLock &, ObjectSharedPtr lang_value)
    {
        using TypeId = db0::bindings::TypeId;
        auto &type_manager = LangToolkit::getTypeManager();
        v_bvector::push_back(std::byte(type_manager.extractInt64(*lang_value)));
    }
    
    std::size_t ByteArray::count(std::byte value) const
    {
        std::size_t count = 0;
        for (auto &elem: *this) {
            if (elem == value) {
                count += 1;
            }
        }
        return count;
    }

    std::size_t ByteArray::count(const std::byte *values, std::size_t values_size) const
    {
        // substring search using Knuth-Morris-Pratt algorithm
        int *P = new int[values_size + 1];
        if (values_size == 0) {
            return size() +1;
        }
        if (values_size == 1) {
            return count(values[0]);
        }
        std::size_t t = 0;
        for (std::size_t j=2; j<=values_size; j++)
        {
            while ((t>0) && (values[t] != values[j-1])) {
                t=P[t];
            }
            if (values[t]==values[j-1]){
                t++;
            }
            P[j]=t;
        }
        std::size_t count = 0;
        std::size_t i = 0;
        std::size_t j = 0;
        while (i < size()) {
            if (values[j] == (*this)[i]) {
                i++;
                j++;
                if (j == values_size) {
                    ++count;
                    j = P[j];
                }
            } else if (j == 0) {
                i++;
            } else {
                j = P[j];
            }
        }
        delete[] P;
        return count;
    }

    std::size_t ByteArray::count(const ByteArray& value, std::size_t size) const
    {
        std::byte *bytes = new std::byte[size];
        for (std::size_t i = 0; i < size; i++) {
            bytes[i] = value[i];
        }
        std::size_t result = count(bytes, size);
        delete[] bytes;
        return result;
    }

    bool ByteArray::operator==(const ByteArray &bytearray) const
    {
        if (size() != bytearray.size()) {
            return false;
        }
        return std::equal(begin(), end(), bytearray.begin());
    }

    bool ByteArray::operator!=(const ByteArray &bytearray) const
    {
        if (size() != bytearray.size()) {
            return false;
        }
        return !(*this == bytearray);
    }
    
}