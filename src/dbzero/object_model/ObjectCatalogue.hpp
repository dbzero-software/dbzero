// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <typeinfo>
#if defined(__GNUG__)
#include <cxxabi.h>
#include <cstdlib>
#include <memory>
#endif

#include <dbzero/core/serialization/string.hpp>
#include <dbzero/core/collections/map/v_map.hpp>
#include <dbzero/core/memory/Address.hpp>

namespace db0::object_model

{

    using Address = db0::Address;


    template <typename T>
    const std::string& get_type_name() {
        static const std::string type_name = []() {
            const char* name = typeid(T).name();

            #if defined(__GNUG__)
                int status = 0;
                std::unique_ptr<char, void(*)(void*)> res{
                    abi::__cxa_demangle(name, nullptr, nullptr, &status),
                    std::free
                };
                return std::string((status == 0) ? res.get() : name);
            #elif defined(_MSC_VER)
                // remove "class " or "struct " prefix
                if (strncmp(name, "class ", 6) == 0) {
                    name += 6;
                } else if (strncmp(name, "struct ", 7) == 0) {
                    name += 7;
                }
                return std::string(name); // already readable on MSVC
            #else
                return std::string(name); // fallback for other compilers
            #endif
        }();
        return type_name;
    }

    class ObjectCatalogue: public db0::v_map<o_string, o_simple<Address>, o_string::comp_t>
    {
    public:
        using super_t = db0::v_map<o_string, o_simple<Address>, o_string::comp_t>;
        using const_iterator = typename super_t::const_iterator;

        ObjectCatalogue(db0::Memspace &);
        ObjectCatalogue(db0::mptr);

        // Add objects by its type name (must not exist in the catalogue yet)
        template <typename T> void addUnique(T &object);

        // Find existing unique instance by name
        template <typename T> const_iterator findUnique() const;
    };

    template <typename T> void ObjectCatalogue::addUnique(T &object)
    {
        auto type_name = get_type_name<T>();
        auto it = this->find(type_name);
        if (it != this->end()) {
            THROWF(db0::InternalException) << "Object of type " << type_name << " already exists in the catalogue";
        }
        this->emplace(type_name, object.getAddress());
    }
    
    template <typename T> ObjectCatalogue::const_iterator ObjectCatalogue::findUnique() const
    {
        auto type_name = get_type_name<T>();
        auto result = this->find(type_name);
        if (result == this->end()) {
            THROWF(db0::InternalException) << "Object of type " << type_name << " not found in the catalogue";
        }
        return result;
    }

}