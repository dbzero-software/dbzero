#pragma once

#include <typeinfo>
#include <dbzero/core/serialization/string.hpp>
#include <dbzero/core/collections/map/v_map.hpp>
#include <dbzero/core/memory/Address.hpp>

namespace db0::object_model

{

    using Address = db0::Address;
    
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
        auto type_name = typeid(T).name();
        auto it = this->find(type_name);
        if (it != this->end()) {
            THROWF(db0::InternalException) << "Object of type " << type_name << " already exists in the catalogue";
        }
        this->emplace(type_name, object.getAddress());
    }
    
    template <typename T> ObjectCatalogue::const_iterator ObjectCatalogue::findUnique() const
    {
        auto type_name = typeid(T).name();
        auto result = this->find(type_name);
        if (result == this->end()) {
            THROWF(db0::InternalException) << "Object of type " << type_name << " not found in the catalogue";
        }
        return result;
    }

}