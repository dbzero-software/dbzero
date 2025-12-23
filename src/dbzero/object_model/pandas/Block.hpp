#pragma once

#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/core/collections/vector/v_bvector.hpp>
#include <dbzero/object_model/value/StorageClass.hpp>
#include <dbzero/object_model/value/Value.hpp>
#include <dbzero/object_model/list/List.hpp>
#include <dbzero/object_model/ObjectBase.hpp>

namespace db0 {

    class Fixture;

}

namespace db0::object_model::pandas

{

    using Fixture = db0::Fixture;

    class Block: public db0::ObjectBase<Block, db0::v_bvector<Value>, StorageClass::DB0_BLOCK>
    {    
        GC0_Declare
    public:
        using super_t = db0::ObjectBase<Block, db0::v_bvector<Value>, StorageClass::DB0_BLOCK>;
        using LangToolkit = db0::python::PyToolkit;
        using ObjectPtr = typename LangToolkit::ObjectPtr;
        using ObjectSharedPtr = typename LangToolkit::ObjectSharedPtr;
        
        // as null placeholder
        Block() = default;
        Block(db0::swine_ptr<Fixture> &);
        Block(db0::swine_ptr<Fixture> &, Address address, AccessFlags = {});
        ~Block();
                
        ObjectSharedPtr getItem(std::size_t i) const;
        void setItem(FixtureLock &, std::size_t i, ObjectPtr lang_value);

        static Block *makeNew(void *at_ptr, db0::swine_ptr<Fixture> &);
        void append(FixtureLock &, ObjectPtr lang_value);
        
        ObjectSharedPtr getStorageClass() const;

        void moveTo(db0::swine_ptr<Fixture> &);

    private:        
        db0::object_model::StorageClass m_storage_class;
    };
    
}