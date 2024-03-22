#pragma once

#include <dbzero/object_model/value/StorageClass.hpp>
#include <dbzero/core/vspace/db0_ptr.hpp>
#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/core/serialization/string.hpp>
#include <dbzero/core/collections/b_index/v_bindex.hpp>
#include <dbzero/core/collections/pools/StringPools.hpp>
#include <dbzero/core/collections/vector/v_bvector.hpp>

namespace db0::object_model

{
    
    class Class;
    using namespace db0;
    using namespace db0::pools;
    using ClassPtr = db0::db0_ptr<Class>;

    struct [[gnu::packed]] o_field: public db0::o_fixed<o_field>
    {
        LP_String m_name;
        StorageClass m_storage_class;
        // pointer to Class associated with the StorageClass::OBJECT values
        ClassPtr m_class_ptr = {};
        
        o_field(RC_LimitedStringPool &, const char *name, StorageClass, ClassPtr = {});
    };
    
    using VFieldVector = db0::v_bvector<o_field>;
    
}
