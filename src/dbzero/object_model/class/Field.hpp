#pragma once

#include <dbzero/object_model/value/StorageClass.hpp>
#include <dbzero/core/vspace/db0_ptr.hpp>
#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/core/serialization/string.hpp>
#include <dbzero/core/collections/b_index/v_bindex.hpp>
#include <dbzero/core/collections/pools/StringPools.hpp>
#include <dbzero/core/collections/vector/v_bvector.hpp>
#include <dbzero/core/compiler_attributes.hpp>

namespace db0::object_model

{

DB0_PACKED_BEGIN
    
    class Class;
    using namespace db0;
    using namespace db0::pools;
    using ClassPtr = db0::db0_ptr<Class>;
    
    struct DB0_PACKED_ATTR o_field: public db0::o_fixed<o_field>
    {
        LP_String m_name;

        o_field(RC_LimitedStringPool &, const char *name);
    };
    
    using VFieldVector = db0::v_bvector<o_field>;
    
DB0_PACKED_END

}
