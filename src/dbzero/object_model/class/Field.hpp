#pragma once

#include <dbzero/core/vspace/db0_ptr.hpp>
#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/core/serialization/string.hpp>
#include <dbzero/core/collections/b_index/v_bindex.hpp>
#include <dbzero/core/collections/pools/StringPools.hpp>
#include <dbzero/core/collections/vector/v_bvector.hpp>
#include <dbzero/core/collections/vector/VLimitedMatrix.hpp>
#include <dbzero/object_model/value/StorageClass.hpp>
#include <dbzero/object_model/object/lofi_store.hpp>

namespace db0::object_model

{
    
    class Class;
    using namespace db0;
    using namespace db0::pools;
    using ClassPtr = db0::db0_ptr<Class>;
    
    struct [[gnu::packed]] o_field: public db0::o_fixed<o_field>
    {
        LP_String m_name;
        
        o_field() = default;
        o_field(RC_LimitedStringPool &, const char *name);
    };
    
    using VFieldVector = db0::v_bvector<o_field>;
    // NOTE: we use lofi_store<2> since it's the lowest supported type fidelity
    // NOTE: +1 is required to account for regular fields (stored with offset = 0)    
    using VFieldMatrix = db0::VLimitedMatrix<o_field, lofi_store<2>::size() + 1>;
    
}
