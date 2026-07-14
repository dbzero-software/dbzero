// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/core/vspace/db0_ptr.hpp>
#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/core/serialization/FixedVersioned.hpp>
#include <dbzero/core/serialization/string.hpp>
#include <dbzero/core/collections/b_index/v_bindex.hpp>
#include <dbzero/core/collections/pools/StringPools.hpp>
#include <dbzero/core/collections/vector/v_bvector.hpp>
#include <dbzero/core/compiler_attributes.hpp>
#include <dbzero/core/collections/vector/VLimitedMatrix.hpp>
#include <dbzero/object_model/value/StorageClass.hpp>
#include <dbzero/object_model/object/lofi_store.hpp>

namespace db0::object_model

{
    
    class Class;
    using namespace db0;
    using namespace db0::pools;
    using ClassPtr = db0::db0_ptr<Class>;

DB0_PACKED_BEGIN
    
    struct DB0_PACKED_ATTR o_field: public db0::o_fixed_versioned<o_field>
    {
        LP_String m_name;
        
        o_field() = default;
        o_field(RC_LimitedStringPool &, const char *name);
    };

DB0_PACKED_END

    // NOTE: we use lofi_store<2> since it's the lowest supported type fidelity
    using VFieldMatrix = db0::VLimitedMatrix<o_field, lofi_store<2>::size()>;

}
