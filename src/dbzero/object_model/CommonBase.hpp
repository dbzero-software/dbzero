// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "ObjectBase.hpp"
#include <dbzero/object_model/value/StorageClass.hpp>
#include <dbzero/core/vspace/v_object.hpp>
#include <dbzero/core/compiler_attributes.hpp>

namespace db0::object_model

{

DB0_PACKED_BEGIN

    class DB0_PACKED_ATTR o_common_base: public db0::o_base<o_common_base, 0, true>
    {
    public:
        // common object header
        o_unique_header m_header;
    };
    
    // common base for ObjectBase derived classes
    class CommonBase: public ObjectBase<CommonBase, db0::v_object<o_common_base>, StorageClass::UNDEFINED>
    {
    public:
    };

DB0_PACKED_END

}