// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "FieldIDMapper.hpp"
#include "FieldMask.hpp"
#include <dbzero/core/serialization/FixedVersioned.hpp>
#include <dbzero/core/vspace/db0_ptr.hpp>
#include <dbzero/core/vspace/v_object.hpp>

namespace db0::object_model

{

DB0_PACKED_BEGIN
    struct DB0_PACKED_ATTR o_field_safe: public db0::o_fixed_versioned<o_field_safe>
    {
        db0_ptr<FieldIDMapper> m_field_id_mapper_ptr;
        db0_ptr<FieldMaskManager> m_field_mask_manager_ptr;

        o_field_safe() = default;
    };
DB0_PACKED_END

    class FieldSafe: public db0::v_object<o_field_safe>
    {
    public:
        using super_t = db0::v_object<o_field_safe>;

        FieldSafe(db0::Memspace &, db0::VObjectCache &);
        FieldSafe(db0::mptr, db0::VObjectCache &);

        FieldIDMapper &getFieldIDMapper();
        const FieldIDMapper &getFieldIDMapper() const;
        FieldMaskManager &getFieldMaskManager();
        const FieldMaskManager &getFieldMaskManager() const;

        void detach() const;
        void commit() const;

    private:
        mutable FieldIDMapper m_field_id_mapper;
        mutable FieldMaskManager m_field_mask_manager;
    };

}
