// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "FieldSafe.hpp"

namespace db0::object_model

{

    FieldSafe::FieldSafe(db0::Memspace &memspace, db0::VObjectCache &cache)
        : super_t(memspace)
        , m_field_id_mapper(memspace)
        , m_field_mask_manager(memspace, cache)
    {
        auto &self = this->modify();
        self.m_field_id_mapper_ptr = m_field_id_mapper;
        self.m_field_mask_manager_ptr = m_field_mask_manager;
    }

    FieldSafe::FieldSafe(db0::mptr ptr, db0::VObjectCache &cache)
        : super_t(ptr)
        , m_field_id_mapper((*this)->m_field_id_mapper_ptr(this->getMemspace()))
        , m_field_mask_manager(this->getMemspace().myPtr((*this)->m_field_mask_manager_ptr.getAddress()), cache)
    {
    }

    FieldIDMapper &FieldSafe::getFieldIDMapper()
    {
        return m_field_id_mapper;
    }

    const FieldIDMapper &FieldSafe::getFieldIDMapper() const
    {
        return m_field_id_mapper;
    }

    FieldMaskManager &FieldSafe::getFieldMaskManager()
    {
        return m_field_mask_manager;
    }

    const FieldMaskManager &FieldSafe::getFieldMaskManager() const
    {
        return m_field_mask_manager;
    }

    void FieldSafe::detach() const
    {
        m_field_id_mapper.detach();
        m_field_mask_manager.detach();
        super_t::detach();
    }

    void FieldSafe::commit() const
    {
        m_field_id_mapper.commit();
        m_field_mask_manager.commit();
        super_t::commit();
    }

}
