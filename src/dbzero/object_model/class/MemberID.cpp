// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "MemberID.hpp"
#include <stdexcept>
#include <iostream>

namespace db0::object_model

{

    const FieldID &MemberID::get(unsigned int fidelity) const
    {
        if (m_primary.second == fidelity) {
            return m_primary.first;
        } else if (m_secondary.first && m_secondary.second == fidelity) {
            return m_secondary.first;
        } else {
            assert(false);
            throw std::runtime_error("MemberID::get: fidelity not found");
        }
    }

    FieldID MemberID::tryGet(unsigned int fidelity) const 
    {
        if (m_primary.second == fidelity) {
            return m_primary.first;
        } else if (m_secondary.first && m_secondary.second == fidelity) {
            return m_secondary.first;
        } else {
            // not found
            return {};
        }
    }
    
    void MemberID::assign(FieldID field_id, unsigned int fidelity)
    {
        if (!m_primary.first) {
            m_primary = { field_id, fidelity };
        } else {
            assert(m_primary.first);
            assert(!m_secondary.first);
            assert(m_primary.second != fidelity && "Fidelity already assigned as primary!");
            m_secondary = { field_id, fidelity };
            if (m_secondary.second > m_primary.second) {
                // ensure primary has the higher fidelity number
                // this is to allow a deterministic unique identifier
                std::swap(m_primary, m_secondary);
            }
        }
    }
    
    MemberID::const_iterator::const_iterator(const MemberID &member_id)
        : m_first_ptr(&member_id.m_primary)
        , m_second_ptr(member_id.m_secondary.first ? &member_id.m_secondary : nullptr)
        , m_current_ptr(m_first_ptr)
    {
    }
    
    MemberID::const_iterator &MemberID::const_iterator::operator++()
    {
        if (m_current_ptr == m_first_ptr) {
            m_current_ptr = m_second_ptr;
        } else {
            m_current_ptr = nullptr;
        }
        return *this;
    }

}

namespace std

{

    std::ostream &operator<<(std::ostream &os, const db0::object_model::MemberID &member_id)
    {
        os << "MemberID(";
        bool first = true;
        for (auto &field_info: member_id) {
            if (!first) {
                os << ", ";
            }
            os << "{ID: " << field_info.first << ", Fidelity: " << field_info.second << "}";
            first = false;
        }
        os << ")";
        return os;
    }
    
}