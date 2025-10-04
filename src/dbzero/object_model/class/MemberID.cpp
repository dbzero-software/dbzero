#include "MemberID.hpp"
#include <stdexcept>

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

    void MemberID::assign(FieldID field_id, unsigned int fidelity)
    {
        if (!m_primary.first) {
            m_primary = { field_id, fidelity };
        } else {
            assert(!m_secondary.first);
            m_secondary = { field_id, fidelity };
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