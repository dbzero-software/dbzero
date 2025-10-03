#pragma once

#include <cstdint>
#include <cassert>
#include "FieldID.hpp"

namespace db0::object_model

{

    // MemberID combines primary field ID + secondary field ID
    // with different fidelities
    class MemberID
    {
    public:
        // field ID + fidelity
        std::pair<FieldID, unsigned int> m_primary;
        std::pair<FieldID, unsigned int> m_secondary;
        
        MemberID() = default;
        MemberID(FieldID field_id, unsigned int fidelity)  
            : m_primary{ field_id, fidelity }
        {
        }
        
        inline operator bool() const {
            // since primary is assigned first
            return m_primary.first;
        }
        
        // assign as primary or secondary
        void assign(FieldID field_id, unsigned int fidelity) 
        {
            if (!m_primary.first) {
                m_primary = { field_id, fidelity };
            } else {
                assert(!m_secondary.first);
                m_secondary = { field_id, fidelity };
            }
        }

        // get the member's primary index
        unsigned int getIndex() const {
            assert(m_primary.first);
            return m_primary.first.getIndex();            
        }
    };
    
}