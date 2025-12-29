// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdint>
#include <cassert>
#include <limits>
#include "FieldID.hpp"

namespace db0::object_model

{

    // MemberID combines primary field ID + secondary field ID
    // with different fidelities
    class MemberID
    {
    public:
        MemberID() = default;
        MemberID(FieldID field_id, unsigned int fidelity)  
            : m_primary{ field_id, fidelity }
        {
        }
        
        inline operator bool() const {
            // since primary is assigned first
            return m_primary.first;
        }
        
        const std::pair<FieldID, unsigned int> &primary() const {
            return m_primary;
        }

        const std::pair<FieldID, unsigned int> &secondary() const {
            return m_secondary;
        }
        
        std::size_t size() const {
            return (m_secondary.first ? 2: (m_primary.first ? 1 : 0));
        }

        // Get the member's ID at the specified fidelity
        // if not found, an exception is thrown
        const FieldID &get(unsigned int fidelity = 0) const;
        FieldID tryGet(unsigned int fidelity = 0) const;
        
        // assign as primary or secondary
        void assign(FieldID field_id, unsigned int fidelity);
        
        bool hasFidelity(unsigned int fidelity) const {
            return (m_primary.second == fidelity) || (m_secondary.second == fidelity);
        }

        // Iterator over the contained FieldIDs
        struct const_iterator
        {
            const_iterator() = default;
            const_iterator(const MemberID &);

            const std::pair<FieldID, unsigned int> &operator*() const {
                assert(m_current_ptr);
                return *m_current_ptr;
            }

            const_iterator &operator++();

            bool operator!=(const const_iterator &other) const {
                return m_current_ptr != other.m_current_ptr;
            }
            
        private:
            const std::pair<FieldID, unsigned int> *m_first_ptr = nullptr;
            const std::pair<FieldID, unsigned int> *m_second_ptr = nullptr;
            const std::pair<FieldID, unsigned int> *m_current_ptr = nullptr;
        };
        
        const_iterator begin() const {
            return const_iterator(*this);
        }
        
        const_iterator end() const {
            return const_iterator();
        }
        
    private:
        // Field ID + fidelity
        std::pair<FieldID, unsigned int> m_primary = { FieldID(), std::numeric_limits<unsigned int>::max() };
        std::pair<FieldID, unsigned int> m_secondary = { FieldID(), std::numeric_limits<unsigned int>::max() };
    };
    
}

namespace std

{

    ostream &operator<<(std::ostream &os, const db0::object_model::MemberID &member_id);
    
}