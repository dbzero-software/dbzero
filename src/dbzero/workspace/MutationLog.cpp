// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "MutationLog.hpp"
#include <cassert>
#include <iostream>

namespace db0

{

    MutationLog::MutationLog(int locked_sections)
    {
        // NOTE: we initialize with 0 assuming all locked sections are active
        if (locked_sections > 0) {
            m_mutation_flags.resize(locked_sections, 0);
        }        
    }
    
    MutationLog::MutationLog(const MutationLog &other)
        : m_mutation_flags(other.m_mutation_flags)
        , m_all_mutation_flags_set(other.m_all_mutation_flags_set)
    {    
    }
    
    MutationLog &MutationLog::operator=(const MutationLog &&other)
    {
        m_mutation_flags = std::move(other.m_mutation_flags);
        m_all_mutation_flags_set = other.m_all_mutation_flags_set;
        return *this;
    }
    
    void MutationLog::onDirty()
    {
        if (!m_all_mutation_flags_set && !m_mutation_flags.empty()) {
            // set all flags to "true" where not released
            for (auto &flag: m_mutation_flags) {
                if (flag == 0) {
                    flag = 1;
                }
            }
            m_all_mutation_flags_set = true;
        }
    }
    
    void MutationLog::add(const MutationLog &other)
    {
        if (other.size() > m_mutation_flags.size()) {
            m_mutation_flags.resize(other.m_mutation_flags.size(), -1);
        }

        unsigned int index = 0;
        for (auto flag: other.m_mutation_flags) {
            if (m_mutation_flags[index] < flag) {
                m_mutation_flags[index] = flag;
            }
            ++index;
        }
        m_all_mutation_flags_set = false;
    }
    
    void MutationLog::beginLocked(unsigned int locked_section_id)
    {
        if (locked_section_id >= m_mutation_flags.size()) {
            m_mutation_flags.resize(locked_section_id + 1, -1);
        }
        m_mutation_flags[locked_section_id] = 0;
        m_all_mutation_flags_set = false;
    }
    
    bool MutationLog::endLocked(unsigned int locked_section_id)
    {
        assert(locked_section_id < m_mutation_flags.size());
        auto result = m_mutation_flags[locked_section_id];
        m_mutation_flags[locked_section_id] = -1;
        // clean-up released slots
        while (!m_mutation_flags.empty() && m_mutation_flags.back() == -1) {
            m_mutation_flags.pop_back();
        }
        
        return result == 1;
    }
    
    void MutationLog::endAllLocked(std::function<void(unsigned int)> callback)
    {
        for (unsigned int i = 0; i < m_mutation_flags.size(); ++i) {
            if (m_mutation_flags[i] == 1) {
                callback(i);
            }
        }
        m_mutation_flags.clear();
        m_all_mutation_flags_set = false;
    }

    std::size_t MutationLog::size() const {
        return m_mutation_flags.size();
    }
    
}