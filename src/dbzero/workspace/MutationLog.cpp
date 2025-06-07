#include "MutationLog.hpp"
#include <cassert>
#include <iostream>

namespace db0

{

    MutationLog::MutationLog(int locked_sections) {
        init(locked_sections);
    }
    
    void MutationLog::init(int size)
    {
        if (size > 0) {
            m_mutation_flags.resize(size, -1);
        }
    }
    
    MutationLog &MutationLog::operator=(const MutationLog &&other)
    {
        m_mutation_flags = std::move(other.m_mutation_flags);
        m_all_mutation_flags_set = other.m_all_mutation_flags_set;
        return *this;
    }

    void MutationLog::onDirty()
    {
        if (!m_mutation_flags.empty() && !m_all_mutation_flags_set) {
            // set all flags to "true" where not released
            for (auto &flag: m_mutation_flags) {
                if (flag == 0) {
                    flag = 1;
                }
            }
            m_all_mutation_flags_set = true;
        }
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

        return result;
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

    MutationHandler MutationLog::getHandler()
    {
        return [this](MutationOp op, unsigned int int_value, std::function<void(unsigned int)> callback) -> bool 
        {
            switch (op) {
                case MutationOp::INIT:
                    this->init(int_value); // as size
                    break;
                case MutationOp::BEGIN_LOCKED:
                    this->beginLocked(int_value);
                    break;
                case MutationOp::END_LOCKED:
                    return this->endLocked(int_value);
                    break;
                case MutationOp::END_ALL_LOCKED:
                    this->endAllLocked(callback);
                    break;                    
            }
            return false;
        };
    }
    
}