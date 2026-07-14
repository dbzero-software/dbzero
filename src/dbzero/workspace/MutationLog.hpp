// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <vector>
#include <functional>

namespace db0

{
    
    // Mutation log tracks changes made to prefixes during a locked section.
    class MutationLog
    {
    public:
        // @param locked_sections the number of active locked sections
        MutationLog(int locked_sections = 0);
        MutationLog(const MutationLog &);
                
        MutationLog &operator=(const MutationLog &&);

        // collect prefix-level mutation flags (for locked sections)
        void onDirty();

        void beginLocked(unsigned int locked_section_id);
        bool endLocked(unsigned int locked_section_id);
        // ends all locked sections, invokes callback for all mutated ones
        void endAllLocked(std::function<void(unsigned int)> callback);
        
        std::size_t size() const;
        
        // collect prefix-level mutation flags from another mutation log
        void add(const MutationLog &other);
    
    private:
        // locked-section specific mutation flags (-1 = released)
        std::vector<char> m_mutation_flags;
        // the flag for additional speedup
        bool m_all_mutation_flags_set = false;
    };

}