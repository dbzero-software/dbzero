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
        MutationLog(int locked_sections);

        // collect prefix-level mutation flags (for locked sections)
        void onDirty();

        void beginLocked(unsigned int locked_section_id);
        bool endLocked(unsigned int locked_section_id);
        // ends all locked sections, invokes callback for all mutated ones
        void endAllLocked(std::function<void(unsigned int)> callback);

    private:
        // locked-section specific mutation flags (-1 = released)
        std::vector<char> m_mutation_flags;
        // the flag for additional speedup
        bool m_all_mutation_flags_set = false;
    };

}