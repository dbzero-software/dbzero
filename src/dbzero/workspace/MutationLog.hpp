#pragma once

#include <vector>
#include <functional>

namespace db0

{
    enum class MutationOp: int
    {        
        INIT = 0,
        BEGIN_LOCKED = 1,
        END_LOCKED = 2,
        END_ALL_LOCKED = 3
    };

    using MutationHandler = std::function<bool(MutationOp, unsigned int int_value,
        std::function<void(unsigned int)> callback)>;
    
    // Mutation log tracks changes made to prefixes during a locked section.
    class MutationLog
    {
    public:
        // @param locked_sections the number of active locked sections
        MutationLog(int locked_sections = 0);
        
        void init(int size);

        // collect prefix-level mutation flags (for locked sections)
        void onDirty();

        void beginLocked(unsigned int locked_section_id);
        bool endLocked(unsigned int locked_section_id);
        // ends all locked sections, invokes callback for all mutated ones
        void endAllLocked(std::function<void(unsigned int)> callback);
        
        MutationHandler getHandler();

        std::size_t size() const;
    
    private:
        // locked-section specific mutation flags (-1 = released)
        std::vector<char> m_mutation_flags;
        // the flag for additional speedup
        bool m_all_mutation_flags_set = false;
    };

}