#pragma once

#include <vector>
#include <dbzero/core/collections/b_index/bindex_interface.hpp>

namespace db0 

{
    
    /**
     * Enums acting as markers allowing to inline tree structure in linear sequence.
     */
    enum LogicalOperation
    {
        NOTHING, AND, OR, ORX, ANDNOT, CLOSE
    };

    /**
     * Struct holiding iterator info.
     */
    struct IteratorInfo
    {
        LogicalOperation m_operation = NOTHING;
        std::uint64_t m_id;
        int m_direction = -1;
        std::string m_label;
        // FT_IndexIterator specific fields
        std::uint64_t m_data;
        db0::bindex::type m_type = db0::bindex::empty;
        std::uint64_t m_key;
        std::uint64_t m_limit;
        std::uint64_t m_index_key;
    };

    /**
     * Class used for visiting iterator tree structure.
     */
    class IteratorVisitor
    {
        std::vector<IteratorInfo> m_iterators;
        
        void enterLogicalIterator(LogicalOperation, std::uint64_t id, int direction);

    public:
        IteratorVisitor() = default;
        virtual ~IteratorVisitor() = default;

        void enterAndIterator(std::uint64_t id, int direction) noexcept;
        void enterOrIterator(std::uint64_t id, int direction) noexcept;
        void enterOrxIterator(std::uint64_t id, int direction) noexcept;
        void enterAndNotIterator(std::uint64_t id, int direction) noexcept;
        void leaveIterator() noexcept;

        void onIndexIterator(std::uint64_t id,
                             int direction,
                             std::uint64_t data,
                             db0::bindex::type data_type,
                             std::uint64_t key,
                             std::uint64_t limit_by,
                             std::uint64_t index_key) noexcept;

        template<typename key_t>
        void onIndexIterator(std::uint64_t /*id*/,
                             int /*direction*/,
                             std::uint64_t /*data*/,
                             db0::bindex::type,
                             key_t,
                             key_t,
                             std::uint64_t) noexcept
        {
            //need this one for test to compile :(
        }
        
        void setLabel(const std::string &label);

        const std::vector<IteratorInfo>& getTree() const noexcept;

        void dumpTree(std::ostream& out);
    };

} 
