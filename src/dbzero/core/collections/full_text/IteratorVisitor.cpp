#include "IteratorVisitor.hpp"
#include <sstream>

namespace db0

{

    void IteratorVisitor::enterLogicalIterator(LogicalOperation operation, std::uint64_t id, int direction) 
    {
        m_iterators.emplace_back();
        IteratorInfo &info = m_iterators.back();
        info.m_operation = operation;
        info.m_id = id;
        info.m_direction = direction;
    }

    void IteratorVisitor::enterAndIterator(std::uint64_t id, int direction) noexcept {
        enterLogicalIterator(AND, id, direction);
    }

    void IteratorVisitor::enterOrIterator(std::uint64_t id, int direction) noexcept {
        enterLogicalIterator(OR, id, direction);
    }

    void IteratorVisitor::enterOrxIterator(std::uint64_t id, int direction) noexcept {
        enterLogicalIterator(ORX, id, direction);
    }

    void IteratorVisitor::enterAndNotIterator(std::uint64_t id, int direction) noexcept {
        enterLogicalIterator(ANDNOT, id, direction);
    }

    void IteratorVisitor::leaveIterator() noexcept {
        m_iterators.emplace_back();
        m_iterators.back().m_operation = CLOSE;
    }

    void IteratorVisitor::onIndexIterator(std::uint64_t id, int direction, std::uint64_t data, db0::bindex::type data_type,
        std::uint64_t key, std::uint64_t limit_by, std::uint64_t index_key) noexcept
    {
        m_iterators.emplace_back();
        IteratorInfo &info = m_iterators.back();
        
        info.m_id        = id;
        info.m_direction = direction;
        info.m_data      = data;
        info.m_type      = data_type;
        info.m_key       = key;
        info.m_limit     = limit_by;
        info.m_index_key = index_key;
    }

    void IteratorVisitor::setLabel(const std::string &label) {
        assert(!m_iterators.empty());
        m_iterators.back().m_label = label;
    }

    const std::vector<db0::IteratorInfo>& IteratorVisitor::getTree() const noexcept
    {
        return m_iterators;
    }

    void IteratorVisitor::dumpTree(std::ostream& out)
    {
        std::string leading_spaces;

        auto enter([&leading_spaces]()->void {
                leading_spaces.push_back(' ');
                leading_spaces.push_back(' ');
            });

        auto leave([&leading_spaces]()->void {
                leading_spaces.pop_back();
                leading_spaces.pop_back();
            });

        auto write([&leading_spaces, &out](const std::string& text)->void {
                out << leading_spaces << text << std::endl;
            });

        for (const auto& elt : m_iterators)
        {
            switch (elt.m_operation)
            {
            case NOTHING:
                break;

            case AND:
                write("and(");
                enter();
                break;

            case OR:
                write("or(");
                enter();
                break;

            case ORX:
                write("orx(");
                enter();
                break;
                
            case ANDNOT:
                write("andnot(");
                enter();
                break;

            case CLOSE:
                leave();
                write(")");
                break;

            }

            if (elt.m_data != std::uint64_t())
            {
                std::stringstream output;
                output << "Data: " << elt.m_data
                    << " Key: " << elt.m_key
                    << " Limit: " << elt.m_limit
                    << " Direction: " << elt.m_direction
                    << " Type: " << elt.m_type;
                write(output.str());
            }
        }
    }
    
}