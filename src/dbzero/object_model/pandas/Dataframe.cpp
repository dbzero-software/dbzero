#include "Dataframe.hpp"
#include <dbzero/object_model/value.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/object_model/object.hpp>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/object_model/pandas/Block.hpp>
#include <dbzero/object_model/pandas/Dataframe.hpp>
#include <dbzero/object_model/ObjectBase.hpp>

namespace db0::object_model::pandas

{
    
    GC0_Define(DataFrame)

    DataFrame::DataFrame(db0::swine_ptr<Fixture> &fixture)
        : super_t(fixture, *fixture)
        , m_frame_index(fixture->myPtr((*this)->m_indexes.getAddress()))
        , m_blocks(fixture->myPtr((*this)->m_blocks.getAddress()))
    {
    }
    
    DataFrame::DataFrame(db0::swine_ptr<Fixture> &fixture, std::uint64_t address)
        : super_t(super_t::tag_from_address(), fixture, address)
        , m_frame_index(fixture->myPtr((*this)->m_indexes.getAddress()))
        , m_blocks(fixture->myPtr((*this)->m_blocks.getAddress()))
    {
    }

    DataFrame::~DataFrame()
    {
        // unregister needs to be called before destruction of members
        unregister();
    }
    
    void DataFrame::appendIndex(ObjectPtr lang_value)
    {
        using TypeId = db0::bindings::TypeId;
        
        m_frame_index.push_back(LangToolkit::getTypeManager().extractList(lang_value).getAddress());
    }

    void DataFrame::detach()
    {
        m_frame_index.detach();
        m_blocks.detach();
        super_t::detach();
    }

    void DataFrame::appendBlock(ObjectPtr block)
    {
        m_blocks.push_back(LangToolkit::getTypeManager().extractBlock(block).getAddress());
    }

    void DataFrame::setBlock(Py_ssize_t i, ObjectPtr block)
    {
        m_blocks[i] = LangToolkit::getTypeManager().extractBlock(block).getAddress();
    }
    
    DataFrame::ObjectSharedPtr DataFrame::getBlock(std::size_t i) const
    {
        if (i >= m_blocks.size()) {
            THROWF(db0::InputException) << "Index out of range: " << i;
        }
        auto fixture = tryGetFixture();
        if (fixture) {
            return LangToolkit::unloadBlock(fixture, m_blocks[i].cast<std::uint64_t>());
        } else {
            throw std::runtime_error("Cannot get fixture");
        }        
    }
    
    DataFrame *DataFrame::makeNew(void *at_ptr, db0::swine_ptr<Fixture> &fixture)
    {
        return new (at_ptr) DataFrame(fixture);
    }

}