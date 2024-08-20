#include "AtomicContext.hpp"
#include "Workspace.hpp"
#include <dbzero/object_model/dict/Dict.hpp>
#include <dbzero/object_model/set/Set.hpp>
#include <dbzero/object_model/list/List.hpp>
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/object_model/tuple/Tuple.hpp>
#include <dbzero/object_model/index/Index.hpp>
#include <dbzero/object_model/pandas/Block.hpp>

namespace db0

{
    
    // MEMO_OBJECT specialization
    template <> void detachObject<TypeId::MEMO_OBJECT, PyToolkit>(PyObjectPtr obj_ptr) {
        PyToolkit::getTypeManager().extractObject(obj_ptr).detach();
    }

    // DB0_BLOCK specialization
    template <> void detachObject<TypeId::DB0_BLOCK, PyToolkit>(PyObjectPtr obj_ptr) {
        PyToolkit::getTypeManager().extractBlock(obj_ptr).detach();
    }

    // DB0_LIST specialization
    template <> void detachObject<TypeId::DB0_LIST, PyToolkit>(PyObjectPtr obj_ptr) {
        PyToolkit::getTypeManager().extractList(obj_ptr).detach();
    }

    // DB0_INDEX specialization
    template <> void detachObject<TypeId::DB0_INDEX, PyToolkit>(PyObjectPtr obj_ptr) {
        PyToolkit::getTypeManager().extractIndex(obj_ptr).detach();
    }

    // DB0_SET specialization
    template <> void detachObject<TypeId::DB0_SET, PyToolkit>(PyObjectPtr obj_ptr) {
        PyToolkit::getTypeManager().extractSet(obj_ptr).detach();
    }

    // DB0_DICT specialization
    template <> void detachObject<TypeId::DB0_DICT, PyToolkit>(PyObjectPtr obj_ptr) {
        PyToolkit::getTypeManager().extractDict(obj_ptr).detach();
    }

    // DB0_TUPLE specialization
    template <> void detachObject<TypeId::DB0_TUPLE, PyToolkit>(PyObjectPtr obj_ptr) {
        PyToolkit::getTypeManager().extractTuple(obj_ptr).detach();
    }
    
    template <> void registerDetachFunctions<PyToolkit>(std::vector<void (*)(PyObjectPtr)> &functions)
    {
        functions.resize(static_cast<int>(TypeId::COUNT));
        std::fill(functions.begin(), functions.end(), nullptr);
        functions[static_cast<int>(TypeId::MEMO_OBJECT)] = detachObject<TypeId::MEMO_OBJECT, PyToolkit>;
        functions[static_cast<int>(TypeId::DB0_BLOCK)] = detachObject<TypeId::DB0_BLOCK, PyToolkit>;
        functions[static_cast<int>(TypeId::DB0_LIST)] = detachObject<TypeId::DB0_LIST, PyToolkit>;
        functions[static_cast<int>(TypeId::DB0_INDEX)] = detachObject<TypeId::DB0_INDEX, PyToolkit>;
        functions[static_cast<int>(TypeId::DB0_SET)] = detachObject<TypeId::DB0_SET, PyToolkit>;
        functions[static_cast<int>(TypeId::DB0_DICT)] = detachObject<TypeId::DB0_DICT, PyToolkit>;
        functions[static_cast<int>(TypeId::DB0_TUPLE)] = detachObject<TypeId::DB0_TUPLE, PyToolkit>;
    }

    AtomicContext::AtomicContext(std::shared_ptr<Workspace> &workspace)
        : m_workspace(workspace)
    {
        m_workspace->beginAtomic(this);
    }
    
    void AtomicContext::makeNew(void *ptr, std::shared_ptr<Workspace> &workspace) {
        new (ptr) AtomicContext(workspace);
    }

    void AtomicContext::cancel()
    {
        if (!m_active) {
            THROWF(db0::InternalException) << "atomic 'cancel' failed: operation already completed" << THROWF_END;
        }

        // all objects from context need to be detached
        auto &type_manager = LangToolkit::getTypeManager();
        for (auto &pair : m_objects) {
            detachObject<PyToolkit>(type_manager.getTypeId(pair.second.get()), pair.second.get());
        }
        m_workspace->cancelAtomic();
        m_active = false;
    }
    
    void AtomicContext::exit()
    {
        if (!m_active) {
            return;
        }

        // detach / flush all workspace objects
        m_workspace->detach();
        // all objects from context need to be detached
        auto &type_manager = LangToolkit::getTypeManager();
        for (auto &pair : m_objects) {
            detachObject<PyToolkit>(type_manager.getTypeId(pair.second.get()), pair.second.get());
        }        
        
        m_workspace->endAtomic();
    }
    
    void AtomicContext::add(std::uint64_t address, ObjectPtr lang_object) 
    {        
        if (m_objects.find(address) == m_objects.end()) {
            m_objects.insert({address, lang_object});            
        }        
    }

    void AtomicContext::moveFrom(AtomicContext &other, std::uint64_t src_address, std::uint64_t dst_address)
    {
        auto it = other.m_objects.find(src_address);
        if (it != other.m_objects.end()) {
            add(dst_address, it->second.get());
            other.m_objects.erase(it);
        }
    }

}