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
    template <> void detachObject<TypeId::MEMO_OBJECT, PyToolkit>(PyObjectPtr lang_value) {
        PyToolkit::getTypeManager().extractObject(lang_value).detach();        
    }

    // DB0_BLOCK specialization
    template <> void detachObject<TypeId::DB0_BLOCK, PyToolkit>(PyObjectPtr lang_value) {
        PyToolkit::getTypeManager().extractBlock(lang_value).detach();
    }

    // DB0_LIST specialization
    template <> void detachObject<TypeId::DB0_LIST, PyToolkit>(PyObjectPtr lang_value) {
        PyToolkit::getTypeManager().extractList(lang_value).detach();
    }

    // DB0_INDEX specialization
    template <> void detachObject<TypeId::DB0_INDEX, PyToolkit>(PyObjectPtr lang_value) {
        PyToolkit::getTypeManager().extractIndex(lang_value).detach();
    }

    // DB0_SET specialization
    template <> void detachObject<TypeId::DB0_SET, PyToolkit>(PyObjectPtr lang_value) {
        PyToolkit::getTypeManager().extractSet(lang_value).detach();
    }

    // DB0_DICT specialization
    template <> void detachObject<TypeId::DB0_DICT, PyToolkit>(PyObjectPtr lang_value) {
        PyToolkit::getTypeManager().extractDict(lang_value).detach();
    }

    // DB0_TUPLE specialization
    template <> void detachObject<TypeId::DB0_TUPLE, PyToolkit>(PyObjectPtr lang_value) {
        PyToolkit::getTypeManager().extractTuple(lang_value).detach();
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
        for (auto &object : m_objects) {
            detachObject<PyToolkit>(type_manager.getTypeId(object.get()), object.get());
        }
        m_workspace->cancelAtomic();
        m_active = false;
    }
    
    void AtomicContext::exit()
    {
        if (!m_active) {
            return;
        }

        // all objects from context need to be detached
        auto &type_manager = LangToolkit::getTypeManager();
        for (auto &object : m_objects) {
            detachObject<PyToolkit>(type_manager.getTypeId(object.get()), object.get());
        }
        m_workspace->endAtomic();
    }

    void AtomicContext::add(ObjectPtr lang_object) {
        m_objects.insert(lang_object);
    }

}