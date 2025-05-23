#include "MemoTypeDecoration.hpp"
#include "Memo.hpp"
#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/workspace/PrefixName.hpp>

namespace db0::python

{

    bool PyMemo_Check(PyObject *obj) {
        return PyMemoType_Check(Py_TYPE(obj));
    }
    
    bool PyMemoType_Check(PyTypeObject *type) {
        return type->tp_init == reinterpret_cast<initproc>(PyAPI_MemoObject_init);
    }
    
    MemoTypeDecoration::MemoTypeDecoration(MemoTypeDecoration &&other)
        : m_py_module(std::move(other.m_py_module))
        , m_prefix_name_ptr(other.m_prefix_name_ptr)
        , m_type_id(other.m_type_id)
        , m_file_name(other.m_file_name)
        , m_init_vars(std::move(other.m_init_vars))        
        , m_py_dyn_prefix_callable(std::move(other.m_py_dyn_prefix_callable))
        , m_migrations(std::move(other.m_migrations))
    {
        m_fixture_uuid.store(other.m_fixture_uuid.load());
        other.m_fixture_uuid = 0;
        init();
    }
    
    MemoTypeDecoration::MemoTypeDecoration(shared_py_object<PyObject*> py_module,
        const char *prefix_name, const char *type_id, const char *file_name, 
        std::vector<std::string> &&init_vars, 
        shared_py_object<PyObject*> py_dyn_prefix_callable,
        std::vector<Migration> &&migrations)
        : m_py_module(py_module)
        , m_prefix_name_ptr(prefix_name)
        , m_type_id(type_id)
        , m_file_name(file_name)
        , m_init_vars(std::move(init_vars))
        , m_py_dyn_prefix_callable(std::move(py_dyn_prefix_callable))
        , m_migrations(std::move(migrations))
    {
        init();
    }
    
    MemoTypeDecoration &MemoTypeDecoration::operator=(MemoTypeDecoration &&other)
    {
        m_py_module = std::move(other.m_py_module);
        m_prefix_name_ptr = other.m_prefix_name_ptr;
        m_type_id = other.m_type_id;
        m_file_name = other.m_file_name;
        m_init_vars = std::move(other.m_init_vars);
        m_py_dyn_prefix_callable = std::move(other.m_py_dyn_prefix_callable);
        m_migrations = std::move(other.m_migrations);
        m_fixture_uuid.store(other.m_fixture_uuid.load());
        other.m_fixture_uuid = 0;
        init();
        return *this;
    }
    
    void MemoTypeDecoration::init()
    {
        for (auto &migration: m_migrations) {
            for (auto &var: migration.m_vars) {
                if (m_ix_migrations.find(var) != m_ix_migrations.end()) {
                    THROWF(db0::InternalException) << "Duplicate migration for variable: " << var;
                }
                m_ix_migrations[var] = &migration;
            }
        }
    }

    MemoTypeDecoration::~MemoTypeDecoration()
    {
        if (!Py_IsInitialized()) {
            // discard unreachable Python objects
            m_py_module.steal();
            m_py_dyn_prefix_callable.steal();            
        }
    }
    
    std::uint64_t MemoTypeDecoration::getFixtureUUID(std::optional<AccessType> access_type)
    {
        if (m_prefix_name_ptr && !m_fixture_uuid) {
            if (!access_type) {
                access_type = AccessType::READ_WRITE;
            }    
            // initialize fixture by prefix name and keep UUID for future use
            auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getFixture(m_prefix_name_ptr, *access_type);
            m_fixture_uuid = fixture->getUUID();
        }
        return m_fixture_uuid;
    }
    
    bool MemoTypeDecoration::hasDynPrefix() const {
        return m_py_dyn_prefix_callable.get() != nullptr;
    }
    
    std::string MemoTypeDecoration::getDynPrefix(PyObject *args, PyObject *kwargs) const
    {
        assert(m_py_dyn_prefix_callable.get());
        auto py_result = Py_OWN(PyObject_Call(*m_py_dyn_prefix_callable, args, kwargs));
        if (!py_result || py_result.get() == Py_None) {
            return "";
        }
        return PyUnicode_AsUTF8(*py_result);
    }
        
    const char *MemoTypeDecoration::getPrefixName() const {
        return m_prefix_name_ptr ? m_prefix_name_ptr : "";
    }

    const std::vector<std::string> &MemoTypeDecoration::getInitVars() const {
        return m_init_vars;
    }
    
    void MemoTypeDecoration::forAllMigrations(const std::unordered_set<std::string> &available_members,
        std::function<bool(Migration &)> callback) const
    {
        // invoke migrations for all missing members, in order of initialization
        for (auto &init_var: m_init_vars) {
            if (available_members.find(init_var) == available_members.end()) {
                auto it = m_ix_migrations.find(init_var);                
                if (it != m_ix_migrations.end()) {
                    if (!callback(*it->second)) {
                        return;
                    }
                }
            }
        }
    }
    
    bool MemoTypeDecoration::hasMigrations() const {
        return !m_migrations.empty();
    }

    MemoTypeDecoration &MemoTypeDecoration::get(PyTypeObject *type)
    {
        assert(PyMemoType_Check(type) && "Invalid type (expected memo type)");
        return PyToolkit::getTypeManager().getMemoTypeDecoration(type);
    }
    
    void MemoTypeDecoration::close() {
        m_fixture_uuid = 0;
    }

}