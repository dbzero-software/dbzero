#include "Class.hpp"
#include <dbzero/core/utils/uuid.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include "ClassFactory.hpp"
#include <dbzero/workspace/Snapshot.hpp>
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/object_model/value/ObjectId.hpp>
#include <dbzero/object_model/value/StorageClass.hpp>

DEFINE_ENUM_VALUES(db0::ClassOptions, "SINGLETON")

namespace db0::object_model

{
    
    using namespace db0;
    using namespace db0::pools;
    
    GC0_Define(Class)
    
    o_class::o_class(RC_LimitedStringPool &string_pool, const std::string &name, std::optional<std::string> module_name,
        const VFieldVector &members, const Schema &schema, const char *type_id, const char *prefix_name, ClassFlags flags,
        const std::uint32_t base_class_ref)
        : m_uuid(db0::make_UUID())
        , m_name(string_pool.addRef(name))
        , m_type_id(type_id ? string_pool.addRef(type_id) : LP_String())
        , m_prefix_name(prefix_name ? string_pool.addRef(prefix_name) : LP_String())
        , m_members_ptr(members)
        , m_schema_ptr(schema)
        , m_flags(flags)
        , m_base_class_ref(base_class_ref)
    {
        if (module_name) {
            m_module_name = string_pool.addRef(*module_name);
        }
    }
    
    Class::Member::Member(FieldID field_id, const char *name)
        : m_field_id(field_id)
        , m_name(name)
    {
    }
    
    Class::Member::Member(FieldID field_id, const std::string &name)
        : m_field_id(field_id)
        , m_name(name)
    {
    }
    
    Class::Class(db0::swine_ptr<Fixture> &fixture, const std::string &name, std::optional<std::string> module_name,
        const char *type_id, const char *prefix_name, const std::vector<std::string> &init_vars, ClassFlags flags, 
        std::shared_ptr<Class> base_class)
        : super_t(fixture, fixture->getLimitedStringPool(), name, module_name, VFieldVector(*fixture), Schema(*fixture), 
            type_id, prefix_name, flags, base_class ? ClassFactory::classRef(*base_class) : 0)
        , m_members((*this)->m_members_ptr(*fixture))
        , m_schema((*this)->m_schema_ptr(*fixture))
        , m_base_class_ptr(base_class)
        , m_uid(this->fetchUID())
    {
        m_schema.postInit(getTotalFunc());
        // copy all init vars from base class
        if (m_base_class_ptr) {        
            const auto &base_init_vars = m_base_class_ptr->getInitVars();
            std::copy(base_init_vars.begin(), base_init_vars.end(), std::inserter(m_init_vars, m_init_vars.end()));
        }
        std::copy(init_vars.begin(), init_vars.end(), std::inserter(m_init_vars, m_init_vars.end()));
    }
    
    Class::Class(db0::swine_ptr<Fixture> &fixture, Address address)
        : super_t(super_t::tag_from_address(), fixture, address)
        , m_members((*this)->m_members_ptr(*fixture))
        , m_schema((*this)->m_schema_ptr(*fixture))
        , m_uid(this->fetchUID())
    {
        m_schema.postInit(getTotalFunc());
        // initialize base class if such exists
        if ((*this)->m_base_class_ref) {
            auto fixture = this->getFixture();
            m_base_class_ptr = fixture->get<ClassFactory>().getTypeByClassRef((*this)->m_base_class_ref).m_class;
        }

        // fetch all members into cache
        refreshMemberCache();
    }
    
    Class::~Class()
    {
        // unregister needs to be called before the destruction of members
        unregister();        
    }
    
    std::string Class::getName() const {
        return getFixture()->getLimitedStringPool().fetch((*this)->m_name);
    }   
    
    std::optional<std::string> Class::getTypeId() const
    {
        if (!(*this)->m_type_id) {
            return std::nullopt;
        }
        return getFixture()->getLimitedStringPool().fetch((*this)->m_type_id);
    }
    
    FieldID Class::addField(const char *name)
    {
        assert(m_index.find(name) == m_index.end());
        bool is_init_var = m_init_vars.find(name) != m_init_vars.end();
        // NOTE: we start field IDs from 1
        auto next_field_id = FieldID::fromIndex(m_members.size());
        m_members.emplace_back(getFixture()->getLimitedStringPool(), name);
        m_member_cache.emplace_back(new Member(next_field_id, name));
        m_index[name] = { next_field_id, is_init_var };
        return next_field_id;
    }
    
    std::pair<FieldID, bool> Class::findField(const char *name) const
    {
        auto it = m_index.find(name);
        if (it == m_index.end()) {
            // try refreshing the cache
            refreshMemberCache();
            it = m_index.find(name);
            if (it == m_index.end()) {
                // field ID not found, check for possible initialization variable
                bool is_init_var = m_init_vars.find(name) != m_init_vars.end();
                return { FieldID(), is_init_var };
            }
        }

        return it->second;
    }
    
    const Class::Member *Class::tryGet(FieldID field_id) const
    {
        auto index = field_id.getIndex();
        if (index < m_member_cache.size()) {
            return m_member_cache[index].get();
        }
        
        if (index >= m_member_cache.size()) {
            // try updating cache (fields might've been added by other process)
            refreshMemberCache();            
        }
        if (index < m_member_cache.size()) {
            return m_member_cache[index].get();
        }
        return nullptr;
    }
    
    const Class::Member &Class::get(FieldID field_id) const
    {
        auto member_ptr = tryGet(field_id);
        if (!member_ptr) {
            THROWF(db0::InputException) << "Member slot not found: " << field_id.getIndex();
        }
        return *member_ptr;
    }
    
    const Class::Member *Class::tryGet(const char *name) const
    {
        auto it = m_index.find(name);
        if (it != m_index.end()) {
            return &get(it->second.first);
        }
        return nullptr;
    }

    const Class::Member &Class::get(const char *name) const
    {
        auto member_ptr = tryGet(name);
        if (!member_ptr) {
            THROWF(db0::InputException) << "Field " << name << " not found in class " << getName();
        }
        return *member_ptr;
    }
    
    bool Class::unloadSingleton(void *at) const
    {
        if (!(*this)->m_singleton_address.isValid()) {
            return false;
        }
        
        auto fixture = getFixture();
        auto &class_factory = fixture->get<ClassFactory>();
        auto stem = Object::unloadStem(fixture, (*this)->m_singleton_address);
        auto type = class_factory.getTypeByPtr(
            db0::db0_ptr_reinterpret_cast<Class>()(ClassFactory::classRefToAddress(stem->m_class_ref))).m_class;
        // unload from stem
        Object::unload(at, std::move(stem), type);
        return true;
    }
    
    bool Class::isSingleton() const {
        return (*this)->m_flags[ClassOptions::SINGLETON];
    }
    
    bool Class::isExistingSingleton() const {
        return isSingleton() && (*this)->m_singleton_address.isValid();
    }
    
    void Class::setSingletonAddress(Object &object)
    {
        assert(!(*this)->m_singleton_address.isValid());
        assert(isSingleton());
        // increment reference count in order to prevent singleton object from being destroyed
        object.incRef();
        modify().m_singleton_address = object.getUniqueAddress();
    }
    
    void Class::refreshMemberCache() const
    {        
        assert(m_members.size() >= m_member_cache.size());
        if (m_members.size() == m_member_cache.size()) {
            return;
        }
        
        // Fetch all members into cache
        unsigned int index = m_member_cache.size();
        auto fixture = getFixture();
        auto &string_pool = fixture->getLimitedStringPool();
        for (auto it = m_members.begin(index), end = m_members.end(); it != end; ++it, ++index) {
            auto field_name = string_pool.fetch(it->m_name);
            m_member_cache.emplace_back(new Member(FieldID::fromIndex(index), field_name));
            bool is_init_var = m_init_vars.find(field_name) != m_init_vars.end();
            m_index[field_name] = { FieldID::fromIndex(index), is_init_var };
        }
    }
    
    std::string Class::getTypeName() const {
        return getFixture()->getLimitedStringPool().fetch((*this)->m_name);
    }

    std::optional<std::string> Class::tryGetModuleName() const 
    {
        if (!(*this)->m_module_name) {
            return std::nullopt;
        }
        return getFixture()->getLimitedStringPool().fetch((*this)->m_module_name);
    }

    std::string Class::getModuleName() const
    {
        auto module_name = tryGetModuleName();
        if (!module_name) {
            THROWF(db0::InternalException) << "Module name not found for class " << getTypeName();
        }
        return *module_name;
    }   
        
    std::optional<std::string> getNameVariant(std::optional<std::string> type_id, std::optional<std::string> type_name,
        std::optional<std::string> module_name, std::optional<std::string> type_fields_str, int variant_id)
    {
        switch (variant_id) {
            case 0: {
                if (!type_id) {
                    return std::nullopt;                    
                }
                return type_id;
            }
            break;

            case 1: {
                // NOTE: module name may not be available in some contexts - e.g. 
                // dynamically loaded classes or jupyter notebook
                // FIXME: design handling missing module names
                assert(type_name);
                std::stringstream _str;
                _str << "cls:" << *type_name;
                if (module_name) {
                    _str << ".pkg:" << *module_name;
                }
                return _str.str();                
            }
            break;

            case 2: {
                // variant 2. name + fields
                // std::stringstream _str;
                // _str << "cls:" << _class.getTypeName() << "." << db0::python::getTypeFields(lang_class);
                // return _str.str();
            }
            break;

            case 3: {
                // variant 3. module + fields
                // std::stringstream _str;
                // _str << "pkg:" << _class.getModuleName() << "." << db0::python::getTypeFields(lang_class);
                // return _str.str();
            }
            break;

            default: {
                assert(false);
                THROWF(db0::InputException) << "Invalid type name variant id: " << variant_id;
            }
            break;
        }
        return std::nullopt;
    }
    
    std::optional<std::string> getNameVariant(const Class &_class, int variant_id) {
        // FIXME: implement get type fields
        return getNameVariant(_class.getTypeId(), _class.getTypeName(), _class.tryGetModuleName(), std::nullopt, variant_id);
    }
    
    void Class::renameField(const char *from_name, const char *to_name)
    {
        assert(from_name);
        assert(to_name);
        if (to_name == from_name) {
            return;
        }

        auto [field_id, was_init_var] = findField(from_name);
        if (!field_id) {
            // do not raise exception if the "to_name" field already exists (likely double rename attemp)            
            if (findField(to_name).first) {
                return;
            }
            THROWF(db0::InputException) << "Field " << from_name << " not found in class " << getName();
        }

        // 1. update in fields vector
        auto &string_pool = getFixture()->getLimitedStringPool();
        // unreference old name in the string pool
        string_pool.unRef(m_members[field_id.getIndex()].m_name);
        m_members.modifyItem(field_id.getIndex()).m_name = string_pool.addRef(to_name);
        // 2. update in member's cache
        refreshMemberCache();
        m_member_cache[field_id.getIndex()]->m_name = to_name;
        // 3. update in the in-memory index
        m_index.erase(from_name);
        auto is_init_var = m_init_vars.find(to_name) != m_init_vars.end();
        m_index[to_name] = { field_id, is_init_var };
    }
    
    void Class::detach()
    {
        m_members.detach();
        m_schema.detach();
        super_t::detach();
    }
    
    void Class::unlinkSingleton() {
        modify().m_singleton_address = {};
    }
    
    void Class::commit()
    {
        m_members.commit();        
        m_schema.commit();
        super_t::commit();
    }

    bool Class::operator!=(const Class &rhs) const
    {
        if (*this->getFixture() != *rhs.getFixture()) {
            return true;
        }
        return this->getAddress() != rhs.getAddress();
    }

    std::uint32_t Class::fetchUID() const
    {
        // return UID as relative address from the underlying SLOT
        auto result = this->getFixture()->makeRelative(this->getAddress(), SLOT_NUM);
        // relative address must not exceed SLOT size
        assert(result < std::numeric_limits<std::uint32_t>::max());
        return result;
    }
    
    bool Class::isExistingSingleton(std::uint64_t fixture_uuid) const
    {
        if (!isSingleton()) {
            return false;
        }

        auto &class_factory = getFixture()->get<ClassFactory>();
        auto lang_class = class_factory.getLangType(*this);
        auto other_fixture = getFixture()->getWorkspace().getFixture(fixture_uuid, AccessType::READ_ONLY);
        auto &other_factory = other_fixture->get<ClassFactory>();
        auto other_type = other_factory.tryGetExistingType(lang_class.get());
        return other_type && other_type->isExistingSingleton();
    }
    
    bool Class::unloadSingleton(void *at, std::uint64_t fixture_uuid) const
    {    
        auto &class_factory = getFixture()->get<ClassFactory>();
        auto lang_class = class_factory.getLangType(*this);
        auto other_fixture = getFixture()->getWorkspace().getFixture(fixture_uuid, AccessType::READ_ONLY);
        auto &other_factory = other_fixture->get<ClassFactory>();
        // locate corresponding type in the other fixture
        auto other_type = other_factory.tryGetExistingType(lang_class.get());
        if (!other_type) {
            return false;
        }
        return other_type->unloadSingleton(at);
    }
    
    ObjectId Class::getSingletonObjectId() const
    {
        if (!isSingleton() || !isExistingSingleton()) {
            THROWF(db0::InternalException) << "Singleton object not found for class " << getTypeName();
        }

        auto singleton_addr = (*this)->m_singleton_address;
        return {
            getFixture()->getUUID(),
            singleton_addr,
            StorageClass::OBJECT_REF
        };
    }
    
    ObjectId Class::getClassId() const
    {
        return {
            getFixture()->getUUID(),
            // NOTICE: no instance ID for the class-ref
            db0::UniqueAddress(this->getAddress(), UniqueAddress::INSTANCE_ID_MAX),
            StorageClass::DB0_CLASS
        };
    }
    
    std::unordered_map<std::string, std::uint32_t> Class::getMembers() const
    {        
        refreshMemberCache();
        std::unordered_map<std::string, std::uint32_t> result;
        for (auto &item: m_index) {
            result[item.first] = item.second.first.getIndex();
        }

        return result;
    }
    
    std::shared_ptr<Class> Class::getNullClass() {
        return std::shared_ptr<Class>(new Class());
    }

    std::shared_ptr<Class> Class::tryGetBaseClass() const {
        return m_base_class_ptr;
    }

    void Class::setInitVars(const std::vector<std::string> &init_vars)
    {
        assert(m_init_vars.empty());        
        std::copy(init_vars.begin(), init_vars.end(), std::inserter(m_init_vars, m_init_vars.end()));        
        // update the field index
        for (auto &name: m_init_vars) {
            auto it = m_index.find(name);
            if (it != m_index.end()) {
                it->second.second = true;
            }            
        }
    }

    Address Class::getSingletonAddress() const {
        return (*this)->m_singleton_address;
    }
    
    const std::unordered_set<std::string> &Class::getInitVars() const {
        return m_init_vars;
    }

    std::function<unsigned int()> Class::getTotalFunc() const
    {
        return [this]() {
            // NOTE: -1 because Class is also referenced (+1) by the ClassFactory
            return this->getRefCount() - 1;
        };
    }
    
}
