#include "Object.hpp"
#include <random>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/core/serialization/string.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/object_model/class.hpp>
#include <dbzero/object_model/value.hpp>
#include <dbzero/object_model/list/List.hpp>
#include <dbzero/core/utils/uuid.hpp>

namespace db0::object_model

{
    
    GC0_Define(Object)
    thread_local ObjectInitializerManager Object::m_init_manager;

    o_object::o_object(std::uint32_t class_ref, std::uint32_t ref_count, const PosVT::Data &pos_vt_data,
        const XValue *index_vt_begin, const XValue *index_vt_end)
        : m_header(ref_count)
        , m_class_ref(class_ref)        
    {
        arrangeMembers()
            (PosVT::type(), pos_vt_data)
            (IndexVT::type(), index_vt_begin, index_vt_end);
    }

    std::size_t o_object::measure(std::uint32_t, std::uint32_t, const PosVT::Data &pos_vt_data,
        const XValue *index_vt_begin, const XValue *index_vt_end)
    {
        return super_t::measureMembers()
            (PosVT::type(), pos_vt_data)
            (IndexVT::type(), index_vt_begin, index_vt_end);
    }
    
    const PosVT &o_object::pos_vt() const {
        return getDynFirst(PosVT::type());
    }

    PosVT &o_object::pos_vt() {
        return getDynFirst(PosVT::type());
    }

    const IndexVT &o_object::index_vt() const {
        return getDynAfter(pos_vt(), IndexVT::type());
    }
    
    IndexVT &o_object::index_vt() {
        return getDynAfter(pos_vt(), IndexVT::type());
    }
    
    Object::Object(std::shared_ptr<Class> db0_class)
        : m_is_dropped(false)
    {
        // prepare for initialization
        m_init_manager.addInitializer(*this, db0_class);
    }

    Object::Object(TypeInitializer &&type_initializer)
        : m_is_dropped(false)
    {
        // prepare for initialization
        m_init_manager.addInitializer(*this, std::move(type_initializer));
    }
    
    Object::Object(db0::swine_ptr<Fixture> &fixture, std::shared_ptr<Class> type, std::uint32_t ref_count, const PosVT::Data &pos_vt_data)
        : super_t(fixture, ClassFactory::classRef(*type), ref_count, pos_vt_data)
        , m_type(type)
        , m_is_dropped(false)
    {
    }

    Object::Object(db0::swine_ptr<Fixture> &fixture, std::uint64_t address)
        : super_t(super_t::tag_from_address(), fixture, address)
        , m_is_dropped(false)
    {
    }
    
    Object::Object(db0::swine_ptr<Fixture> &fixture, ObjectStem &&stem, std::shared_ptr<Class> type)
        : super_t(super_t::tag_from_stem(), fixture, std::move(stem))
        , m_type(type)
        , m_is_dropped(false)        
    {
    }
    
    Object::~Object()
    {   
        // unregister needs to be called before destruction of members
        unregister();
        if (!hasInstance()) {
            // release initializer if it exists, object not created
            m_init_manager.tryCloseInitializer(*this);
        }
    }
    
    Object *Object::makeNew(void *at_ptr, std::shared_ptr<Class> type) {
        // placement new
        return new (at_ptr) Object(type);
    }
    
    Object *Object::makeNew(void *at_ptr, TypeInitializer &&type_initializer) {
        // placement new
        return new (at_ptr) Object(std::move(type_initializer));
    }

    Object *Object::makeNull(void *at_ptr) {
        return new (at_ptr) Object();
    }
    
    Object::ObjectStem Object::unloadStem(db0::swine_ptr<Fixture> &fixture, std::uint64_t address)
    {
        db0::v_object<o_object> stem(db0::tag_verified(), fixture->myPtr(address));
        // validate the instance ID if assigned
        auto instance_id = db0::getInstanceId(address);
        if (instance_id && (instance_id != stem->m_header.m_instance_id)) {
            THROWF(db0::InputException) << "Invalid UUID or object has been deleted";
        }
        return stem;
    }
    
    Object *Object::unload(void *at_ptr, std::uint64_t address, std::shared_ptr<Class> type)
    {
        auto fixture = type->getFixture();
        Object *object = new (at_ptr) Object(fixture, address);
        object->setType(type);
        return object;
    }

    Object *Object::unload(void *at_ptr, ObjectStem &&stem, std::shared_ptr<Class> type)
    {        
        auto fixture = type->getFixture();
        // placement new
        return new (at_ptr) Object(fixture, std::move(stem), type);        
    }
    
    void Object::postInit(FixtureLock &fixture)
    {
        if (!hasInstance()) {
            auto &initializer = m_init_manager.getInitializer(*this);
            PosVT::Data pos_vt_data;
            auto index_vt_data = initializer.getData(pos_vt_data);
            
            // place object in the same fixture as its class
            // construct the dbzero instance & assign to self
            m_type = initializer.getClassPtr();
            assert(m_type);
            super_t::init(*fixture, ClassFactory::classRef(*m_type), initializer.getRefCount(), pos_vt_data,
                index_vt_data.first, index_vt_data.second);
            // reference associated class
            m_type->incRef();
            
            // bind singleton address (now that instance exists)
            if (m_type->isSingleton()) {
                m_type->setSingletonAddress(*this);
            }            
            initializer.close();
        }
        
        assert(hasInstance());
    }
    
    std::pair<db0::bindings::TypeId, StorageClass> Object::recognizeType(Fixture &fixture, ObjectPtr lang_value) const
    {
        auto type_id = LangToolkit::getTypeManager().getTypeId(lang_value);
        auto storage_class = TypeUtils::m_storage_class_mapper.getStorageClass(type_id);
        if (type_id == TypeId::MEMO_OBJECT) {
            // object reference must be from the same fixture
            auto &obj = LangToolkit::getTypeManager().extractObject(lang_value);
            if (fixture.getUUID() != obj.getFixture()->getUUID()) {
                THROWF(db0::InputException) << "Linking objects from different prefixes is not allowed. Store db0.uuid instead";
            }
        }
        return { type_id, storage_class };
    }

    void Object::setPreInit(const char *field_name, ObjectPtr obj_ptr) const
    {
        assert(!hasInstance());
        auto &initializer = m_init_manager.getInitializer(*this);
        auto fixture = initializer.getFixture();
        auto &db0_class = initializer.getClass();
        auto [type_id, storage_class] = recognizeType(*fixture, obj_ptr);
        
        // find already existing field index
        auto [field_id, is_init_var] = db0_class.findField(field_name);
        if (!field_id) {
            // update class definition
            field_id = db0_class.addField(field_name);
        }
        
        // register a member with the initializer
        initializer.set(field_id.getIndex(), storage_class, createMember<LangToolkit>(fixture, type_id, obj_ptr));
    }
    
    void Object::set(FixtureLock &fixture, const char *field_name, ObjectPtr lang_value)
    {
        assert(hasInstance());
        auto [type_id, storage_class] = recognizeType(**fixture, lang_value);
        
        assert(m_type);
        // find already existing field index
        auto [field_id, is_init_var] = m_type->findField(field_name);
        if (!field_id) {
            // try mutating the class first
            field_id = m_type->addField(field_name);
        }
        
        assert(field_id);
        // FIXME: value should be destroyed on exception
        auto value = createMember<LangToolkit>(*fixture, type_id, lang_value);
        // make sure object address is not null
        assert(!(storage_class == StorageClass::OBJECT_REF && value.cast<std::uint64_t>() == 0));
        if (field_id.getIndex() < (*this)->pos_vt().size()) {
            auto &pos_vt = modify().pos_vt();
            unrefMember(*fixture, pos_vt.types()[field_id.getIndex()], pos_vt.values()[field_id.getIndex()]);
            // update attribute stored in the positional value-table
            pos_vt.set(field_id.getIndex(), storage_class, value);
            return;
        }
        
        // try updating field in the index-vt
        unsigned int index_vt_pos;
        if ((*this)->index_vt().find(field_id.getIndex(), index_vt_pos)) {
            auto &index_vt = modify().index_vt();
            unrefMember(*fixture, index_vt.xvalues()[index_vt_pos]);
            index_vt.set(index_vt_pos, storage_class, value);
            return;
        }
        
        // add field to the kv_index
        XValue xvalue(field_id.getIndex(), storage_class, value);
        auto kv_index_ptr = addKV_First(xvalue);
        if (kv_index_ptr) {
            // try updating an existing element first
            XValue old_value;
            if (kv_index_ptr->updateExisting(xvalue, &old_value)) {
                unrefMember(*fixture, old_value);
                // in case of the IttyIndex updating an element changes the address
                // which needs to be updated in the object
                if (kv_index_ptr->getIndexType() == bindex::itty) {
                    modify().m_kv_address = kv_index_ptr->getAddress();
                }
            } else {
                if (kv_index_ptr->insert(xvalue)) {
                    // type or address of the kv-index has changed which needs to be reflected
                    modify().m_kv_address = kv_index_ptr->getAddress();
                    modify().m_kv_type = kv_index_ptr->getIndexType();
                }
            }
        }
    }
    
    bool Object::tryGetMember(const char *field_name, std::pair<StorageClass, Value> &member) const
    {
        /* FIXME:
        if (strcmp(field_name, "__cache__") == 0) {
            if (!initialized()) {
                THROWF(db0::InternalException)
                    << "__cache__ members not allowed in __init__, please place them in __postinit__";
            }
            return bp::object(getMemberCache());
        }
        
        if (strcmp(field_name, "__tags__") == 0) {
            return bp::object(tags());
        }
        */

        if (m_is_dropped) {
            THROWF(db0::InputException) << "Object does not exist";
        }

        auto class_ptr = m_type.get();
        if (!class_ptr) {
            // retrieve class from the initializer
            class_ptr = &m_init_manager.getInitializer(*this).getClass();
        }
        
        assert(class_ptr);
        auto field_index = class_ptr->findField(field_name); 
        
        if (field_index == Class::NField) {
            /* FIXME
            // try pulling from cached members if not found
            return getMemberCacheReference().get(field_name);
            */
           return false;
        }
        
        return tryGetMemberAt(field_index, member);
    }
    
    Object::ObjectSharedPtr Object::tryGet(const char *field_name) const
    {
        std::pair<StorageClass, Value> member;
        if (!tryGetMember(field_name, member)) {
            return nullptr;
        }
        
        auto fixture = this->getFixture();
        return unloadMember<LangToolkit>(fixture, member.first, member.second, field_name);
    }
    
    Object::ObjectSharedPtr Object::tryGetAs(const char *field_name, TypeObjectPtr lang_type) const
    {
        std::pair<StorageClass, Value> member;
        if (!tryGetMember(field_name, member)) {
            return nullptr;
        }
        auto fixture = this->getFixture();
        if (member.first == StorageClass::OBJECT_REF) {
            auto &class_factory = fixture->get<ClassFactory>();
            return PyToolkit::unloadObject(fixture, member.second.cast<std::uint64_t>(), class_factory, lang_type);
        }
        return unloadMember<LangToolkit>(fixture, member.first, member.second, field_name);
    }
    
    Object::ObjectSharedPtr Object::get(const char *field_name) const
    {        
        auto obj = tryGet(field_name);
        if (!obj) {
            THROWF(db0::InputException) << "Attribute not found: " << field_name;
        }
        return obj;
    }
    
    bool Object::tryGetMemberAt(FieldID field_id, bool is_init_var, std::pair<StorageClass, Value> &result) const
    {   
        if (!field_id) {
            if (!is_init_var) {
                return false;
            }
            // report as None even if the field_id has not been assigned yet
            result = { StorageClass::NONE, Value() };
            return true;
        }

        auto index = field_id.getIndex();
        if (!hasInstance()) {
            // try retrieving from initializer
            if (m_init_manager.getInitializer(*this).tryGetAt(index, result)) {
                return true;
            }
            if (!is_init_var) {
                return false;
            }
            // report as None even if the field_id has not been assigned yet
            result = { StorageClass::NONE, Value() };
            return true;
        }
        
        // retrieve from positionally encoded values
        if ((*this)->pos_vt().find(index, result)) {
            return true;
        }

        if ((*this)->index_vt().find(index, result)) {
            return true;
        }

        auto kv_index_ptr = tryGetKV_Index();
        if (kv_index_ptr) {
            XValue xvalue(index);
            if (kv_index_ptr->findOne(xvalue)) {
                // member fetched from the kv_index
                result.first = xvalue.m_type;
                result.second = xvalue.m_value;
                return true;
            }
        }

        if (!is_init_var) {
            return false;
        }
        // report as None even if the field_id has not been assigned yet
        result = { StorageClass::NONE, Value() };
        return true;
    }
    
    db0::swine_ptr<Fixture> Object::tryGetFixture() const
    {
        if (!hasInstance()) {
            // retrieve from the initializer
            return m_init_manager.getInitializer(*this).tryGetFixture();
        }
        return super_t::tryGetFixture();
    }

    db0::swine_ptr<Fixture> Object::getFixture() const
    {
        auto fixture = this->tryGetFixture();
        if (!fixture) {
            THROWF(db0::InternalException) << "Object is no longer accessible";
        }
        return fixture;
    }
    
    Memspace &Object::getMemspace() const {
        return *getFixture();
    }

    void Object::setType(std::shared_ptr<Class> type)
    {
        assert(!m_type);
        m_type = type;
    }

    bool Object::isSingleton() const {
        return getType().isSingleton();
    }
    
    void Object::dropMembers() const
    {
        auto fixture = this->getFixture();
        assert(fixture);
        // drop pos-vt members first
        {
            auto &types = (*this)->pos_vt().types();
            auto &values = (*this)->pos_vt().values();
            auto value = values.begin();
            for (auto type = types.begin(); type != types.end(); ++type, ++value) {
                unrefMember(fixture, *type, *value);
            }
        }
        // drop index-vt members next
        {
            auto &xvalues = (*this)->index_vt().xvalues();
            for (auto &xvalue: xvalues) {
                unrefMember(fixture, xvalue);
            }
        }
        // finally drop kv-index members
        auto kv_index_ptr = tryGetKV_Index();
        if (kv_index_ptr) {
            auto it = kv_index_ptr->beginJoin(1);
            for (;!it.is_end(); ++it) {
                unrefMember(fixture, *it);
            }
        }        
    }
    
    void Object::unSingleton()
    {
        auto &type = getType();
        // drop reference from the class
        if (type.isSingleton()) {
            // clear singleton address
            type.unlinkSingleton();
            modify().m_header.decRef();
        }
    }
    
    void Object::destroy() const
    {
        if (hasInstance()) {
            dropMembers();
            // dereference associated class (may require unloading)
            if (m_type) {
                m_type->decRef();            
            } else {
                std::const_pointer_cast<Class>(unloadType())->decRef();
            }
        }
        super_t::destroy();
    }
    
    FieldLayout Object::getFieldLayout() const
    {
        FieldLayout layout;
        // collect pos-vt information                
        for (auto type: (*this)->pos_vt().types()) {
            layout.m_pos_vt_fields.push_back(type);
        }
        
        // collect index-vt information        
        for (auto &xvalue: (*this)->index_vt().xvalues()) {
            layout.m_index_vt_fields.emplace_back(xvalue.getIndex(), xvalue.m_type);
        }

        // collect kv-index information
        auto kv_index_ptr = tryGetKV_Index();
        if (kv_index_ptr) {
            auto it = kv_index_ptr->beginJoin(1);
            for (;!it.is_end(); ++it) {
                layout.m_kv_index_fields.emplace_back((*it).getIndex(), (*it).m_type);
            }
        }

        return layout;
    }
    
    KV_Index *Object::addKV_First(const XValue &value)
    {
        if (!m_kv_index) {
            if ((*this)->m_kv_address) {
                m_kv_index = std::make_unique<KV_Index>(
                    std::make_pair(&getMemspace(), (*this)->m_kv_address), (*this)->m_kv_type
                );
            } else {
                // create new kv-index intiialized with the first value
                m_kv_index = std::make_unique<KV_Index>(getMemspace(), value);
                modify().m_kv_address = m_kv_index->getAddress();
                modify().m_kv_type = m_kv_index->getIndexType();
                // return nullptr to indicate that the value has been inserted
                return nullptr;
            }
        }
        // return reference without inserting
        return m_kv_index.get();
    }

    const KV_Index *Object::tryGetKV_Index() const
    {
        // if KV index address has changed, update the cached instance
        if (!m_kv_index || m_kv_index->getAddress() != (*this)->m_kv_address) {
            if ((*this)->m_kv_address) {
                m_kv_index = std::make_unique<KV_Index>(
                    std::make_pair(&getMemspace(), (*this)->m_kv_address), (*this)->m_kv_type
                );
            }
        }
    
        return m_kv_index.get();
    }
    
    Class &Object::getType() {
        return m_type ? *m_type : m_init_manager.getInitializer(*this).getClass();
    }
    
    void Object::forAll(std::function<void(const std::string &, const XValue &)> f) const
    {
        // visit pos-vt members first
        auto &obj_type = this->getType();
        {
            auto &types = (*this)->pos_vt().types();
            auto &values = (*this)->pos_vt().values();
            auto value = values.begin();
            unsigned int index = 0;
            for (auto type = types.begin(); type != types.end(); ++type, ++value, ++index) {
                f(obj_type.get(FieldID::fromIndex(index)).m_name, { index, *type, *value });
            }
        }
        // visit index-vt members next
        {
            auto &xvalues = (*this)->index_vt().xvalues();
            for (auto &xvalue: xvalues) {
                f(obj_type.get(FieldID::fromIndex(xvalue.getIndex())).m_name, xvalue);
            }
        }
        // finally visit kv-index members
        auto kv_index_ptr = tryGetKV_Index();
        if (kv_index_ptr) {
            auto it = kv_index_ptr->beginJoin(1);
            for (;!it.is_end(); ++it) {
                f(obj_type.get(FieldID::fromIndex((*it).getIndex())).m_name, *it);
            }
        }
    }
    
    void Object::forAll(std::function<void(const std::string &, ObjectSharedPtr)> f) const
    {
        auto fixture = this->getFixture();
        forAll([&](const std::string &name, const XValue &xvalue) {
            // all references convert to UUID
            auto py_member = unloadMember<LangToolkit>(fixture, xvalue.m_type, xvalue.m_value);
            if (isReference(xvalue.m_type)) {
                f(name, LangToolkit::getUUID(py_member.get()));
            } else {
                f(name, py_member);
            }
        });
    }
    
    void Object::incRef()
    {
        if (hasInstance()) {
            super_t::incRef();            
        } else {
            // incRef with the initializer
            m_init_manager.getInitializer(*this).incRef();
        }
    }
    
    bool Object::operator==(const Object &other) const
    {
        if (!hasInstance() || !other.hasInstance()) {
            // compare objects without DB0 instance
            return this == &other;
        }
        if (this->getFixture()->getUUID() != other.getFixture()->getUUID()) {
            return false;
        }
        return this->getAddress() == other.getAddress();
    }
    
    void Object::moveTo(db0::swine_ptr<Fixture> &) {
        throw std::runtime_error("Not implemented");
    }
    
    void Object::setFixture(db0::swine_ptr<Fixture> &new_fixture)
    {        
        if (hasInstance()) {
            THROWF(db0::InputException) << "set_prefix failed: object already initialized";
        }
        
        if (!m_init_manager.getInitializer(*this).trySetFixture(new_fixture)) {
            // signal problem with PyErr_BadPrefix
            auto fixture = this->getFixture();
            LangToolkit::setError(LangToolkit::getTypeManager().getBadPrefixError(), fixture->getUUID());
        }
    }
    
    void Object::detach() const
    {
        m_type->detach();
        // invalidate since detach is not supported by the MorphingBIndex
        m_kv_index = nullptr;
        super_t::detach();
    }
    
    void Object::commit() const
    {
        m_type->commit();
        if (m_kv_index) {
            m_kv_index->commit();
        }
        super_t::commit();
    }

    void Object::unrefMember(db0::swine_ptr<Fixture> &fixture, StorageClass type, Value value) const {
        db0::object_model::unrefMember<LangToolkit>(fixture, type, value);
    }

    void Object::unrefMember(db0::swine_ptr<Fixture> &fixture, XValue value) const {
        db0::object_model::unrefMember<LangToolkit>(fixture, value.m_type, value.m_value);
    }
    
    std::shared_ptr<Class> Object::unloadType() const
    {
        auto fixture = this->getFixture();
        return fixture->get<ClassFactory>().getTypeByClassRef((*this)->m_class_ref).m_class;
    }
<<<<<<< HEAD
=======
    
    std::uint64_t Object::getAddress() const
    {
        if (!hasInstance()) {
            THROWF(db0::InternalException) << "Object instance does not exist yet (did you forget to use db0.materialized(self) in constructor ?)";
        }
        return super_t::getAddress();
    }
>>>>>>> main

}