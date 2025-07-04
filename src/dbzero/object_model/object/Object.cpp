#include "Object.hpp"
#include <random>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/core/serialization/string.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/object_model/class.hpp>
#include <dbzero/object_model/value.hpp>
#include <dbzero/object_model/list/List.hpp>
#include <dbzero/core/utils/uuid.hpp>

DEFINE_ENUM_VALUES(db0::object_model::ObjectOptions, "DROPPED", "DEFUNCT")

namespace db0::object_model

{
    
    GC0_Define(Object)
    ObjectInitializerManager Object::m_init_manager;

    bool isEqual(const KV_Index *kv_ptr_1, const KV_Index *kv_ptr_2)
    {
        if (!kv_ptr_1) {
            return !kv_ptr_2;
        }

        if (!kv_ptr_2) {
            return false;
        }

        // item-wise comparison
        return *kv_ptr_1 == *kv_ptr_2;
    }
    
    template <typename IntT> IntT safeCast(unsigned int value, const char *err_msg)
    {
        if (value > std::numeric_limits<IntT>::max()) {
            THROWF(db0::InputException) << err_msg;
        }
        return static_cast<std::uint8_t>(value);
    }

    o_object::o_object(std::uint32_t class_ref, std::pair<std::uint32_t, std::uint32_t> ref_counts, std::uint8_t num_type_tags,
        const PosVT::Data &pos_vt_data, const XValue *index_vt_begin, const XValue *index_vt_end)
        : m_header(ref_counts)
        , m_class_ref(class_ref)        
        , m_num_type_tags(num_type_tags)
    {
        arrangeMembers()
            (PosVT::type(), pos_vt_data)
            (IndexVT::type(), index_vt_begin, index_vt_end);
    }
    
    std::size_t o_object::measure(std::uint32_t, std::pair<std::uint32_t, std::uint32_t>, std::uint8_t, const PosVT::Data &pos_vt_data,
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
    
    void o_object::incRef(bool is_tag) {
        m_header.incRef(is_tag);
    }
    
    bool o_object::hasRefs() const
    {
        // NOTE: type tags are not counted as "proper" references
        if (m_header.m_ref_counter.getFirst() > this->m_num_type_tags) {
            return true;
        }
        return m_header.m_ref_counter.getSecond() > 0;
    }

    Object::Object(UniqueAddress addr)
        : m_flags { ObjectOptions::DROPPED }
        , m_unique_address(addr)
    {
    }

    Object::Object(std::shared_ptr<Class> db0_class)
    {
        // prepare for initialization
        m_init_manager.addInitializer(*this, db0_class);
    }

    Object::Object(TypeInitializer &&type_initializer)
    {
        // prepare for initialization
        m_init_manager.addInitializer(*this, std::move(type_initializer));
    }
    
    Object::Object(db0::swine_ptr<Fixture> &fixture, std::shared_ptr<Class> type, 
        std::pair<std::uint32_t, std::uint32_t> ref_counts, const PosVT::Data &pos_vt_data)
        : super_t(fixture, ClassFactory::classRef(*type), ref_counts, 
            safeCast<std::uint8_t>(type->getNumBases() + 1, "Too many base classes"), pos_vt_data)
        , m_type(type)
    {
    }
    
    Object::Object(db0::swine_ptr<Fixture> &fixture, Address address)
        : super_t(super_t::tag_from_address(), fixture, address)        
    {
    }
    
    Object::Object(db0::swine_ptr<Fixture> &fixture, ObjectStem &&stem, std::shared_ptr<Class> type)
        : super_t(super_t::tag_from_stem(), fixture, std::move(stem))
        , m_type(type)        
    {
        assert(hasValidClassRef());
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
    
    Object *Object::makeNull(void *at_ptr, UniqueAddress addr) {
        return new (at_ptr) Object(addr);
    }
    
    Object::ObjectStem Object::tryUnloadStem(db0::swine_ptr<Fixture> &fixture, Address address, std::uint16_t instance_id)
    {
        std::size_t size_of;
        if (!fixture->isAddressValid(address, &size_of)) {
            return {};
        }
        // Unload from a verified address
        db0::v_object<o_object> stem(db0::tag_verified(), fixture->myPtr(address), size_of);
        if (instance_id && stem->m_header.m_instance_id != instance_id) {
            // instance ID validation failed
            return {};
        }
        // do not unload if reference count is zero
        if (!stem->hasRefs()) {
            return {};
        }
        
        return stem;
    }

    Object::ObjectStem Object::unloadStem(db0::swine_ptr<Fixture> &fixture, Address address, std::uint16_t instance_id)
    {
        auto result = tryUnloadStem(fixture, address, instance_id);
        if (!result) {
            THROWF(db0::InputException) << "Invalid UUID or object has been deleted";
        }
        return result;
    }
    
    Object *Object::unload(void *at_ptr, Address address, std::shared_ptr<Class> type_hint)
    {
        auto fixture = type_hint->getFixture();
        Object *object = new (at_ptr) Object(fixture, address);
        object->setTypeWithHint(type_hint);
        return object;
    }
    
    Object *Object::unload(void *at_ptr, ObjectStem &&stem, std::shared_ptr<Class> type_hint)
    {        
        auto fixture = type_hint->getFixture();
        // placement new
        return new (at_ptr) Object(fixture, std::move(stem), getTypeWithHint(*fixture, stem->m_class_ref, type_hint));
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
            super_t::init(*fixture, ClassFactory::classRef(*m_type), initializer.getRefCounts(),
                safeCast<std::uint8_t>(m_type->getNumBases() + 1, "Too many base classes"), pos_vt_data, 
                index_vt_data.first, index_vt_data.second
            );
            // reference associated class
            m_type->incRef(false);
            m_type->updateSchema(pos_vt_data.m_types);
            m_type->updateSchema(index_vt_data.first, index_vt_data.second);
            
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
        auto pre_storage_class = TypeUtils::m_storage_class_mapper.getPreStorageClass(type_id);
        if (type_id == TypeId::MEMO_OBJECT) {
            // object reference must be from the same fixture
            auto &obj = LangToolkit::getTypeManager().extractObject(lang_value);
            if (fixture.getUUID() != obj.getFixture()->getUUID()) {
                THROWF(db0::InputException) << "Referencing objects from foreign prefixes is not allowed. Use db0.weak_proxy instead";
            }
        }
        
        // may need to refine the storage class (i.e. long weak ref might be needed instead)
        StorageClass storage_class;
        if (pre_storage_class == PreStorageClass::OBJECT_WEAK_REF) {
            storage_class = db0::getStorageClass(pre_storage_class, fixture, lang_value);
        } else {
            storage_class = db0::getStorageClass(pre_storage_class);
        }
        
        return { type_id, storage_class };
    }
    
    void Object::setPreInit(const char *field_name, ObjectPtr obj_ptr) const
    {
        assert(!hasInstance());
        auto &initializer = m_init_manager.getInitializer(*this);
        auto fixture = initializer.getFixture();
        auto &type = initializer.getClass();
        auto [type_id, storage_class] = recognizeType(*fixture, obj_ptr);
        
        // find already existing field index
        auto [field_id, is_init_var] = type.findField(field_name);
        if (!field_id) {
            // update class definition
            field_id = type.addField(field_name);
        }
        
        // register a member with the initializer
        initializer.set(field_id.getIndex(), storage_class, createMember<LangToolkit>(fixture, type_id, storage_class, obj_ptr));
    }
    
    void Object::set(FixtureLock &fixture, const char *field_name, ObjectPtr lang_value)
    {        
        assert(hasInstance());
        auto [type_id, storage_class] = recognizeType(**fixture, lang_value);
        
        if (this->span() > 1) {
            // NOTE: large objects i.e. with span > 1 must always be marked with a silent mutation flag
            // this is because the actual change may be missed if performed on a different-then the 1st DP
            _touch();
        }

        assert(m_type);
        // find already existing field index
        auto [field_id, is_init_var] = m_type->findField(field_name);
        if (!field_id) {
            // try mutating the class first
            field_id = m_type->addField(field_name);
        }
        
        assert(field_id);
        // FIXME: value should be destroyed on exception
        auto value = createMember<LangToolkit>(*fixture, type_id, storage_class, lang_value);
        // make sure object address is not null
        assert(!(storage_class == StorageClass::OBJECT_REF && value.cast<std::uint64_t>() == 0));
        if (field_id.getIndex() < (*this)->pos_vt().size()) {
            auto &pos_vt = modify().pos_vt();
            auto old_storage_class = pos_vt.types()[field_id.getIndex()];    
            unrefMember(*fixture, old_storage_class, pos_vt.values()[field_id.getIndex()]);
            // update attribute stored in the positional value-table
            pos_vt.set(field_id.getIndex(), storage_class, value);
            m_type->updateSchema(field_id, old_storage_class, storage_class);            
            return;
        }
        
        // try updating field in the index-vt
        unsigned int index_vt_pos;
        if ((*this)->index_vt().find(field_id.getIndex(), index_vt_pos)) {
            auto &index_vt = modify().index_vt();
            auto old_storage_class = index_vt.xvalues()[index_vt_pos].m_type;
            unrefMember(*fixture, index_vt.xvalues()[index_vt_pos]);
            index_vt.set(index_vt_pos, storage_class, value);
            m_type->updateSchema(field_id, old_storage_class, storage_class);            
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
                if (kv_index_ptr->getIndexType() == bindex::type::itty) {
                    modify().m_kv_address = kv_index_ptr->getAddress();                    
                }
                m_type->updateSchema(field_id, old_value.m_type, storage_class);
            } else {
                if (kv_index_ptr->insert(xvalue)) {
                    // type or address of the kv-index has changed which needs to be reflected                    
                    modify().m_kv_address = kv_index_ptr->getAddress();
                    modify().m_kv_type = kv_index_ptr->getIndexType();
                }
                                
                m_type->addToSchema(field_id, storage_class);
            }
        } else {            
            m_type->addToSchema(field_id, storage_class);
        }
        
        // the KV-index insert operation must be registered as the potential silent mutation
        // but the operation can be avided if the object is already marked as modified        
        if (!super_t::isModified()) {
            this->_touch();
        }        
    }
    
    std::pair<FieldID, bool> Object::findField(const char *name) const
    {
        if (isDropped()) {
            // defunct objects should not be accessed
            assert(!isDefunct());
            THROWF(db0::InputException) << "Object does not exist";
        }

        auto class_ptr = m_type.get();
        if (!class_ptr) {
            // retrieve class from the initializer
            class_ptr = &m_init_manager.getInitializer(*this).getClass();
        }

        assert(class_ptr);
        return class_ptr->findField(name);
    }
    
    bool Object::tryGetMember(const char *field_name, std::pair<StorageClass, Value> &member) const
    {
        auto [field_id, is_init_var] = this->findField(field_name);
        return tryGetMemberAt(field_id, is_init_var, member);        
    }
    
    std::optional<XValue> Object::tryGetX(const char *field_name) const
    {
        auto [field_id, is_init_var] = this->findField(field_name);
        if (!field_id) {
            return std::nullopt;
        }

        std::pair<StorageClass, Value> member;
        if (!tryGetMemberAt(field_id, is_init_var, member)) {
            return std::nullopt;
        }
        return XValue(field_id.getIndex(), member.first, member.second);
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
            return PyToolkit::unloadObject(fixture, member.second.asAddress(), class_factory, lang_type);
        }
        return unloadMember<LangToolkit>(fixture, member.first, member.second, field_name);
    }
    
    Object::ObjectSharedPtr Object::get(const char *field_name) const
    {
        auto obj = tryGet(field_name);
        if (!obj) {
            if (isDropped()) {
                THROWF(db0::InputException) << "Object is no longer accessible";
            }
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
            auto initializer_ptr = m_init_manager.findInitializer(*this);
            if (!initializer_ptr) {
                return false;
            }
            if (initializer_ptr->tryGetAt(index, result)) {
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
            if (isDropped()) {
                return {};
            }
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
        assert(hasValidClassRef());
    }

    void Object::setTypeWithHint(std::shared_ptr<Class> type_hint)
    {
        assert(!m_type);        
        assert(type_hint);
        assert(hasInstance());
        if (ClassFactory::classRef(*type_hint) == (*this)->m_class_ref) {
            m_type = type_hint;
        } else {
            m_type = unloadType();
        }
    }
    
    bool Object::isSingleton() const {
        return getType().isSingleton();
    }
    
    void Object::dropMembers(Class &class_ref) const
    {
        auto fixture = this->getFixture();
        assert(fixture);        
        // drop pos-vt members first
        {
            auto &types = (*this)->pos_vt().types();
            auto &values = (*this)->pos_vt().values();
            auto value = values.begin();
            unsigned int index = 0;
            for (auto type = types.begin(); type != types.end(); ++type, ++value, ++index) {
                unrefMember(fixture, *type, *value);
                class_ref.removeFromSchema(FieldID::fromIndex(index), *type);
            }
        }
        // drop index-vt members next
        {
            auto &xvalues = (*this)->index_vt().xvalues();
            for (auto &xvalue: xvalues) {
                unrefMember(fixture, xvalue);
                class_ref.removeFromSchema(xvalue);
            }
        }
        // finally drop kv-index members
        auto kv_index_ptr = tryGetKV_Index();
        if (kv_index_ptr) {
            auto it = kv_index_ptr->beginJoin(1);
            for (;!it.is_end(); ++it) {
                unrefMember(fixture, *it);
                class_ref.removeFromSchema(*it);
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
            modify().m_header.decRef(false);
        }
    }
    
    void Object::destroy() const
    {
        if (hasInstance()) {
            // associated class type (may require unloading)
            auto type = m_type;
            if (!type) {
                // retrieve type from the initializer
                type = std::const_pointer_cast<Class>(unloadType());
            }
            dropMembers(*type);
            // dereference associated class
            type->decRef(false);
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

    bool Object::hasKV_Index() const {
        return m_kv_index || (*this)->m_kv_address;
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
    
    std::unordered_set<std::string> Object::getMembers() const 
    {
        std::unordered_set<std::string> result;
        // visit pos-vt members first
        auto &obj_type = this->getType();
        {
            for (unsigned int index = 0;index < (*this)->pos_vt().size(); ++index) {
                result.insert(obj_type.get(FieldID::fromIndex(index)).m_name);
            }
        }
        
        // visit index-vt members next
        {
            auto &xvalues = (*this)->index_vt().xvalues();
            for (auto &xvalue: xvalues) {
                result.insert(obj_type.get(FieldID::fromIndex(xvalue.getIndex())).m_name);
            }
        }
        // finally visit kv-index members
        auto kv_index_ptr = tryGetKV_Index();
        if (kv_index_ptr) {
            auto it = kv_index_ptr->beginJoin(1);
            for (;!it.is_end(); ++it) {
                result.insert(obj_type.get(FieldID::fromIndex((*it).getIndex())).m_name);
            }
        }
        return result;
    }
    
    void Object::forAll(std::function<bool(const std::string &, const XValue &)> f) const
    {
        // visit pos-vt members first
        auto &obj_type = this->getType();
        {
            auto &types = (*this)->pos_vt().types();
            auto &values = (*this)->pos_vt().values();
            auto value = values.begin();
            unsigned int index = 0;
            for (auto type = types.begin(); type != types.end(); ++type, ++value, ++index) {
                if (!f(obj_type.get(FieldID::fromIndex(index)).m_name, { index, *type, *value })) {
                    return;
                }
            }
        }
        // visit index-vt members next
        {
            auto &xvalues = (*this)->index_vt().xvalues();
            for (auto &xvalue: xvalues) {
                if (!f(obj_type.get(FieldID::fromIndex(xvalue.getIndex())).m_name, xvalue)) {
                    return;
                }
            }
        }
        // finally visit kv-index members
        auto kv_index_ptr = tryGetKV_Index();
        if (kv_index_ptr) {
            auto it = kv_index_ptr->beginJoin(1);
            for (;!it.is_end(); ++it) {
                if (!f(obj_type.get(FieldID::fromIndex((*it).getIndex())).m_name, *it)) {
                    return;
                }
            }
        }
    }
    
    void Object::forAll(std::function<bool(const std::string &, ObjectSharedPtr)> f) const
    {
        auto fixture = this->getFixture();
        forAll([&](const std::string &name, const XValue &xvalue) -> bool {
            // all references convert to UUID
            auto py_member = unloadMember<LangToolkit>(fixture, xvalue.m_type, xvalue.m_value);
            if (isReference(xvalue.m_type)) {
                return f(name, LangToolkit::getUUID(py_member.get()));
            } else {
                return f(name, py_member);
            }
        });
    }
    
    void Object::incRef(bool is_tag)
    {
        if (hasInstance()) {
            super_t::incRef(is_tag);
        } else {
            // incRef with the initializer
            m_init_manager.getInitializer(*this).incRef(is_tag);
        }
    }
    
    std::uint32_t Object::decRef(bool is_tag)
    {
        // this operation is a potentially silent mutation
        _touch();        
        return super_t::decRef(is_tag);
    }
    
    bool Object::hasRefs() const
    {
        assert(hasInstance());
        return (*this)->hasRefs();
    }
    
    bool Object::equalTo(const Object &other) const
    {
        if (!hasInstance() || !other.hasInstance()) {
            THROWF(db0::InputException) << "Object not initialized";
        }
        
        if (this->isDefunct() || other.isDefunct()) {
            // defunct objects should not be compared
            assert(!isDefunct());
            THROWF(db0::InputException) << "Object does not exist";
        }

        if ((*this)->m_class_ref != other->m_class_ref) {
            // different types
            return false;
        }

        if (this->getFixture()->getUUID() == other.getFixture()->getUUID() 
            && this->getUniqueAddress() == other.getUniqueAddress()) 
        {
            // comparing 2 versions of the same object (fastest)
            if (!((*this)->pos_vt() == other->pos_vt())) {
                return false;
            }
            if (!((*this)->index_vt() == other->index_vt())) {
                return false;
            }
            if (!hasKV_Index() && !other.hasKV_Index()) {
                return true;
            }
            return isEqual(this->tryGetKV_Index(), other.tryGetKV_Index());
        }
        
        // field-wise compare otherwise (slower)
        bool result = true;
        this->forAll([&](const std::string &name, const XValue &xvalue) -> bool {
            auto maybe_other_value = other.tryGetX(name.c_str());
            if (!maybe_other_value) {
                result = false;
                return false;
            }
            
            if (!xvalue.equalTo(*maybe_other_value)) {
                result = false;
                return false;
            }
            return true;
        });
        return result;
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
        // reset the silent-mutation flag
        m_touched = false;
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
    
    Address Object::getAddress() const
    {
        assert(!isDefunct());
        if (!hasInstance()) {            
            THROWF(db0::InternalException) << "Object instance does not exist yet (did you forget to use db0.materialized(self) in constructor ?)";
        }
        return super_t::getAddress();
    }
    
    UniqueAddress Object::getUniqueAddress() const
    {
        if (hasInstance()) {
            return super_t::getUniqueAddress();
        } else {
            assert(m_flags[ObjectOptions::DROPPED]);
            return m_unique_address;
        }
    }
    
    bool Object::hasValidClassRef() const
    {
        if (hasInstance() && m_type) {            
            return (*this)->m_class_ref == ClassFactory::classRef(*m_type);
        }
        return true;
    }
    
    std::shared_ptr<Class> Object::getTypeWithHint(const Fixture &fixture, std::uint32_t class_ref, std::shared_ptr<Class> type_hint)
    {
        assert(type_hint);
        if (ClassFactory::classRef(*type_hint) == class_ref) {
            return type_hint;
        }
        return fixture.get<ClassFactory>().getTypeByClassRef(class_ref).m_class;
    }
    
    void Object::setDefunct() const {
        m_flags.set(ObjectOptions::DEFUNCT);
    }
    
    void Object::touch()
    {
        if (hasInstance() && !isDefunct()) {
            // NOTE: for already modified and small objects we may skip "touch"
            if (!super_t::isModified() || this->span() > 1) {
                // NOTE: large objects i.e. with span > 1 must always be marked with a silent mutation flag
                // this is because the actual change may be missed if performed on a different-then the 1st DP
                _touch();
            }            
        }
    }
    
    void Object::_touch()
    {
        if (!m_touched) {
            // mark the 1st byte of the object as modified (forced-diff)
            // this is always the 1st DP occupied by the object
            modify(0, 1);
            m_touched = true;
        }
    }

}