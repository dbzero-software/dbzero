#include "Object.hpp"
#include <random>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/core/serialization/string.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/object_model/class.hpp>
#include <dbzero/object_model/value.hpp>
#include <dbzero/object_model/list/List.hpp>
#include <dbzero/object_model/tags/TagIndex.hpp>
#include <dbzero/core/utils/uuid.hpp>

DEFINE_ENUM_VALUES(db0::object_model::ObjectOptions, "DROPPED", "DEFUNCT")

namespace db0::object_model

{
    
    GC0_Define(Object)
    ObjectInitializerManager Object::m_init_manager;

    FlagSet<AccessOptions> getAccessOptions(const Class &type) {
        return type.isNoCache() ? FlagSet<AccessOptions> { AccessOptions::no_cache } : FlagSet<AccessOptions> {};
    }

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
    
    o_object::o_object(std::uint32_t class_ref, std::pair<std::uint32_t, std::uint32_t> ref_counts,
        std::uint8_t num_type_tags, const PosVT::Data &pos_vt_data, unsigned int pos_vt_offset, 
        const XValue *index_vt_begin, const XValue *index_vt_end)
        : m_header(ref_counts)        
        , m_num_type_tags(num_type_tags)
    {
        arrangeMembers()
            (PosVT::type(), pos_vt_data, pos_vt_offset)
            (packed_int32::type(), class_ref)
            (IndexVT::type(), index_vt_begin, index_vt_end);
    }
    
    std::size_t o_object::measure(std::uint32_t class_ref, std::pair<std::uint32_t, std::uint32_t>, std::uint8_t,
        const PosVT::Data &pos_vt_data, unsigned int pos_vt_offset, 
        const XValue *index_vt_begin, const XValue *index_vt_end)
    {
        return super_t::measureMembers()
            (PosVT::type(), pos_vt_data, pos_vt_offset)
            (packed_int32::type(), class_ref)
            (IndexVT::type(), index_vt_begin, index_vt_end);
    }

    const PosVT &o_object::pos_vt() const {
        return getDynFirst(PosVT::type());
    }

    PosVT &o_object::pos_vt() {
        return getDynFirst(PosVT::type());
    }

    const packed_int32 &o_object::classRef() const {
        return getDynAfter(pos_vt(), packed_int32::type());
    }

    std::uint32_t o_object::getClassRef() const {
        return classRef().value();
    }

    const IndexVT &o_object::index_vt() const {
        return getDynAfter(classRef(), IndexVT::type());
    }
    
    IndexVT &o_object::index_vt() {
        return getDynAfter(classRef(), IndexVT::type());
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

    bool o_object::hasAnyRefs() const {
        return m_header.hasRefs();
    }

    Object::Object(UniqueAddress addr, unsigned int ext_refs)
        : m_flags { ObjectOptions::DROPPED }
        , m_ext_refs(ext_refs)
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
        std::pair<std::uint32_t, std::uint32_t> ref_counts, const PosVT::Data &pos_vt_data, unsigned int pos_vt_offset)        
        : super_t(fixture, type->getClassRef(), ref_counts,
            safeCast<std::uint8_t>(type->getNumBases() + 1, "Too many base classes"), pos_vt_data, pos_vt_offset, nullptr, nullptr,
            getAccessOptions(*type))
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
    
    Object::Object(db0::swine_ptr<Fixture> &fixture, Address address, std::shared_ptr<Class> type_hint, with_type_hint)
        : Object(fixture, address)
    {
        assert(*fixture == *type_hint->getFixture());
        setTypeWithHint(type_hint);
    }
    
    Object::Object(db0::swine_ptr<Fixture> &fixture, ObjectStem &&stem, std::shared_ptr<Class> type_hint, with_type_hint)
        : Object(fixture, std::move(stem), getTypeWithHint(*fixture, stem->getClassRef(), type_hint))
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
    
    void Object::dropInstance(FixtureLock &)
    {
        auto unique_addr = this->getUniqueAddress();
        auto ext_refs = this->getExtRefs();
        this->~Object();
        // construct a null placeholder
        new ((void*)this) Object(unique_addr, ext_refs);
    }
    
    Object::ObjectStem Object::tryUnloadStem(db0::swine_ptr<Fixture> &fixture, Address address, std::uint16_t instance_id)
    {
        std::size_t size_of;
        if (!fixture->isAddressValid(address, REALM_ID, &size_of)) {
            return {};
        }
        // Unload from a verified address
        ObjectVType stem(db0::tag_verified(), fixture->myPtr(address), size_of);
        if (instance_id && stem->m_header.m_instance_id != instance_id) {
            // instance ID validation failed
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
    
    void Object::postInit(FixtureLock &fixture)
    {
        if (!hasInstance()) {
            auto &initializer = m_init_manager.getInitializer(*this);
            PosVT::Data pos_vt_data;
            unsigned int pos_vt_offset = 0;
            auto index_vt_data = initializer.getData(pos_vt_data, pos_vt_offset);
            
            // place object in the same fixture as its class
            // construct the dbzero instance & assign to self
            m_type = initializer.getClassPtr();
            assert(m_type);
            super_t::init(*fixture, m_type->getClassRef(), initializer.getRefCounts(),
                safeCast<std::uint8_t>(m_type->getNumBases() + 1, "Too many base classes"), 
                pos_vt_data, pos_vt_offset, index_vt_data.first, index_vt_data.second,
                getAccessOptions(*m_type)
            );
            
            // reference associated class
            m_type->incRef(false);
            m_type->updateSchema(pos_vt_offset, pos_vt_data.m_types, pos_vt_data.m_values);
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
        // NOTE: allow storage as PACK_2
        auto pre_storage_class = TypeUtils::m_storage_class_mapper.getPreStorageClass(type_id, true);
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
    
    void Object::removePreInit(const char *field_name) const
    {
        auto &initializer = m_init_manager.getInitializer(*this);
        auto &type = initializer.getClass();
        
        // Find an already existing field index
        auto member_id = std::get<0>(type.findField(field_name));
        if (!member_id) {
            THROWF(db0::InputException) << "Attribute not found: " << field_name;
        }
        
        for (const auto &field_info: member_id) {
            assert(field_info.first);            
            auto loc = field_info.first.getIndexAndOffset();
            // mark as deleted
            if (field_info.second == 0) {
                initializer.set(loc, StorageClass::DELETED, {});
            } else {
                assert(field_info.second == 2 && "Only fidelity == 2 is supported");
                if (member_id.hasFidelity(0)) {
                    // remove any existing regular initialization
                    auto loc0 = member_id.get(0).getIndexAndOffset();
                    initializer.remove(loc0);
                }                
                initializer.set(loc, StorageClass::PACK_2, Value::DELETED,
                    lofi_store<2>::mask(loc.second));
            }
        }
    }
    
    void Object::setPreInit(const char *field_name, ObjectPtr obj_ptr) const
    {
        assert(!hasInstance());
        if (!LangToolkit::isValid(obj_ptr)) {
            removePreInit(field_name);
            return;
        }

        auto &initializer = m_init_manager.getInitializer(*this);
        auto fixture = initializer.getFixture();
        auto &type = initializer.getClass();
        auto [type_id, storage_class] = recognizeType(*fixture, obj_ptr);
        auto storage_fidelity = getStorageFidelity(storage_class);
        
        // Find an already existing field index
        auto [member_id, is_init_var] = type.findField(field_name);
        // NOTE: even if a field already exists we might need to extend its supported fidelities
        if (!member_id || !member_id.hasFidelity(storage_fidelity)) {
            // update class definition
            // use the default fidelity for the storage class
            member_id = type.addField(field_name, storage_fidelity);
        }
        
        if (storage_fidelity == 0) {
            if (member_id.hasFidelity(2)) {
                // remove any existing lo-fi initialization
                auto loc = member_id.get(2).getIndexAndOffset();
                initializer.remove(loc, lofi_store<2>::mask(loc.second));
            }
            // register a regular member with the initializer
            // NOTE: a new member receives the no-cache flag if set (at the type level)
            auto member_flags = type.isNoCache() ? AccessFlags { AccessOptions::no_cache } : AccessFlags();
            initializer.set(member_id.get(0).getIndexAndOffset(), storage_class,
                createMember<LangToolkit>(fixture, type_id, storage_class, obj_ptr, member_flags)
            );
        } else {
            if (member_id.hasFidelity(0)) {
                // remove any existing regular initialization
                auto loc = member_id.get(0).getIndexAndOffset();
                initializer.remove(loc);
            }
            // For now only fidelity == 2 is supported (lo-fi storage)
            assert(storage_fidelity == 2);
            auto loc = member_id.get(storage_fidelity).getIndexAndOffset();
            // no access flags for lo-fi members
            auto value = lofi_store<2>::create(loc.second, 
                createMember<LangToolkit>(fixture, type_id, storage_class, obj_ptr, {}).m_store);
            // register a lo-fi member with the initializer (using mask)
            initializer.set(loc, storage_class, value, lofi_store<2>::mask(loc.second));
        }
    }
    
    void Object::remove(FixtureLock &fixture, const char *field_name)
    {
        assert(hasInstance());
        if (this->span() > 1) {
            // NOTE: large objects i.e. with span > 1 must always be marked with a silent mutation flag
            // this is because the actual change may be missed if performed on a different-then the 1st DP
            _touch();
        }
        
        assert(m_type);
        // Find an already existing field index
        auto [member_id, is_init_var] = m_type->findField(field_name);
        if (!member_id) {
            THROWF(db0::InputException) << "Attribute not found: " << field_name;
        }
        
        unsigned int pos = 0;
        auto [field_info, loc_ptr] = tryGetMemberSlot(member_id, pos);
        if (!field_info.first) {
            THROWF(db0::InputException) << "Attribute not found: " << field_name;
        }
        
        // NOTE: unreference as DELETED
        unrefWithLoc(fixture, field_info.first, loc_ptr, pos, StorageClass::DELETED, 
            field_info.second);
        
        // the KV-index erase operation must be registered as the potential silent mutation
        // but the operation can be avoided if the object is already marked as modified
        if (!loc_ptr && !super_t::isModified()) {
            this->_touch();
        }
    }
    
    void Object::unrefPosVT(FixtureLock &fixture, FieldID field_id, unsigned int pos, StorageClass storage_class,
        unsigned int fidelity)
    {
        auto &pos_vt = modify().pos_vt();        
        auto old_storage_class = pos_vt.types()[pos];
        if (fidelity == 0) {
            unrefMember(*fixture, old_storage_class, pos_vt.values()[pos]);
            // mark member as unreferenced by assigning storage class
            pos_vt.set(pos, storage_class, {});
            m_type->removeFromSchema(field_id, fidelity, getSchemaTypeId(old_storage_class));
        } else {
            assert(fidelity == 2);
            auto value = pos_vt.values()[pos];
            auto offset = field_id.getOffset();
            if (storage_class != StorageClass::DELETED && !lofi_store<2>::fromValue(value).isSet(offset)) {
                // value is already unset
                return;
            }

            auto old_type_id = getSchemaTypeId(old_storage_class, lofi_store<2>::fromValue(value).get(offset));
            // either reset or mark as deleted
            if (storage_class == StorageClass::DELETED) {
                lofi_store<2>::fromValue(value).set(offset, Value::DELETED);
            } else {
                lofi_store<2>::fromValue(value).reset(offset);
            }
            pos_vt.set(pos, old_storage_class, value);
            m_type->removeFromSchema(field_id, fidelity, old_type_id);
        }
    }

    void Object::unrefIndexVT(FixtureLock &fixture, FieldID field_id, unsigned int index_vt_pos,
        StorageClass storage_class, unsigned int fidelity)
    {
        auto &index_vt = modify().index_vt();
        auto old_storage_class = index_vt.xvalues()[index_vt_pos].m_type;
        if (fidelity == 0) {
            unrefMember(*fixture, index_vt.xvalues()[index_vt_pos]);
            // mark member as unreferenced by assigning storage class
            index_vt.set(index_vt_pos, storage_class, {});
            m_type->removeFromSchema(field_id, fidelity, getSchemaTypeId(old_storage_class));
        } else {
            assert(fidelity == 2);
            auto value = index_vt.xvalues()[index_vt_pos].m_value;
            auto offset = field_id.getOffset();
            if (storage_class != StorageClass::DELETED && !lofi_store<2>::fromValue(value).isSet(offset)) {
                // value is already unset
                return;
            }
            auto old_type_id = getSchemaTypeId(old_storage_class, lofi_store<2>::fromValue(value).get(offset));
            if (storage_class == StorageClass::DELETED) {
                lofi_store<2>::fromValue(value).set(offset, Value::DELETED);
            } else {
                lofi_store<2>::fromValue(value).reset(offset);
            }            
            index_vt.set(index_vt_pos, old_storage_class, value);
            m_type->removeFromSchema(field_id, fidelity, old_type_id);
        }
    }

    void Object::unrefKVIndexValue(FixtureLock &fixture, FieldID field_id, StorageClass storage_class,
        unsigned int fidelity)
    {
        auto kv_index_ptr = tryGetKV_Index();
        if (!kv_index_ptr) {
            THROWF(db0::InputException) << "Attribute not found";
        }
        XValue xvalue(field_id.getIndex());
        if (!kv_index_ptr->findOne(xvalue)) {        
            THROWF(db0::InputException) << "Attribute not found";
        }
        
        if (fidelity == 0) {
            unrefMember(*fixture, xvalue);
            if (storage_class == StorageClass::DELETED) {
                // mark as deleted in kv-index
                xvalue.m_type = StorageClass::DELETED;
                kv_index_ptr->updateExisting(xvalue);
                // in case of the IttyIndex updating an element changes the address
                // which needs to be updated in the object
                if (kv_index_ptr->getIndexType() == bindex::type::itty) {
                    modify().m_kv_address = kv_index_ptr->getAddress();                    
                }
            } else {
                auto old_addr = kv_index_ptr->getAddress();
                kv_index_ptr->erase(xvalue);
                auto new_addr = kv_index_ptr->getAddress();
                if (new_addr != old_addr) {
                    // type or address of the kv-index has changed which needs to be reflected
                    modify().m_kv_address = new_addr;
                    modify().m_kv_type = kv_index_ptr->getIndexType();
                }
            }
            m_type->removeFromSchema(field_id, fidelity, getSchemaTypeId(xvalue.m_type));
        } else {
            assert(fidelity == 2);
            auto value = xvalue.m_value;
            auto offset = field_id.getOffset();
            if (storage_class != StorageClass::DELETED && !lofi_store<2>::fromValue(value).isSet(offset)) {
                // value is already unset
                return;
            }
            auto old_type_id = getSchemaTypeId(xvalue.m_type, lofi_store<2>::fromValue(value).get(offset));
            if (storage_class == StorageClass::DELETED) {
                lofi_store<2>::fromValue(value).set(offset, Value::DELETED);
            } else {
                lofi_store<2>::fromValue(value).reset(offset);
            }            
            xvalue.m_value = value;
            kv_index_ptr->updateExisting(xvalue);
            // in case of the IttyIndex updating an element changes the address
            // which needs to be updated in the object
            if (kv_index_ptr->getIndexType() == bindex::type::itty) {
                modify().m_kv_address = kv_index_ptr->getAddress();                    
            }

            m_type->removeFromSchema(field_id, fidelity, old_type_id);
        }
    }
    
    void Object::unrefWithLoc(FixtureLock &fixture, FieldID field_id, const void *loc_ptr, unsigned int pos,
        StorageClass storage_class, unsigned int fidelity)
    {
        if (loc_ptr == &(*this)->pos_vt()) {
            unrefPosVT(fixture, field_id, pos, storage_class, fidelity);
        } else if (loc_ptr == &(*this)->index_vt()) {
            unrefIndexVT(fixture, field_id, pos, storage_class, fidelity);
        } else {
            unrefKVIndexValue(fixture, field_id, storage_class, fidelity);
        }
    }

    void Object::setPosVT(FixtureLock &fixture, FieldID field_id, unsigned int pos, unsigned int fidelity,
        StorageClass storage_class, Value value)
    {        
        auto &pos_vt = modify().pos_vt(); 
        auto pos_value = pos_vt.values()[pos];
        if (fidelity == 0) {
            auto old_storage_class = pos_vt.types()[pos];
            unrefMember(*fixture, old_storage_class, pos_value);
            // update attribute stored in the positional value-table
            pos_vt.set(pos, storage_class, value);
            m_type->updateSchema(field_id, fidelity, getSchemaTypeId(old_storage_class), getSchemaTypeId(storage_class));
        } else {
            auto offset = field_id.getOffset();
            auto old_type_id = getSchemaTypeId(storage_class, lofi_store<2>::fromValue(pos_value).get(offset));
            lofi_store<2>::fromValue(pos_value).set(offset, value.m_store);
            pos_vt.set(pos, storage_class, pos_value);
            auto new_type_id = getSchemaTypeId(storage_class, value);
            m_type->updateSchema(field_id, fidelity, old_type_id, new_type_id);
        }
    }

    void Object::addToPosVT(FixtureLock &fixture, FieldID field_id, unsigned int pos, unsigned int fidelity,
        StorageClass storage_class, Value value)
    {
        auto &pos_vt = modify().pos_vt();
        auto pos_value = pos_vt.values()[pos];
        if (fidelity == 0) {
            // update attribute stored in the positional value-table
            pos_vt.set(pos, storage_class, value);
            m_type->addToSchema(field_id, fidelity, getSchemaTypeId(storage_class));
        } else {
            unsigned int offset = field_id.getOffset();
            lofi_store<2>::fromValue(pos_value).set(offset, value.m_store);
            pos_vt.set(pos, storage_class, pos_value);
            m_type->addToSchema(field_id, fidelity, getSchemaTypeId(storage_class, value));
        }
    }
    
    void Object::setIndexVT(FixtureLock &fixture, FieldID field_id, unsigned int index_vt_pos,
        unsigned int fidelity, StorageClass storage_class, Value value)
    {
        auto &index_vt = modify().index_vt();
        if (fidelity == 0) {
            auto old_storage_class = index_vt.xvalues()[index_vt_pos].m_type;
            unrefMember(*fixture, index_vt.xvalues()[index_vt_pos]);
            index_vt.set(index_vt_pos, storage_class, value);
            m_type->updateSchema(field_id, fidelity, getSchemaTypeId(old_storage_class), getSchemaTypeId(storage_class));
        } else {
            auto index_vt_value = index_vt.xvalues()[index_vt_pos].m_value;
            auto offset = field_id.getOffset();
            auto old_type_id = getSchemaTypeId(storage_class, lofi_store<2>::fromValue(index_vt_value).get(offset));
            lofi_store<2>::fromValue(index_vt_value).set(offset, value.m_store);
            index_vt.set(index_vt_pos, storage_class, index_vt_value);
            auto new_type_id = getSchemaTypeId(storage_class, value);
            m_type->updateSchema(field_id, fidelity, old_type_id, new_type_id);
        }
    }

    void Object::addToIndexVT(FixtureLock &fixture, FieldID field_id, unsigned int index_vt_pos,
        unsigned int fidelity, StorageClass storage_class, Value value)
    {
        auto &index_vt = modify().index_vt();
        if (fidelity == 0) {
            index_vt.set(index_vt_pos, storage_class, value);
            m_type->addToSchema(field_id, fidelity, getSchemaTypeId(storage_class));
        } else {
            auto index_vt_value = index_vt.xvalues()[index_vt_pos].m_value;
            lofi_store<2>::fromValue(index_vt_value).set(field_id.getOffset(), value.m_store);
            index_vt.set(index_vt_pos, storage_class, index_vt_value);
            m_type->addToSchema(field_id, fidelity, getSchemaTypeId(storage_class, value));
        }
    }

    void Object::setKVIndexValue(FixtureLock &fixture, FieldID field_id, unsigned int fidelity,
        StorageClass storage_class, Value value)
    {
        assert(m_type);
        XValue xvalue(field_id.getIndex(), storage_class, value);
        // encode for lo-fi storage if needed
        if (fidelity != 0) {
            xvalue.m_value = lofi_store<2>::create(field_id.getOffset(), value.m_store);
        }

        auto kv_index_ptr = addKV_First(xvalue);
        if (kv_index_ptr) {
            // try updating an existing element first
            XValue old_value;
            if (kv_index_ptr->updateExisting(xvalue, &old_value)) {
                if (fidelity == 0) {
                    unrefMember(*fixture, old_value);
                    m_type->updateSchema(field_id, fidelity, getSchemaTypeId(old_value.m_type), 
                        getSchemaTypeId(storage_class)
                    );
                } else {
                    auto kv_value = old_value.m_value;
                    auto offset = field_id.getOffset();
                    auto old_type_id = getSchemaTypeId(old_value.m_type, lofi_store<2>::fromValue(kv_value).get(offset));                    
                    lofi_store<2>::fromValue(kv_value).set(offset, value.m_store);
                    xvalue.m_value = kv_value;
                    kv_index_ptr->updateExisting(xvalue);
                    auto new_type_id = getSchemaTypeId(storage_class, value);
                    m_type->updateSchema(field_id, fidelity, old_type_id, new_type_id);
                }
                // in case of the IttyIndex updating an element changes the address
                // which needs to be updated in the object
                if (kv_index_ptr->getIndexType() == bindex::type::itty) {
                    modify().m_kv_address = kv_index_ptr->getAddress();                    
                }
            } else {
                if (kv_index_ptr->insert(xvalue)) {
                    // type or address of the kv-index has changed which needs to be reflected                    
                    modify().m_kv_address = kv_index_ptr->getAddress();
                    modify().m_kv_type = kv_index_ptr->getIndexType();
                }
                
                m_type->addToSchema(field_id, fidelity, getSchemaTypeId(storage_class, value));
            }
        } else {
            m_type->addToSchema(field_id, fidelity, getSchemaTypeId(storage_class, value));
        }
    }

    void Object::addToKVIndex(FixtureLock &fixture, FieldID field_id, unsigned int fidelity,
        StorageClass storage_class, Value value)
    {
        assert(m_type);
        XValue xvalue(field_id.getIndex(), storage_class, value);
        // encode for lo-fi storage if needed
        if (fidelity != 0) {
            xvalue.m_value = lofi_store<2>::create(field_id.getOffset(), value.m_store);
        }
        auto kv_index_ptr = addKV_First(xvalue);
        if (kv_index_ptr) {
            // NOTE: for fidelity > 0 the element might already exist
            XValue old_value;
            if (fidelity > 0 && kv_index_ptr->updateExisting(xvalue, &old_value)) {
                auto kv_value = old_value.m_value;
                lofi_store<2>::fromValue(kv_value).set(field_id.getOffset(), value.m_store);
                xvalue.m_value = kv_value;
                kv_index_ptr->updateExisting(xvalue);
                // in case of the IttyIndex updating an element changes the address
                // which needs to be updated in the object
                if (kv_index_ptr->getIndexType() == bindex::type::itty) {
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
               
        m_type->addToSchema(field_id, fidelity, getSchemaTypeId(storage_class, value));
    }
    
    std::pair<FieldInfo, const void *> Object::tryGetMemberSlot(const MemberID &member_id,
        unsigned int &pos) const
    {
        for (auto &field_info: member_id) {
            auto [index, offset] = field_info.first.getIndexAndOffset();
            // pos-vt lookup
            if ((*this)->pos_vt().find(index, pos)) {
                if (field_info.second == 0 || slotExists((*this)->pos_vt().values()[pos], field_info.second, offset)) {                    
                    return { field_info, &(*this)->pos_vt() };
                } else {
                    continue;             
                }
            }
            
            // index-vt lookup
            if ((*this)->index_vt().find(index, pos)) {
                if (field_info.second == 0 || slotExists((*this)->index_vt().xvalues()[pos].m_value, field_info.second, offset)) {
                    return { field_info, &(*this)->index_vt() };
                } else {
                    continue;
                }
            }
            
            // kv-index lookup
            auto kv_index_ptr = tryGetKV_Index();
            if (kv_index_ptr) {
                XValue value(index);
                if (kv_index_ptr->findOne(value)) {
                    if (field_info.second == 0 || slotExists(value.m_value, field_info.second, offset)) {
                        return { field_info, nullptr };
                    }
                }
            }
        }
        
        // not found or deleted
        return { {}, nullptr };  
    }
    
    std::pair<const void*, unsigned int> Object::tryGetLoc(FieldID field_id) const
    {
        auto index = field_id.getIndex();
        unsigned int pos = 0;
        // pos-vt lookup
        if ((*this)->pos_vt().find(index, pos)) {
            return { &(*this)->pos_vt(), pos };
        }
        // index-vt lookup
        if ((*this)->index_vt().find(index, pos)) {
            return { &(*this)->index_vt(), pos };
        }
        // not found or located in the kv-index
        return { nullptr, 0 };
    }
    
    void Object::setWithLoc(FixtureLock &fixture, FieldID field_id, const void *loc_ptr, unsigned int pos,
        unsigned int fidelity, StorageClass storage_class, Value value)
    {
        if (loc_ptr == &(*this)->pos_vt()) {
            setPosVT(fixture, field_id, pos, fidelity, storage_class, value);
            return;
        }
        
        if (loc_ptr == &(*this)->index_vt()) {
            setIndexVT(fixture, field_id, pos, fidelity, storage_class, value);
            return;
        }
        
        // must be in the kv-index
        assert(!loc_ptr);
        setKVIndexValue(fixture, field_id, fidelity, storage_class, value);
    }
    
    void Object::addWithLoc(FixtureLock &fixture, FieldID field_id, const void *loc_ptr, unsigned int pos,
        unsigned int fidelity, StorageClass storage_class, Value value)
    {
        if (loc_ptr == &(*this)->pos_vt()) {
            addToPosVT(fixture, field_id, pos, fidelity, storage_class, value);
            return;
        }

        if (loc_ptr == &(*this)->index_vt()) {
            addToIndexVT(fixture, field_id, pos, fidelity, storage_class, value);
            return;
        }

        assert(!loc_ptr);
        addToKVIndex(fixture, field_id, fidelity, storage_class, value);
    }
    
    void Object::set(FixtureLock &fixture, const char *field_name, ObjectPtr lang_value)
    {        
        assert(hasInstance());
        // attribute delete operation
        if (!PyToolkit::isValid(lang_value)) {
            remove(fixture, field_name);
            return;
        }

        auto [type_id, storage_class] = recognizeType(**fixture, lang_value);
        
        if (this->span() > 1) {
            // NOTE: large objects i.e. with span > 1 must always be marked with a silent mutation flag
            // this is because the actual change may be missed if performed on a different-then the 1st DP
            _touch();
        }
        
        assert(m_type);
        // find already existing field index
        auto [member_id, is_init_var] = m_type->findField(field_name);
        auto storage_fidelity = getStorageFidelity(storage_class);
        // get field ID matching the required storage fidelity
        FieldID field_id;
        FieldInfo old_field_info;
        unsigned int old_pos = 0;
        const void *old_loc_ptr = nullptr;
        if (member_id) {
            std::tie(old_field_info, old_loc_ptr) = tryGetMemberSlot(member_id, old_pos);
        }
        
        if (!member_id || !(field_id = member_id.tryGet(storage_fidelity))) {
            // try mutating the class first
            member_id = m_type->addField(field_name, storage_fidelity);
            field_id = member_id.get(storage_fidelity);
        }
        
        assert(field_id && member_id);
        // NOTE: a new member inherits the parent's no-cache flag
        // FIXME: value should be destroyed on exception
        auto value = createMember<LangToolkit>(*fixture, type_id, storage_class, lang_value, getMemberFlags());
        // make sure object address is not null
        assert(!(storage_class == StorageClass::OBJECT_REF && value.cast<std::uint64_t>() == 0));
                
        if (field_id == old_field_info.first) {
            // Set / update value at the existing location
            setWithLoc(fixture, field_id, old_loc_ptr, old_pos, storage_fidelity, storage_class, value);
        } else {
            // must reset / unreference the old value (stored elsewhere)
            if (old_field_info.first) {
                unrefWithLoc(fixture, old_field_info.first, old_loc_ptr, old_pos, StorageClass::UNDEFINED, 
                    old_field_info.second);
            }
            
            const void *loc_ptr = nullptr;
            unsigned int pos = 0;
            // NOTE: slot may already exist (pos-vt or index-vt) either for regular or lo-fi storage
            std::tie(loc_ptr, pos) = tryGetLoc(field_id);            
            // Either use existing slot or create a new (kv-index)
            addWithLoc(fixture, field_id, loc_ptr, pos, storage_fidelity, storage_class, value);
        }
        
        // the KV-index insert operation must be registered as the potential silent mutation
        // but the operation can be avoided if the object is already marked as modified
        if (!super_t::isModified()) {
            this->_touch();
        }        
    }
    
    std::pair<MemberID, bool> Object::findField(const char *name) const
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
    
    FieldID Object::tryGetMember(const char *field_name, std::pair<StorageClass, Value> &member,
        bool &is_init_var) const
    {   
        MemberID member_id;
        std::tie(member_id, is_init_var) = this->findField(field_name);        
        bool exists, deleted = false;
        if (member_id) {
            std::tie(exists, deleted) = tryGetMemberAt(member_id.primary(), member);
            if (exists) {
                assert(!deleted);
                return member_id.primary().first;
            }
            
            // the primary slot was not occupied, try with the secondary
            bool secondary_deleted = false;
            std::tie(exists, secondary_deleted) = tryGetMemberAt(member_id.secondary(), member);
            if (exists) {
                assert(!secondary_deleted);
                return member_id.secondary().first;
            }

            deleted |= secondary_deleted;
        }
        
        if (is_init_var) {
            // unless explicitly deleted, 
            // report as None even if the field_id has not been assigned yet
            member = { deleted ? StorageClass::DELETED : StorageClass::NONE, Value() };
        }

        // member not found
        return {};
    }
    
    std::optional<XValue> Object::tryGetX(const char *field_name) const
    {
        auto [member_id, is_init_var] = this->findField(field_name);
        bool exists, deleted = false;
        if (member_id) {
            assert(member_id.primary().first);
            std::pair<StorageClass, Value> member;
            std::tie(exists, deleted) = tryGetMemberAt(member_id.primary(), member);
            if (exists) {
                assert(!deleted);
                return XValue(member_id.primary().first.getIndex(), member.first, member.second);
            }
            // the primary slot was not occupied, try with the secondary
            bool secondary_deleted = false;
            std::tie(exists, secondary_deleted) = tryGetMemberAt(member_id.secondary(), member);
            if (exists) {
                assert(!secondary_deleted);
                return XValue(member_id.secondary().first.getIndex(), member.first, member.second);
            }
            deleted |= secondary_deleted;
        }

        if (!deleted && is_init_var) {
            // unless explicitly deleted,
            // report as None even if the field_id has not been assigned yet
            return XValue(member_id.primary().first.getIndex(), StorageClass::NONE, Value());
        }
        
        return std::nullopt;
    }
    
    Object::ObjectSharedPtr Object::tryGet(const char *field_name) const
    {
        std::pair<StorageClass, Value> member;
        bool is_init_var = false;
        auto field_id = tryGetMember(field_name, member, is_init_var);
        // NOTE: init vars are always reported as None if not explicitly set nor explicitly deleted
        if (field_id || (is_init_var && member.first != StorageClass::DELETED)) {
            auto fixture = this->getFixture();
            // prevent accessing a deleted or undefined member
            assert(member.first != StorageClass::DELETED && member.first != StorageClass::UNDEFINED);        
            // NOTE: offset is required for lo-fi members
            return unloadMember<LangToolkit>(
                fixture, member.first, member.second, field_id.maybeOffset()
            );
        }
        
        return nullptr;
    }
    
    Object::ObjectSharedPtr Object::tryGetAs(const char *field_name, TypeObjectPtr lang_type) const
    {
        std::pair<StorageClass, Value> member;
        bool is_init_var = false;
        auto field_id = tryGetMember(field_name, member, is_init_var);
        if (field_id || (is_init_var && member.first != StorageClass::DELETED)) {
            // prevent accessing a deleted member
            assert(member.first != StorageClass::DELETED && member.first != StorageClass::UNDEFINED);
            auto fixture = this->getFixture();
            if (member.first == StorageClass::OBJECT_REF) {
                auto &class_factory = getClassFactory(*fixture);
                return PyToolkit::unloadObject(fixture, member.second.asAddress(), class_factory, lang_type);
            }
            
            // NOTE: offset is required for lo-fi members
            return unloadMember<LangToolkit>(
                fixture, member.first, member.second, field_id.getOffset()
            );
        }

        return nullptr;
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

    bool  Object::slotExists(Value value, unsigned int fidelity, unsigned int at) const
    {
        assert(fidelity != 0 && "Operation only available for lo-fi values");
        // lo-fi value
        assert(fidelity == 2);
        return lofi_store<2>::fromValue(value).isSet(at);
    }

    std::pair<bool, bool> Object::hasValueAt(Value value, unsigned int fidelity, unsigned int at) const
    {
        assert(fidelity != 0 && "Operation only available for lo-fi values");
        // lo-fi value
        assert(fidelity == 2);
        if (lofi_store<2>::fromValue(value).isSet(at)) {
            // might be deleted
            bool deleted = (lofi_store<2>::fromValue(value).get(at) == Value::DELETED);
            return { !deleted, deleted };
        } else {
            // NOTE: unset value is assumed as empty / undefined
            return { false, false };
        }
    }
    
    std::pair<bool, bool> Object::tryGetMemberAt(std::pair<FieldID, unsigned int> field_info,
        std::pair<StorageClass, Value> &result) const
    {
        if (!field_info.first) {
            return { false, false };
        }

        auto loc = field_info.first.getIndexAndOffset();
        if (!hasInstance()) {
            // try retrieving from initializer
            auto initializer_ptr = m_init_manager.findInitializer(*this);
            if (!initializer_ptr) {
                return { false, false };
            }
            return { initializer_ptr->tryGetAt(loc, result), false };
        }
        
        // retrieve from positionally encoded values
        if ((*this)->pos_vt().find(loc.first, result)) {
            // NOTE: removed field slots might be marked as DELETED            
            if (result.first == StorageClass::DELETED) {
                // report as deleted
                return { false, true };
            }
            
            if (field_info.second == 0) {
                return { result.first != StorageClass::UNDEFINED, false };
            } else {
                return hasValueAt(result.second, field_info.second, loc.second);
            }
        }
        
        if ((*this)->index_vt().find(loc.first, result)) {
            if (result.first == StorageClass::DELETED) {
                // report as deleted
                return { false, true };
            }
            
            if (field_info.second == 0) {
                return { result.first != StorageClass::UNDEFINED, false };
            } else {
                return hasValueAt(result.second, field_info.second, loc.second);
            }
        }
        
        auto kv_index_ptr = tryGetKV_Index();
        if (kv_index_ptr) {
            XValue xvalue(loc.first);
            if (kv_index_ptr->findOne(xvalue)) {
                assert(xvalue.getIndex() == loc.first);
                if (xvalue.m_type == StorageClass::DELETED) {
                    // report as deleted
                    return { false, true };
                }

                // member fetched from the kv_index
                result.first = xvalue.m_type;
                result.second = xvalue.m_value;

                if (field_info.second == 0) {
                    return { result.first != StorageClass::UNDEFINED, false };
                } else {
                    return hasValueAt(result.second, field_info.second, loc.second);
                }
            }
        }
        
        // Does not exist, not explicitly removed
        return { false, false };
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
        if (type_hint->getClassRef() == (*this)->getClassRef()) {
            m_type = type_hint;
        } else {
            m_type = unloadType();
        }
    }
    
    bool Object::isSingleton() const {
        return getType().isSingleton();
    }
    
    void Object::dropTags(Class &type) const
    {
        // only drop if any type tags are assigned
        if ((*this)->m_header.m_ref_counter.getFirst() > 0) {
            auto fixture = this->getFixture();
            assert(fixture);
            auto &tag_index = fixture->get<TagIndex>();
            const Class *type_ptr = &type;
            auto unique_address = this->getUniqueAddress();
            while (type_ptr) {
                // remove auto-assigned type (or its base) tag
                tag_index.removeTypeTag(unique_address, type_ptr->getAddress());
                // NOTE: no need to decRef since object is being destroyed
                type_ptr = type_ptr->getBaseClassPtr();
            }
        }
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
            unsigned int index = types.offset();
            for (auto type = types.begin(); type != types.end(); ++type, ++value, ++index) {
                if (*type == StorageClass::DELETED || *type == StorageClass::UNDEFINED) {
                    // skip undefined or deleted members
                    continue;
                }
                unrefMember(fixture, *type, *value);
                class_ref.removeFromSchema(index, *type, *value);
            }
        }
        // drop index-vt members next
        {
            auto &xvalues = (*this)->index_vt().xvalues();
            for (auto &xvalue: xvalues) {
                if (xvalue.m_type == StorageClass::DELETED || xvalue.m_type == StorageClass::UNDEFINED) {
                    // skip undefined or deleted members
                    continue;
                }
                unrefMember(fixture, xvalue);
                class_ref.removeFromSchema(xvalue);
            }
        }
        // finally drop kv-index members
        auto kv_index_ptr = tryGetKV_Index();
        if (kv_index_ptr) {
            auto it = kv_index_ptr->beginJoin(1);
            for (;!it.is_end(); ++it) {
                if ((*it).m_type == StorageClass::DELETED || (*it).m_type == StorageClass::UNDEFINED) {
                    // skip undefined or deleted members
                    continue;
                }
                unrefMember(fixture, *it);
                class_ref.removeFromSchema(*it);
            }
        }
    }
    
    void Object::unSingleton(FixtureLock &)
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
            
            dropTags(*type);
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
    
    KV_Index *Object::tryGetKV_Index() const
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

    void Object::getMembersFrom(const Class &this_type, unsigned int index, StorageClass storage_class,
        Value value, std::unordered_set<std::string> &result) const
    {
        if (storage_class == StorageClass::DELETED || storage_class == StorageClass::UNDEFINED) {
            // skip undefined or deleted members
            return;
        }

        if (storage_class == StorageClass::PACK_2) {
            auto it = lofi_store<2>::fromValue(value).begin(), end = lofi_store<2>::fromValue(value).end();
            for (; it != end; ++it) {
                result.insert(this_type.getMember({ index, it.getOffset() }).m_name);
            }
        } else {
            result.insert(this_type.getMember(FieldID::fromIndex(index)).m_name);
        }
    }
    
    std::unordered_set<std::string> Object::getMembers() const
    {
        std::unordered_set<std::string> result;
        // Visit pos-vt members first
        auto &obj_type = this->getType();
        {
            auto &types = (*this)->pos_vt().types();
            auto &values = (*this)->pos_vt().values();
            unsigned int index = types.offset();
            auto size = types.size();
            for (unsigned int pos = 0;pos < size; ++index, ++pos) {
                getMembersFrom(obj_type, index, types[pos], values[pos], result);
            }
        }
        
        // Visit index-vt members next
        {
            auto &xvalues = (*this)->index_vt().xvalues();
            for (auto &xvalue: xvalues) {
                auto index = xvalue.getIndex();
                getMembersFrom(obj_type, index, xvalue.m_type, xvalue.m_value, result);
            }
        }
        
        // Finally, visit kv-index members
        auto kv_index_ptr = tryGetKV_Index();
        if (kv_index_ptr) {
            auto it = kv_index_ptr->beginJoin(1);
            for (;!it.is_end(); ++it) {
                auto index = (*it).getIndex();
                getMembersFrom(obj_type, index, (*it).m_type, (*it).m_value, result);
            }
        }
        return result;
    }
    
    void Object::forAll(std::function<bool(const std::string &, const XValue &, unsigned int)> f) const
    {
        // Visit pos-vt members first
        auto &obj_type = this->getType();
        {
            auto &types = (*this)->pos_vt().types();
            auto &values = (*this)->pos_vt().values();
            auto value = values.begin();
            unsigned int index = types.offset();
            for (auto type = types.begin(); type != types.end(); ++type, ++value, ++index) {
                if (*type == StorageClass::DELETED || *type == StorageClass::UNDEFINED) {
                    // skip deleted or undefined members
                    continue;
                }
                if (*type == StorageClass::PACK_2) {
                    // iterate individual lo-fi members
                    if (!forAll({index, *type, *value}, f)) {
                        return;
                    }
                } else {
                    if (!f(obj_type.getMember(FieldID::fromIndex(index)).m_name, { index, *type, *value }, 0)) {
                        return;
                    }
                }
            }
        }

        // Visit index-vt members next
        {
            auto &xvalues = (*this)->index_vt().xvalues();
            for (auto &xvalue: xvalues) {  
                if (xvalue.m_type == StorageClass::DELETED || xvalue.m_type == StorageClass::UNDEFINED) {
                    // skip deleted or undefined members                    
                    continue;
                }              
                if (xvalue.m_type == StorageClass::PACK_2) {
                    // iterate individual lo-fi members
                    if (!forAll(xvalue, f)) {
                        return;
                    }
                } else {
                    // regular member
                    if (!f(obj_type.getMember(FieldID::fromIndex(xvalue.getIndex())).m_name, xvalue, 0)) {
                        return;
                    }
                }
            }
        }
        
        // Finally, visit kv-index members
        auto kv_index_ptr = tryGetKV_Index();
        if (kv_index_ptr) {
            auto it = kv_index_ptr->beginJoin(1);
            for (;!it.is_end(); ++it) {
                if ((*it).m_type == StorageClass::DELETED || (*it).m_type == StorageClass::UNDEFINED) {
                    // skip deleted or undefined members
                    continue;
                }
                if ((*it).m_type == StorageClass::PACK_2) {
                    // iterate individual lo-fi members
                    if (!forAll(*it, f)) {
                        return;
                    }
                } else {
                    if (!f(obj_type.getMember(FieldID::fromIndex((*it).getIndex())).m_name, *it, 0)) {
                        return;
                    }
                }
            }
        }
    }
    
    void Object::forAll(std::function<bool(const std::string &, ObjectSharedPtr)> f) const
    {
        auto fixture = this->getFixture();
        forAll([&](const std::string &name, const XValue &xvalue, unsigned int offset) -> bool {
            // all references convert to UUID
            auto py_member = unloadMember<LangToolkit>(fixture, xvalue.m_type, xvalue.m_value, offset);
            return f(name, py_member);
        });
    }

    bool Object::forAll(XValue xvalue, std::function<bool(const std::string &, const XValue &, unsigned int offset)> f) const
    {
        assert(xvalue.m_type == StorageClass::PACK_2);
        unsigned int index = xvalue.getIndex();
        auto _value = xvalue.m_value;
        auto it = lofi_store<2>::fromValue(_value).begin(), end = lofi_store<2>::fromValue(_value).end();
        auto &obj_type = this->getType();
        for (; it != end; ++it) {
            if (!f(obj_type.getMember(FieldID::fromIndex(index, it.getOffset())).m_name,
                xvalue, it.getOffset()))
            {
                return false;
            }
        }
        return true;
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
    
    bool Object::decRef(bool is_tag)
    {
        // this operation is a potentially silent mutation
        _touch();
        super_t::decRef(is_tag);
        return !hasRefs();
    }
    
    bool Object::hasRefs() const
    {
        assert(hasInstance());
        return (*this)->hasRefs();
    }
    
    bool Object::hasAnyRefs() const {
        return (*this)->hasAnyRefs();
    }
    
    bool Object::hasTagRefs() const {
        return this->hasInstance() && (*this)->m_header.m_ref_counter.getFirst() > 0;
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

        if ((*this)->getClassRef() != other->getClassRef()) {
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
        this->forAll([&](const std::string &name, const XValue &xvalue, unsigned int offset) -> bool {
            auto maybe_other_value = other.tryGetX(name.c_str());
            if (!maybe_other_value) {
                result = false;
                return false;
            }
            
            if (!xvalue.equalTo(*maybe_other_value, offset)) {
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
        return getClassFactory(*fixture).getTypeByClassRef((*this)->getClassRef()).m_class;
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
            // NOTE: defunct objects don't have a valid address (not assigned yet)
            assert(m_flags[ObjectOptions::DROPPED]);
            return m_unique_address;
        }
    }
    
    bool Object::hasValidClassRef() const
    {
        if (hasInstance() && m_type) {
            return (*this)->getClassRef() == m_type->getClassRef();
        }
        return true;
    }
    
    std::shared_ptr<Class> Object::getTypeWithHint(const Fixture &fixture, std::uint32_t class_ref, std::shared_ptr<Class> type_hint)
    {
        assert(type_hint);
        if (type_hint->getClassRef() == class_ref) {
            return type_hint;
        }
        return getClassFactory(fixture).getTypeByClassRef(class_ref).m_class;
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
    
    void Object::addExtRef() const {
        ++m_ext_refs;
    }

    void Object::removeExtRef() const
    {
        assert(m_ext_refs > 0);
        --m_ext_refs;
    }
    
}