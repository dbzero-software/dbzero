// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "Object.hpp"
#include <dbzero/object_model/class.hpp>
#include <dbzero/object_model/value/Member.hpp>

namespace db0::object_model

{
    
    GC0_Define(Object)
    
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
    
    void Object::getFieldLayoutImpl(FieldLayout &layout) const
    {
        super_t::getFieldLayoutImpl(layout);
        // collect kv-index information
        auto kv_index_ptr = tryGetKV_Index();
        if (kv_index_ptr) {
            auto it = kv_index_ptr->beginJoin(1);
            for (;!it.is_end(); ++it) {
                layout.m_kv_index_fields.emplace_back((*it).getIndex(), (*it).m_type);
            }
        }
    }
    
    void Object::getMembersImpl(std::unordered_set<std::string> &result) const
    {
        super_t::getMembersImpl(result);
        // Finally, visit kv-index members
        auto kv_index_ptr = tryGetKV_Index();
        if (kv_index_ptr) {
            auto &obj_type = this->getType();
            auto it = kv_index_ptr->beginJoin(1);
            for (;!it.is_end(); ++it) {
                auto index = (*it).getIndex();
                getMembersFrom(obj_type, index, (*it).m_type, (*it).m_value, result);
            }
        }        
    }
    
    bool Object::tryEqualToImpl(const ObjectImplBase<o_object, Object> &other, bool &result) const
    {
        if (super_t::tryEqualToImpl(other, result)) {
            return true;
        }

        if (this->getFixture()->getUUID() == other.getFixture()->getUUID() 
            && this->getUniqueAddress() == other.getUniqueAddress()) 
        {
            auto &other_obj = static_cast<const Object&>(other);
            if (!hasKV_Index() && !other_obj.hasKV_Index()) {
                result = true;
                return true;
            }
            result = isEqual(this->tryGetKV_Index(), other_obj.tryGetKV_Index());
            return true;
        }
        
        return false;
    }
    
    void Object::unSingleton(FixtureLock &)
    {
        auto &type = getType();
        // drop reference from the class
        if (type.isSingleton()) {
            // clear singleton address
            type.unlinkSingleton();
            this->modify().m_header.decRef(false);
        }
    }

    bool Object::isSingleton() const {
        return getType().isSingleton();
    }

    bool Object::tryFindMemberAt(std::pair<FieldID, unsigned int> field_info, std::pair<StorageClass, Value> &result,
        std::pair<bool, bool> &find_result) const
    {
        if (super_t::tryFindMemberAt(field_info, result, find_result)) {
            return true;
        }

        auto kv_index_ptr = tryGetKV_Index();
        if (kv_index_ptr) {
            auto loc = field_info.first.getIndexAndOffset();
            XValue xvalue(loc.first);
            if (kv_index_ptr->findOne(xvalue)) {
                assert(xvalue.getIndex() == loc.first);
                if (xvalue.m_type == StorageClass::DELETED) {
                    // report as deleted
                    find_result = { false, true };
                    return true;
                }

                // member fetched from the kv_index
                result.first = xvalue.m_type;
                result.second = xvalue.m_value;

                if (field_info.second == 0) {
                    find_result = { result.first != StorageClass::UNDEFINED, false };
                    return true;
                } else {
                    find_result = hasValueAt(result.second, field_info.second, loc.second);
                    return true;
                }
            }
        }
        
        return false;
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
        auto value = createMember<LangToolkit>(
            *fixture, type_id, storage_class, lang_value, this->getMemberFlags()
        );
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
                this->modify().m_kv_address = m_kv_index->getAddress();
                this->modify().m_kv_type = m_kv_index->getIndexType();
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
                    this->modify().m_kv_address = kv_index_ptr->getAddress();
                }
            } else {
                if (kv_index_ptr->insert(xvalue)) {
                    // type or address of the kv-index has changed which needs to be reflected                    
                    this->modify().m_kv_address = kv_index_ptr->getAddress();
                    this->modify().m_kv_type = kv_index_ptr->getIndexType();
                }                                
            }
        }
               
        m_type->addToSchema(field_id, fidelity, getSchemaTypeId(storage_class, value));
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
                    this->modify().m_kv_address = kv_index_ptr->getAddress();
                }
            } else {
                auto old_addr = kv_index_ptr->getAddress();
                kv_index_ptr->erase(xvalue);
                auto new_addr = kv_index_ptr->getAddress();
                if (new_addr != old_addr) {
                    // type or address of the kv-index has changed which needs to be reflected
                    this->modify().m_kv_address = new_addr;
                    this->modify().m_kv_type = kv_index_ptr->getIndexType();
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
                this->modify().m_kv_address = kv_index_ptr->getAddress();                    
            }

            m_type->removeFromSchema(field_id, fidelity, old_type_id);
        }
    }
    
    void Object::dropMembers(db0::swine_ptr<Fixture> &fixture, Class &class_ref) const
    {
        super_t::dropMembers(fixture, class_ref);
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
    
    bool Object::tryUnrefWithLoc(FixtureLock &fixture, FieldID field_id, const void *loc_ptr, unsigned int pos,
        StorageClass storage_class, unsigned int fidelity)
    {
        if (super_t::tryUnrefWithLoc(fixture, field_id, loc_ptr, pos, storage_class, fidelity)) {
            return true;
        }
        unrefKVIndexValue(fixture, field_id, storage_class, fidelity);
        return true;
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
                    this->modify().m_kv_address = kv_index_ptr->getAddress();
                }
            } else {
                if (kv_index_ptr->insert(xvalue)) {
                    // type or address of the kv-index has changed which needs to be reflected                    
                    this->modify().m_kv_address = kv_index_ptr->getAddress();
                    this->modify().m_kv_type = kv_index_ptr->getIndexType();
                }
                
                m_type->addToSchema(field_id, fidelity, getSchemaTypeId(storage_class, value));
            }
        } else {
            m_type->addToSchema(field_id, fidelity, getSchemaTypeId(storage_class, value));
        }
    }
    
    bool Object::tryFindMemberSlot(const std::pair<FieldID, unsigned int> &field_info, unsigned int &pos,
        std::pair<FieldInfo, const void *> &result) const
    {
        if (super_t::tryFindMemberSlot(field_info, pos, result)) {
            return true;
        }

        // kv-index lookup
        auto kv_index_ptr = tryGetKV_Index();
        if (kv_index_ptr) {
            auto [index, offset] = field_info.first.getIndexAndOffset();
            XValue value(index);
            if (kv_index_ptr->findOne(value)) {
                if (field_info.second == 0 || slotExists(value.m_value, field_info.second, offset)) {
                    result = { field_info, nullptr };
                }
                return true;                
            }
        }

        // not found or deleted
        return false;
    }

    void Object::setPosVT(FixtureLock &fixture, FieldID field_id, unsigned int pos, unsigned int fidelity,
        StorageClass storage_class, Value value)
    {     
        auto &pos_vt = this->modify().pos_vt();
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
    
    void Object::setIndexVT(FixtureLock &fixture, FieldID field_id, unsigned int index_vt_pos,
        unsigned int fidelity, StorageClass storage_class, Value value)
    {
        auto &index_vt = this->modify().index_vt();
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

    void Object::addToPosVT(FixtureLock &fixture, FieldID field_id, unsigned int pos, unsigned int fidelity,
        StorageClass storage_class, Value value)
    {
        auto &pos_vt = this->modify().pos_vt();
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

    void Object::addToIndexVT(FixtureLock &fixture, FieldID field_id, unsigned int index_vt_pos,
        unsigned int fidelity, StorageClass storage_class, Value value)
    {
        auto &index_vt = this->modify().index_vt();
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
    
    bool Object::forAllImpl(std::function<bool(const std::string &, const XValue &, unsigned int)> f) const
    {
        if (super_t::forAllImpl(f)) { 
            // Finally, visit kv-index members
            auto kv_index_ptr = tryGetKV_Index();
            if (kv_index_ptr) {
                auto &obj_type = this->getType();
                auto it = kv_index_ptr->beginJoin(1);
                for (;!it.is_end(); ++it) {
                    if ((*it).m_type == StorageClass::DELETED || (*it).m_type == StorageClass::UNDEFINED) {
                        // skip deleted or undefined members
                        continue;
                    }
                    if ((*it).m_type == StorageClass::PACK_2) {
                        // iterate individual lo-fi members
                        if (!forAll(*it, f)) {
                            return false;
                        }
                    } else {
                        if (!f(obj_type.getMember(FieldID::fromIndex((*it).getIndex())).m_name, *it, 0)) {
                            return false;
                        }
                    }
                }
            }
        }
        return true;
    }
    
    void Object::dropInstance(FixtureLock &)
    {
        auto unique_addr = this->getUniqueAddress();
        auto ext_refs = this->getExtRefs();
        this->~Object();
        // construct a null placeholder
        new ((void*)this) Object(tag_as_dropped(), unique_addr, ext_refs);
    }
    
}