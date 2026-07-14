// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "ObjectImmutableImpl.hpp"

#include <dbzero/bindings/python/embedded/EmbeddedObject.hpp>
#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/object_model/class/Class.hpp>
#include <dbzero/object_model/class/ClassFactory.hpp>
#include <dbzero/object_model/dict/o_py_dict.hpp>
#include <dbzero/object_model/object/ObjectAnyImpl.hpp>
#include <dbzero/object_model/object/ObjectInitializer.hpp>
#include <dbzero/object_model/object/o_embedded_object.hpp>
#include <dbzero/object_model/set/o_py_set.hpp>
#include <dbzero/object_model/tags/TagIndex.hpp>
#include <dbzero/object_model/tuple/o_py_tuple.hpp>
#include <dbzero/object_model/value/Member.hpp>

#include <limits>
#include <unordered_map>

namespace db0::object_model

{
    GC0_Define(ObjectImmutableImpl)

    namespace
    {
        FlagSet<AccessOptions> getAccessOptions(const Class &type)
        {
            return type.isNoCache() ? FlagSet<AccessOptions> { AccessOptions::no_cache } : FlagSet<AccessOptions> {};
        }

        ObjectImmutableImpl::ObjectSharedPtr makeEmbeddedObjectView(
            db0::swine_ptr<Fixture> &fixture, ObjectImmutableImpl::ObjectPtr rootObject,
            const o_embedded_object &embeddedObject
        )
        {
            if (!rootObject) {
                THROWF(db0::InternalException)
                    << "Embedded object retrieval requires an initialized root language object";
            }

            auto &classFactory = fixture->get<ClassFactory>();
            auto type = classFactory.getTypeByClassRef(embeddedObject.getClassRef()).m_class;
            auto memoType = classFactory.getLangType(*type);
            if (memoType.get()) {
                return python::makeEmbeddedMemoObject(rootObject, embeddedObject, std::move(type), memoType.get());
            }
            return python::makeEmbeddedObject(rootObject, embeddedObject, std::move(type));
        }

        std::uint8_t safeNumTypeTags(unsigned int value)
        {
            if (value > std::numeric_limits<std::uint8_t>::max()) {
                THROWF(db0::InputException) << "Too many base classes";
            }
            return static_cast<std::uint8_t>(value);
        }

        void unrefNestedEmbeddedObjects(db0::swine_ptr<Fixture> &fixture, const o_embedded_object &embeddedObject);
        void unrefEmbeddedObject(db0::swine_ptr<Fixture> &fixture, const o_embedded_object &embeddedObject);

        void unrefEmbeddedItem(db0::swine_ptr<Fixture> &fixture, const o_tuple_item &item);

        void unrefEmbeddedTuple(db0::swine_ptr<Fixture> &fixture, const o_py_tuple &tuple)
        {
            for (const auto &item: tuple) {
                unrefEmbeddedItem(fixture, item);
            }
        }

        void unrefEmbeddedSet(db0::swine_ptr<Fixture> &fixture, const o_py_set &set)
        {
            for (const auto &item: set) {
                unrefEmbeddedItem(fixture, item);
            }
        }

        void unrefEmbeddedDict(db0::swine_ptr<Fixture> &fixture, const o_py_dict &dict)
        {
            for (const auto &pair: dict) {
                unrefEmbeddedItem(fixture, pair.key());
                unrefEmbeddedItem(fixture, pair.value());
            }
        }

        void unrefEmbeddedItem(db0::swine_ptr<Fixture> &fixture, const o_tuple_item &item)
        {
            switch (item.itemKind()) {
                case StorageClass::EMBEDDED_OBJECT:
                    unrefEmbeddedObject(fixture, o_embedded_object::__const_ref(item.embeddedPayload().begin()));
                    return;
                case StorageClass::EMBEDDED_TUPLE:
                    unrefEmbeddedTuple(fixture, o_py_tuple::__const_ref(item.embeddedPayload().begin()));
                    return;
                case StorageClass::EMBEDDED_SET:
                    unrefEmbeddedSet(fixture, o_py_set::__const_ref(item.embeddedPayload().begin()));
                    return;
                case StorageClass::EMBEDDED_DICT:
                    unrefEmbeddedDict(fixture, o_py_dict::__const_ref(item.embeddedPayload().begin()));
                    return;
                default:
                    return;
            }
        }

        void unrefEmbeddedObjectTables(db0::swine_ptr<Fixture> &fixture, const o_embedded_object &embeddedObject)
        {
            auto &types = embeddedObject.pos_vt().types();
            auto &values = embeddedObject.pos_vt().values();
            auto value = values.begin();
            for (auto type = types.begin(); type != types.end(); ++type, ++value) {
                if (*type == StorageClass::DELETED || *type == StorageClass::UNDEFINED) {
                    continue;
                }
                unrefMember<LangConfig::LangToolkit>(fixture, *type, *value);
            }

            for (const auto &xvalue: embeddedObject.index_vt().xvalues()) {
                if (xvalue.m_type == StorageClass::DELETED || xvalue.m_type == StorageClass::UNDEFINED) {
                    continue;
                }
                unrefMember<LangConfig::LangToolkit>(fixture, xvalue.m_type, xvalue.m_value);
            }
        }

        void unrefEmbeddedObject(db0::swine_ptr<Fixture> &fixture, const o_embedded_object &embeddedObject)
        {
            unrefEmbeddedObjectTables(fixture, embeddedObject);
            unrefNestedEmbeddedObjects(fixture, embeddedObject);
        }

        void unrefNestedEmbeddedObjects(db0::swine_ptr<Fixture> &fixture, const o_embedded_object &embeddedObject)
        {
            for (const auto &entry: embeddedObject.field_map()) {
                const auto &value = entry.value();
                unrefEmbeddedItem(fixture, value);
            }
        }

        void transformEmbeddedObjectValues(
            db0::swine_ptr<Fixture> &fixture, ObjectImmutableImpl &object, ObjectImmutableImpl::ObjectPtr rootObject,
            const ImmutableObjectInitializer &initializer
        )
        {
            if (!rootObject) {
                return;
            }

            for (const auto &value: initializer.objects()) {
                if (value.m_storage_class == StorageClass::DELETED) {
                    continue;
                }
                assert(value.m_object.get());

                auto *embeddedValue = object->variableValue(value.m_loc.first);
                assert(embeddedValue);

                if (value.m_storage_class == StorageClass::OBJECT_REF
                    || value.m_storage_class == StorageClass::EMBEDDED_OBJECT) {
                    assert(embeddedValue->itemKind() == StorageClass::EMBEDDED_OBJECT);
                    const auto &embeddedObject = o_embedded_object::__const_ref(
                        embeddedValue->embeddedPayload().begin()
                    );
                    db0::python::transformEmbeddedObject(
                        fixture, rootObject, value.m_object.get(), embeddedObject
                    );
                    continue;
                }

                if (value.m_storage_class == StorageClass::DB0_TUPLE || value.m_storage_class == StorageClass::DB0_LIST) {
                    assert(embeddedValue->itemKind() == StorageClass::EMBEDDED_TUPLE);
                    const auto &embeddedTuple = o_py_tuple::__const_ref(embeddedValue->embeddedPayload().begin());
                    db0::python::transformEmbeddedTuple(
                        fixture, rootObject, value.m_object.get(), embeddedTuple
                    );
                    continue;
                }

                if (value.m_storage_class == StorageClass::DB0_SET) {
                    assert(embeddedValue->itemKind() == StorageClass::EMBEDDED_SET);
                    const auto &embeddedSet = o_py_set::__const_ref(embeddedValue->embeddedPayload().begin());
                    db0::python::transformEmbeddedSet(
                        fixture, rootObject, value.m_object.get(), embeddedSet
                    );
                    continue;
                }

                if (value.m_storage_class == StorageClass::DB0_DICT) {
                    assert(embeddedValue->itemKind() == StorageClass::EMBEDDED_DICT);
                    const auto &embeddedDict = o_py_dict::__const_ref(embeddedValue->embeddedPayload().begin());
                    db0::python::transformEmbeddedDict(
                        fixture, rootObject, value.m_object.get(), embeddedDict
                    );
                }
            }
        }

        unsigned int assignDefaultTypeTags(
            db0::swine_ptr<Fixture> &fixture, ObjectImmutableImpl::ObjectPtr object, const Class &type
        )
        {
            auto *classPtr = &type;
            if (!classPtr->assignDefaultTags()) {
                return 0;
            }

            auto &tagIndex = fixture->get<TagIndex>();
            unsigned int result = 0;
            while (classPtr) {
                tagIndex.addTag(object, classPtr->getAddress(), true);
                classPtr = classPtr->getBaseClassPtr();
                ++result;
            }
            return result;
        }

        unsigned int processEmbeddedObjects(
            db0::swine_ptr<Fixture> &fixture, ObjectImmutableImpl &object,
            ObjectImmutableImpl::ObjectPtr rootObject
        )
        {
            const auto &offsetIndex = object->getOffsetIndex();
            if (!rootObject) {
                if (offsetIndex.size() > 0) {
                    THROWF(db0::InternalException)
                        << "Embedded immutable type tag assignment requires an initialized root language object";
                }
                return 0;
            }

            auto &classFactory = fixture->get<ClassFactory>();
            const auto *root = reinterpret_cast<const std::byte *>(object.operator->());
            unsigned int result = 0;
            for (auto offset: offsetIndex) {
                const auto &embeddedObject = o_embedded_object::__const_ref(root + offset);
                auto type = classFactory.getTypeByClassRef(embeddedObject.getClassRef()).m_class;

                auto embeddedObjectView = makeEmbeddedObjectView(fixture, rootObject, embeddedObject);
                result += assignDefaultTypeTags(fixture, embeddedObjectView.get(), *type);
            }
            return result;
        }

    }

    void ObjectImmutableImpl::postInit(FixtureLock &fixture, ObjectPtr lang_object)
    {
        if (lang_object) {
            setLangObject(lang_object);
        }
        if (!this->hasInstance()) {
            auto &initializer = InitManager::instance.getInitializer(*this);
            auto *immutableInitializer = dynamic_cast<ImmutableObjectInitializer *>(&initializer);
            assert(immutableInitializer);

            this->m_type = initializer.getClassPtr();
            assert(this->m_type);

            auto &type = *this->m_type;

            PosVT::Data posVtData;
            unsigned int posVtOffset = 0;
            auto indexVtData = initializer.getData(posVtData, posVtOffset);

            auto numTypeTags = safeNumTypeTags(type.getNumBases() + 1);

            this->init(*fixture, type.getClassRef(), initializer.getRefCounts(), numTypeTags,
                *immutableInitializer, getAccessOptions(type)
            );
            for (const auto &objectValue: immutableInitializer->objects()) {
                type.addToSchema(objectValue.m_loc.first, objectValue.m_storage_class, {});
            }

            type.incRef(false);
            type.updateSchema(posVtOffset, posVtData.m_types, posVtData.m_values);
            type.updateSchema(indexVtData.first, indexVtData.second);

            if (type.isSingleton()) {
                type.setSingletonAddress(*this);
            }
            transformEmbeddedObjectValues(*fixture, *this, m_lang_object, *immutableInitializer);
            if (m_lang_object) {
                this->modify().m_num_type_tags = safeNumTypeTags(
                    (*this)->m_num_type_tags + processEmbeddedObjects(*fixture, *this, m_lang_object)
                );
            }
            initializer.flushTagFields(m_lang_object);
            InitManager::instance.tryCloseInitializer(*this);
        }

        assert(this->hasInstance());
        this->setInitComplete(true);
    }

    void ObjectImmutableImpl::setLangObject(ObjectPtr object) const
    {
        m_lang_object = object;
    }

    ObjectImmutableImpl::ObjectPtr ObjectImmutableImpl::getLangObject() const
    {
        return m_lang_object;
    }

    void ObjectImmutableImpl::destroy()
    {
        m_lang_object = nullptr;
        super_t::destroy();
    }

    void ObjectImmutableImpl::dropInstance(FixtureLock &)
    {
        auto uniqueAddress = this->getUniqueAddress();
        auto extRefs = this->getExtRefs();
        this->destroy();
        this->~ObjectImmutableImpl();
        new ((void *)this) ObjectImmutableImpl(tag_as_dropped(), uniqueAddress, extRefs);
    }

    ObjectImmutableImpl::ObjectSharedPtr ObjectImmutableImpl::tryGet(
        MemberLoc memberLoc, bool *isAutoGenerated
    ) const
    {
        bool baseIsAutoGenerated = false;
        auto result = super_t::tryGet(memberLoc, &baseIsAutoGenerated);
        if (result.get() && !baseIsAutoGenerated) {
            if (isAutoGenerated) {
                *isAutoGenerated = false;
            }
            return result;
        }

        const auto &memberId = memberLoc.first;
        if (!memberId) {
            if (result.get() && isAutoGenerated) {
                *isAutoGenerated = baseIsAutoGenerated;
            }
            return result;
        }

        for (const auto &fieldInfo: memberId) {
            auto object = tryGetEmbeddedField(fieldInfo);
            if (object.get()) {
                if (isAutoGenerated) {
                    *isAutoGenerated = false;
                }
                return object;
            }
        }

        if (result.get() && isAutoGenerated) {
            *isAutoGenerated = baseIsAutoGenerated;
        }
        return result;
    }

    ObjectImmutableImpl::ObjectSharedPtr ObjectImmutableImpl::tryGet(
        const char *fieldName, bool *isAutoGenerated
    ) const
    {
        return tryGet(this->findField(fieldName), isAutoGenerated);
    }

    ObjectImmutableImpl::ObjectSharedPtr ObjectImmutableImpl::getEmbeddedInstanceAtOffset(
        std::uint64_t offset
    ) const
    {
        auto fixture = this->getFixture();
        auto rootObject = getLangObject();

        const auto &embeddedObject = getEmbeddedObjectAtOffset(offset);
        return makeEmbeddedObjectView(fixture, rootObject, embeddedObject);
    }

    const o_embedded_object &ObjectImmutableImpl::getEmbeddedObjectAtOffset(std::uint64_t offset) const
    {
        if (!this->hasInstance() || !(*this)->getOffsetIndex().contains(offset)) {
            THROWF(db0::BadAddressException) << "Invalid embedded immutable object offset: " << offset;
        }

        const auto *root = reinterpret_cast<const std::byte *>(this->operator->());
        return o_embedded_object::__const_ref(root + offset);
    }

    ObjectImmutableImpl::ObjectSharedPtr ObjectImmutableImpl::tryGetEmbeddedField(
        const FieldInfo &fieldInfo
    ) const
    {
        const auto &[fieldId, fidelity] = fieldInfo;
        if (!fieldId || fidelity != 0) {
            return {};
        }

        if (this->hasInstance()) {
            auto *embeddedValue = (*this)->variableValue(fieldId.getIndex());
            if (!embeddedValue) {
                return {};
            }
            auto fixture = this->getFixture();
            auto rootObject = getLangObject();
            LangConfig::LangToolkit::ObjectSharedExtPtr cachedRootObject;
            if (!rootObject) {
                cachedRootObject = fixture->getLangCache().get(this->getAddress());
                rootObject = cachedRootObject.get();
            }
            return python::PyToolkit::unloadEmbeddedInstance(fixture, rootObject, *embeddedValue);
        }

        auto *initializer = dynamic_cast<ImmutableObjectInitializer *>(
            InitManager::instance.findInitializer(*this)
        );
        if (!initializer) {
            return {};
        }

        ObjectSharedPtr object;
        return initializer->tryGetObjectAt(fieldId.getIndexAndOffset(), object) ? object : ObjectSharedPtr();
    }

    ObjectImmutableImpl::ObjectSharedPtr ObjectImmutableImpl::tryGetEmbeddedField(
        db0::swine_ptr<Fixture> &fixture, ObjectPtr rootObject, const o_embedded_object &embeddedObject,
        const FieldInfo &fieldInfo, AccessFlags memberFlags
    )
    {
        const auto &[fieldId, fidelity] = fieldInfo;
        if (!fieldId) {
            return {};
        }

        if (auto fixedValue = embeddedObject.fixedValue(fieldId.getIndex(), fieldId.getOffset())) {
            return unloadMember<LangConfig::LangToolkit>(
                fixture, fixedValue->m_kind, Value(fixedValue->m_value), 0, memberFlags
            );
        }

        if (fidelity != 0) {
            return {};
        }

        auto *embeddedValue = embeddedObject.variableValue(fieldId.getIndex());
        if (!embeddedValue) {
            return {};
        }
        return python::PyToolkit::unloadEmbeddedInstance(fixture, rootObject, *embeddedValue);
    }

    ObjectImmutableImpl::ObjectSharedPtr ObjectImmutableImpl::get(const char *fieldName) const
    {
        auto obj = tryGet(fieldName);
        if (!obj) {
            if (this->isDropped()) {
                THROWF(db0::InputException) << "Object is no longer accessible";
            }
            THROWF(db0::InputException) << "Attribute not found: " << fieldName;
        }
        return obj;
    }

    void ObjectImmutableImpl::getMembersImpl(std::unordered_set<std::string> &result) const
    {
        if (!this->hasInstance()) {
            auto *initializer = dynamic_cast<ImmutableObjectInitializer *>(
                InitManager::instance.findInitializer(*this)
            );
            if (!initializer) {
                return;
            }

            auto &objType = initializer->getClass();
            PosVT::Data posVtData;
            unsigned int posVtOffset = 0;
            auto indexVtData = initializer->getData(posVtData, posVtOffset);

            for (std::size_t pos = 0; pos < posVtData.size(); ++pos) {
                getMembersFrom(
                    objType, static_cast<unsigned int>(pos + posVtOffset), posVtData.m_types[pos],
                    posVtData.m_values[pos], result
                );
            }
            for (auto xvalue = indexVtData.first; xvalue != indexVtData.second; ++xvalue) {
                getMembersFrom(objType, xvalue->getIndex(), xvalue->m_type, xvalue->m_value, result);
            }

            std::unordered_map<std::uint32_t, bool> embeddedObjectMembers;
            for (const auto &objectValue: initializer->objects()) {
                if (objectValue.m_loc.second != 0) {
                    continue;
                }
                if (!objectValue.m_object || objectValue.m_storage_class == StorageClass::DELETED) {
                    embeddedObjectMembers.erase(objectValue.m_loc.first);
                } else {
                    embeddedObjectMembers[objectValue.m_loc.first] = true;
                }
            }
            for (const auto &[index, _]: embeddedObjectMembers) {
                result.insert(objType.getMember(FieldID::fromIndex(index)).m_name);
            }
            return;
        }

        super_t::getMembersImpl(result);
        auto &objType = this->getType();
        for (const auto &entry: (*this)->field_map()) {
            std::uint32_t index = 0;
            if (entry.key().itemKind() == StorageClass::PACKED_INT32) {
                index = entry.key().packedIntPayload().value();
            } else if (entry.key().itemKind() == StorageClass::INT64) {
                index = static_cast<std::uint32_t>(entry.key().intPayload().value());
            } else {
                continue;
            }
            result.insert(objType.getMember(FieldID::fromIndex(index)).m_name);
        }
    }

    void ObjectImmutableImpl::dropMembers(db0::swine_ptr<Fixture> &fixture, Class &classRef) const
    {
        super_t::dropMembers(fixture, classRef);
        unrefEmbeddedObject(fixture, (*this)->getObject());
    }
    
}
