// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "ObjectInitializer.hpp"
#include <algorithm>
#include <dbzero/object_model/class.hpp>
#include <dbzero/workspace/Fixture.hpp>

namespace db0::object_model

{
        
    void ObjectInitializer::close() {
        m_manager.closeAt(m_loc);
    }
    
    void ObjectInitializer::reset()
    {
        m_closed = true;
        m_object_ptr = nullptr;        
        m_class = nullptr;        
        m_values.clear();
        m_has_value.clear();
        m_ref_counts = {0, 0};
        m_type_initializer = {};
        m_fixture = {};        
    }
    
    Class &ObjectInitializer::getClass() const {
        return *getClassPtr();
    }
    
    std::shared_ptr<Class> ObjectInitializer::getClassPtr() const
    {
        if (!m_class) {
            assert(m_type_initializer);
            assert(m_fixture);

            if (!m_fixture) {
                THROWF(db0::InternalException)
                    << "ObjectInitializer: Unable to initialize type because Fixture not set" << THROWF_END;
            }
            m_class = m_type_initializer(m_fixture);
            m_type_initializer = {};
            m_fixture = {};
        }
        assert(m_class);
        return m_class;
    }
    
    void ObjectInitializer::operator=(std::uint32_t loc) {
        m_loc = loc;
    }
    
    void ObjectInitializer::set(std::pair<std::uint32_t, std::uint32_t> loc, StorageClass storage_class,
        Value value, std::uint64_t mask) 
    {
        m_values.push_back({ loc.first, storage_class, value }, mask);
        m_has_value.set(loc, true);
    }

    bool ObjectInitializer::remove(std::pair<std::uint32_t, std::uint32_t> loc, std::uint64_t mask) 
    {
        if (!m_has_value.get(loc)) {
            // no value present
            return false;
        }
        m_has_value.set(loc, false);
        return m_values.remove(loc.first, mask);
    }
    
    bool ObjectInitializer::tryGetAt(std::pair<std::uint32_t, std::uint32_t> loc,
        std::pair<StorageClass, Value> &result) const
    {
        if (!m_has_value.get(loc)) {
            // no value present
            return false;
        }
        // retrieve the whole value
        return m_values.tryGetAt(loc.first, result);
    }

    db0::swine_ptr<Fixture> ObjectInitializer::getFixture() const {
        return getClass().getFixture();
    }

    db0::swine_ptr<Fixture> ObjectInitializer::tryGetFixture() const {
        return getClass().tryGetFixture();
    }

    std::pair<const XValue*, const XValue*> ObjectInitializer::getData(PosVT::Data &data, unsigned int &offset)
    {
        return getDataFrom(m_values, data, offset);
    }

    std::pair<const XValue*, const XValue*> ObjectInitializer::getDataFrom(
        XValuesVector &initializationValues, PosVT::Data &data, unsigned int &offset
    ) const
    {
        initializationValues.sortAndMerge();
        if (initializationValues.empty()) {
            // object has no data
            return { nullptr, nullptr };
        }
        
        // offset if the first pos-vt index
        offset = initializationValues.front().getIndex();
        // Divide values into index-encoded and position-encoded (pos-vt)
        // index represents the number of pos-vt elements
        auto index = initializationValues.size();
        auto it = initializationValues.begin() + index - 1;
        // below rule allows pos-vt to be created with the fill-rate of at least 50%
        while (index > 0 && ((it->getIndex() - offset) > ((index - offset) << 1))) {
            --index;
            --it;
        }
        
        // Special rule to include lo-fi slot @pos = 0
        if (offset == 1 && (it->getIndex() < (index + (index >> 1)))) {
            offset = 0;
        }
        
        if (index > 0) {
            auto size = (it->getIndex() - offset) + 1;
            // copy pos-vt elements if such exist
            auto &types = data.m_types;
            auto &values = data.m_values;
            types.reserve(size);
            values.reserve(size);
            for (auto it = initializationValues.begin(), end = initializationValues.begin() + index; it != end; ++it) {
                // fill with undefined elements until reaching the index
                while (types.size() < (it->getIndex() - offset)) {
                    types.push_back(StorageClass::UNDEFINED);
                    values.emplace_back();
                }
                // set the actual value
                types.push_back(it->m_type);
                values.push_back(it->m_value);
            }
            assert(types.size() == size);
        }
        
        auto *begin = initializationValues.data();
        return { begin + index, begin + initializationValues.size() };
    }
    
    void ObjectInitializer::incRef(bool is_tag)
    {
        if (is_tag) {
            if (m_ref_counts.first == std::numeric_limits<std::uint32_t>::max()) {
                THROWF(db0::InternalException) << "ObjectInitializer: ref-count overflow" << THROWF_END;
            }
            ++m_ref_counts.first;
        } else {
            if (m_ref_counts.second == std::numeric_limits<std::uint32_t>::max()) {
                THROWF(db0::InternalException) << "ObjectInitializer: ref-count overflow" << THROWF_END;
            }
            ++m_ref_counts.second;
        }
    }

    bool ObjectInitializer::empty() const {
        return m_values.empty();
    }

    bool ImmutableObjectInitializer::isFixedStorageClass(StorageClass storage_class)
    {
        switch (storage_class) {
            case StorageClass::UNDEFINED:
            case StorageClass::DELETED:
            case StorageClass::NONE:
            case StorageClass::INT64:
            case StorageClass::PTIME64:
            case StorageClass::FP_NUMERIC64:
            case StorageClass::DATE:
            case StorageClass::DATETIME:
            case StorageClass::DATETIME_TZ:
            case StorageClass::TIME:
            case StorageClass::TIME_TZ:
            case StorageClass::DECIMAL:
            case StorageClass::BOOLEAN:
            case StorageClass::PACK_2:
            case StorageClass::PACKED_INT32:
                return true;
            default:
                return false;
        }
    }

    void ImmutableObjectInitializer::setObject(
        std::pair<std::uint32_t, std::uint32_t> loc, StorageClass storage_class, Value value,
        ObjectSharedPtr object, std::uint64_t mask
    )
    {
        set(loc, storage_class, value, mask);
        if (isFixedStorageClass(storage_class)) {
            eraseObjectAt(loc);
            return;
        }

        eraseObjectAt(loc);
        m_objects.push_back({ loc, storage_class, std::move(object) });
    }

    bool ImmutableObjectInitializer::remove(std::pair<std::uint32_t, std::uint32_t> loc, std::uint64_t mask)
    {
        eraseObjectAt(loc);
        return ObjectInitializer::remove(loc, mask);
    }

    bool ImmutableObjectInitializer::tryGetObjectAt(
        std::pair<std::uint32_t, std::uint32_t> loc, ObjectSharedPtr &object
    ) const
    {
        for (const auto &value: m_objects) {
            if (value.m_loc == loc) {
                object = value.m_object;
                return object.get() != nullptr;
            }
        }
        return false;
    }

    std::pair<const XValue*, const XValue*> ImmutableObjectInitializer::getData(
        PosVT::Data &data, unsigned int &offset
    ) const
    {
        m_values.sortAndMerge();
        m_fixed_values.clear();
        m_fixed_values.reserve(m_values.size());
        for (const auto &value: m_values) {
            m_fixed_values.push_back(value);
        }
        for (const auto &value: m_objects) {
            assert(value.m_loc.second == 0 && "Variable-length embedded fields must use default fidelity");
            m_fixed_values.remove(value.m_loc.first);
        }
        return getDataFrom(m_fixed_values, data, offset);
    }

    void ImmutableObjectInitializer::resetObjects()
    {
        m_objects.clear();
        m_fixed_values.clear();
    }

    const std::vector<ImmutableObjectInitializer::ObjectValue> &ImmutableObjectInitializer::objects() const
    {
        return m_objects;
    }

    void ImmutableObjectInitializer::eraseObjectAt(std::pair<std::uint32_t, std::uint32_t> loc)
    {
        m_objects.erase(
            std::remove_if(m_objects.begin(), m_objects.end(), [&](const auto &value) {
                return value.m_loc == loc;
            }),
            m_objects.end()
        );
    }

    bool ObjectInitializer::trySetFixture(db0::swine_ptr<Fixture> &new_fixture)
    {
        assert(new_fixture);
        if (!empty()) {
            THROWF(db0::InputException) << "set_prefix failed: must be called before initializing any object members";
        }

        if (m_fixture && *m_fixture == *new_fixture) {
            // already set to the same fixture
            return true;
        }

        // migrate type to other fixture/ class factory
        if (m_class) {
            auto fixture = m_class->getFixture();
            if (*fixture != *new_fixture) {
                auto &class_factory = getClassFactory(*fixture);
                auto &new_factory = getClassFactory(*new_fixture);
                auto new_class = new_factory.getOrCreateType(class_factory.getLangType(*m_class).get());
                if (new_class->isExistingSingleton()) {
                    // cannot initialize existing singleton, report failure
                    return false;
                }
                m_class = new_class;
            }
        }
        
        m_fixture = new_fixture;
        return true;
    }
    
    void ObjectInitializerManager::closeAt(std::uint32_t loc)
    {
        auto result = m_initializers[loc]->getClassPtr();
        if (auto *initializer = dynamic_cast<ImmutableObjectInitializer *>(m_initializers[loc].get())) {
            initializer->resetObjects();
        }
        m_initializers[loc]->reset();
        // move to inactive slot
        std::swap(m_initializers[loc], m_initializers[m_active_count - 1]);
        *(m_initializers[loc]) = loc;
        *(m_initializers[m_active_count - 1]) = m_active_count - 1;
        --m_active_count;        
    }
    
}
