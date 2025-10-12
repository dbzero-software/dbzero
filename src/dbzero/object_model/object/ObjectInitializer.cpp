#include "ObjectInitializer.hpp"
#include <dbzero/object_model/class.hpp>
#include <dbzero/workspace/Fixture.hpp>

namespace db0::object_model

{

    ObjectInitializer::ObjectInitializer(ObjectInitializerManager &manager, std::uint32_t loc, 
        Object &object, std::shared_ptr<Class> db0_class)
        : m_manager(manager)
        , m_loc(loc)
        , m_closed(false)
        , m_object_ptr(&object)
        , m_class(db0_class)
        // NOTE: limit the dim-2 size to improve performance
        , m_has_value(lofi_store<2>::size())
    {        
    }
    
    ObjectInitializer::ObjectInitializer(ObjectInitializerManager &manager, std::uint32_t loc, Object &object,
        TypeInitializer &&type_initializer)
        : m_manager(manager)
        , m_loc(loc)
        , m_closed(false)
        , m_object_ptr(&object)
        // NOTE: limit the dim-2 size to improve performance
        , m_has_value(lofi_store<2>::size())
        , m_type_initializer(std::move(type_initializer))
    {        
    }
    
    void ObjectInitializer::init(Object &object, std::shared_ptr<Class> db0_class)
    {
        assert(m_closed);        
        m_closed = false;
        m_object_ptr = &object;
        m_class = db0_class;
    }
    
    void ObjectInitializer::init(Object &object, TypeInitializer &&type_initializer)
    {
        assert(m_closed);
        assert(!m_class);
        m_closed = false;
        m_object_ptr = &object;
        m_type_initializer = std::move(type_initializer);
    }

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
        m_values.sortAndMerge();
        if (m_values.empty()) {
            // object has no data
            return { &*m_values.begin(), &*m_values.end() };
        }
        
        // offset if the first pos-vt index
        offset = m_values.front().getIndex();
        // Divide values into index-encoded and position-encoded (pos-vt)
        // index represents the number of pos-vt elements
        auto index = m_values.size();
        auto it = m_values.begin() + index - 1;
        // below rule allows pos-vt to be created with the fill-rate of at least 50%
        while (index > 0 && ((it->getIndex() - offset) > (index << 1))) {
            --index;
            --it;
        }
        
        if (index > 0) {
            auto size = (it->getIndex() - offset) + 1;
            // copy pos-vt elements if such exist
            auto &types = data.m_types;
            auto &values = data.m_values;
            types.reserve(size);
            values.reserve(size);
            for (auto it = m_values.begin(), end = m_values.begin() + index; it != end; ++it) {
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
        
        return { &*(m_values.begin() + index), &*(m_values.end()) };
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
    
    std::shared_ptr<Class> ObjectInitializerManager::tryCloseInitializer(const Object &object)
    {        
        for (auto i = 0u; i < m_active_count; ++i) {
            if (m_initializers[i]->operator==(object)) {
                auto result = m_initializers[i]->getClassPtr();
                closeAt(i);
                return result;
            }
        }
        return nullptr;
    }
    
    std::shared_ptr<Class> ObjectInitializerManager::closeInitializer(const Object &object)
    {
        auto result = tryCloseInitializer(object);
        if (result) {
            return result;
        }
        THROWF(db0::InternalException) << "Initializer not found" << THROWF_END;
    }
    
    ObjectInitializer *ObjectInitializerManager::findInitializer(const Object &object) const
    {
        for (auto i = 0u; i < m_active_count; ++i) {
            if (m_initializers[i]->operator==(object)) {
                // move to front to allow faster lookup the next time
                if (i != 0) {
                    std::swap(m_initializers[i], m_initializers[0]);
                    *(m_initializers[i]) = i;
                    *(m_initializers[0]) = 0;
                }
                return m_initializers[0].get();
            }
        }
        return nullptr;   
    }
    
    void ObjectInitializerManager::closeAt(std::uint32_t loc)
    {
        auto result = m_initializers[loc]->getClassPtr();
        m_initializers[loc]->reset();
        // move to inactive slot
        std::swap(m_initializers[loc], m_initializers[m_active_count - 1]);
        *(m_initializers[loc]) = loc;
        *(m_initializers[m_active_count - 1]) = m_active_count - 1;
        --m_active_count;        
    }

}