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
    {
        m_values.reserve(128);
    }
    
    ObjectInitializer::ObjectInitializer(ObjectInitializerManager &manager, std::uint32_t loc, Object &object,
        TypeInitializer &&type_initializer)
        : m_manager(manager)
        , m_loc(loc)
        , m_closed(false)
        , m_object_ptr(&object)
        , m_type_initializer(std::move(type_initializer))
    {
        m_values.reserve(128);
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
        m_instance_key = {};        
        m_class = nullptr;        
        m_values.clear();
        m_sorted_size = 0;
        m_ref_count = 0;
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

    void ObjectInitializer::set(unsigned int at, StorageClass storage_class, Value value) {
        m_values.push_back({ at, storage_class, value });
    }
    
    bool ObjectInitializer::tryGetAt(unsigned int index, std::pair<StorageClass, Value> &result) const
    {
        // try locating element within the unsorted items first (starting from the end)
        auto alt_it = m_values.end() - 1;
        auto alt_end = m_values.begin() + m_sorted_size - 1;
        while (alt_it != alt_end) {
            if (alt_it->getIndex() == index) {
                result.first = alt_it->m_type;
                result.second = alt_it->m_value;                
                return true;
            }
            --alt_it;            
        }

        assert(m_sorted_size <= m_values.size());
        // if number of unsorted values exceeds 15 then sort
        if ((m_values.size() - m_sorted_size) > 15) {
            // must use stable-sort to preserve order of equal elements
            std::stable_sort(m_values.begin(), m_values.end());
            m_sorted_size = m_values.size();
        }
        
        // using bisect try locating element by index
        auto it_end = m_values.begin() + m_sorted_size;
        auto it = std::lower_bound(m_values.begin(), it_end, index);
        if (it == it_end || it->getIndex() != index) {
            // element not found
            return false;
        }
        ++it;
        // pick the last from equal elements
        while (it != it_end && it->getIndex() == index) {
            ++it;
        }
        --it;
        result.first = it->m_type;
        result.second = it->m_value;        
        return true;
    }
    
    db0::swine_ptr<Fixture> ObjectInitializer::getFixture() const {
        return getClass().getFixture();
    }

    db0::swine_ptr<Fixture> ObjectInitializer::tryGetFixture() const {
        return getClass().tryGetFixture();
    }
    
    std::uint32_t ObjectInitializer::finalizeValues()
    {
        if (m_values.empty()) {
            return 0;
        }

        // sort all values
        std::stable_sort(m_values.begin(), m_values.end());
        m_sorted_size = m_values.size();
        // remove duplicates by taking the last from equal elements
        auto it_in = m_values.begin(), end = m_values.end();
        auto it_out = m_values.begin();
        ++it_in;
        if (it_in == end) {
            return m_values.size();
        }
        while (it_in != end) {
            if (it_in->getIndex() != it_out->getIndex()) {
                ++it_out;
            }            
            *it_out = *it_in;            
            ++it_in;
        }
        // it_out points to the last valid element
        return it_out - m_values.begin() + 1;
    }
    
    std::pair<const XValue*, const XValue*> ObjectInitializer::getData(PosVT::Data &data)
    {
        auto count = finalizeValues();
        if (count == 0) {
            // object has no data
            return { &*m_values.begin(), &*m_values.end() };
        }

        // divide values into index-encoded and position-encoded
        // index representes the number of pos-vt elements
        auto index = count;
        auto it = m_values.begin() + index - 1;
        // below rule allows pos-vt to be created with at fill rate of at least 50%
        while (index > 0 && (it->getIndex() > (index << 1))) {
            --index;
            --it;
        }
        
        if (index > 0) {
            // copy pos-vt elements if such exist
            auto &types = data.m_types;
            auto &values = data.m_values;
            types.reserve(index);
            values.reserve(index);
            for (auto it = m_values.begin(), end = m_values.begin() + index; it != end; ++it) {
                types.push_back(it->m_type);
                values.push_back(it->m_value);
            }
        }

        return { &*(m_values.begin() + index), &*(m_values.begin() + count) };
    }

    void ObjectInitializer::incRef() {
        ++m_ref_count;
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
                auto &class_factory = fixture->get<ClassFactory>();
                auto &new_factory = new_fixture->get<ClassFactory>();
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