#include "Schema.hpp"
#include <vector>

namespace db0::object_model

{

    o_type_item::o_type_item(TypeId type_id, std::uint32_t count)
        : m_type_id(type_id)
        , m_count(count)
    {        
    }

    o_type_item::o_type_item(TypeId type_id, int count)
        : m_type_id(type_id)
    {
        assert(count >= 0);
        m_count = static_cast<std::uint32_t>(count);
    }

    bool o_type_item::operator!() const {
        return (m_type_id == TypeId::UNKNOWN || m_count == 0);
    }

    o_type_item &o_type_item::operator=(std::tuple<unsigned int, TypeId, int> item)
    {
        m_type_id = std::get<1>(item);
        assert(std::get<2>(item) >= 0);
        m_count = std::get<2>(item);
        return *this;
    }
    
    o_schema::o_schema(
        Memspace &memspace, std::vector<std::tuple<unsigned int, TypeId, int> >::const_iterator begin,
        std::vector<std::tuple<unsigned int, TypeId, int> >::const_iterator end)
        : m_primary_type_id(std::get<1>(*begin))
    {
        assert(begin != end);
        ++begin;
        // fill-in secondary type ID statistics
        if (begin != end) {
            m_secondary_type = o_type_item(std::get<1>(*begin), std::get<2>(*begin));
            ++begin;
        }
        // fill-in the remaining type IDs
        if (begin != end) {
            std::vector<o_type_item> extra_items;
            while (begin != end) {
                extra_items.emplace_back(std::get<1>(*begin), std::get<2>(*begin));
                ++begin;
            }

            // sort by type ID
            std::sort(extra_items.begin(), extra_items.end());            
            TypeVector type_vector(memspace);
            type_vector.bulkPushBack(extra_items.begin(), extra_items.size());
            m_type_vector_ptr = db0::db0_ptr<TypeVector>(type_vector);
        }
    }
    
    std::pair<o_schema::TypeId, o_schema::TypeId> o_schema::getType() const {
        return { m_primary_type_id, m_secondary_type.m_type_id };
    }

    std::vector<o_schema::TypeId> o_schema::getAllTypes(Memspace &memspace) const 
    {
        std::vector<TypeId> result;
        if (m_primary_type_id == TypeId::UNKNOWN) {
            return result;
        }    
        result.push_back(m_primary_type_id);
        if (!m_secondary_type) {
            return result;
        }
        result.push_back(m_secondary_type.m_type_id);
        if (m_type_vector_ptr) {
            auto type_vector = m_type_vector_ptr(memspace);
            std::vector<o_type_item> extra_items;
            std::copy(type_vector.begin(), type_vector.end(),
                std::back_inserter(extra_items));
            // sort by occurrence count descending
            std::sort(extra_items.begin(), extra_items.end(),
                [](const o_type_item &a, const o_type_item &b) {
                    return a.m_count > b.m_count;
                });
            for (const auto &item : extra_items) {
                result.push_back(item.m_type_id);
            }
        }
        return result;
    }

    void o_schema::update(Memspace &memspace,
        std::vector<std::tuple<unsigned int, TypeId, int> >::const_iterator begin,
        std::vector<std::tuple<unsigned int, TypeId, int> >::const_iterator end,
        const total_func &get_total)
    {
        // NOTE: primary type is not counted
        assert(begin != end);
        // assign secondary type ID and count
        if (!m_secondary_type) {
            // if the secondary type ID is not set, set it to the first one
            m_secondary_type = { std::get<1>(*begin), std::get<2>(*begin) };            
            ++begin;
        }
        
        TypeVector type_vector;
        std::vector<o_type_item> extra_items;
        while (begin != end) {
            if (std::get<1>(*begin) == m_secondary_type.m_type_id) {
                auto diff = std::get<2>(*begin);
                m_secondary_type.m_count += diff;                
                if (m_secondary_type.m_count == 0) {
                    // remove secondary type ID
                    m_secondary_type = o_type_item();
                }
            } else {
                if (!type_vector) {
                    if (m_type_vector_ptr) {
                        type_vector = m_type_vector_ptr(memspace);
                    } else {
                        type_vector = TypeVector(memspace);
                    }
                }
                auto it = type_vector.find(o_type_item{std::get<1>(*begin), 0});
                if (it == type_vector.end()) {
                    // add new type ID
                    extra_items.emplace_back(std::get<1>(*begin), std::get<2>(*begin));                    
                } else {
                    // update existing type ID
                    extra_items.emplace_back(std::get<1>(*begin), it->m_count + std::get<2>(*begin));
                }
                m_total_extra += std::get<2>(*begin);
            }
            ++begin;
        }
        
        if (!extra_items.empty()) {
            assert(type_vector);
            // register the extra items
            type_vector.bulkInsertUnique(extra_items.begin(), extra_items.end(), nullptr);
            // type vector's address might've been changed
            if (type_vector && m_type_vector_ptr.getAddress() != type_vector.getAddress()) {
                m_type_vector_ptr = db0::db0_ptr<TypeVector>(type_vector);
            }
        }

        // try swapping primary / secondary and extra types
        auto primary_count = get_total() - m_secondary_type.m_count - m_total_extra;
        if (m_secondary_type.m_count > primary_count) {
            // swap primary / secondary
            auto temp = m_secondary_type.m_type_id;
            m_secondary_type = o_type_item(m_primary_type_id, primary_count);            
            m_primary_type_id = temp;            
        }
        
        // pruning rule (for swapping extra types)
        if (m_total_extra > m_secondary_type.m_count) {
            // FIXME: implement 
            throw std::runtime_error("Swap extra not implemented yet");
        }
    }

    Schema::Schema(Memspace &memspace, total_func get_total)
        : super_t(memspace)
        , m_get_total(get_total)
    {    
    }
    
    Schema::Schema::Schema(mptr ptr, total_func get_total)
        : super_t(ptr)
        , m_get_total(get_total)
    {    
    }
    
    Schema::~Schema()
    {        
    }

    class Schema::Builder
    {
    public:
        using TypeId = Schema::TypeId;

        Builder(const Schema &schema)
            : m_schema(schema)
            , m_primary_type_cache(schema, true)
            , m_secondary_type_cache(schema, false)
        {
        }
        
        void collect(unsigned int field_id, TypeId type_id, int update)
        {
            assert(update != 0);
            // note: primary types are not counted
            if (type_id != m_primary_type_cache.get(field_id)) {
                // register either with the secondary type updates or with the updates map (for all types)
                if (type_id == m_secondary_type_cache.get(field_id)) {
                    if (field_id >= m_secondary_updates.size()) {
                        m_secondary_updates.resize(field_id + 1, 0);
                    }
                    m_secondary_updates[field_id] += update;
                    // cleanup
                    if (m_secondary_updates[field_id] == 0) {
                        if (std::all_of(m_secondary_updates.begin(), m_secondary_updates.end(), [](int count) { return count == 0; })) {
                            m_secondary_updates.clear();
                        }
                    }
                } else {
                    auto key = std::make_pair(field_id, type_id);
                    auto it = m_updates.find(key);
                    if (it == m_updates.end()) {
                        m_updates[key] = update;
                    } else {
                        it->second += update;
                        if (it->second == 0) {
                            m_updates.erase(it);
                        }
                    }
                }
            }
        }

        bool empty() const
        {
            return m_updates.empty() && 
                std::all_of(m_secondary_updates.begin(), m_secondary_updates.end(), [](int count) { return count == 0; });
        }

        void flush(Schema &schema)
        {
            // collect the updates and flush with the schema
            // field ID, type ID, update count
            std::vector<std::tuple<unsigned int, TypeId, int> > sorted_updates;
            // collect from secondary type updates
            for (unsigned int field_id = 0; field_id < m_secondary_updates.size(); ++field_id) {                
                sorted_updates.emplace_back(field_id, m_secondary_type_cache.get(field_id), m_secondary_updates[field_id]);
            }

            for (const auto &update : m_updates) {
                sorted_updates.emplace_back(update.first.first, update.first.second, update.second);
            }

            std::sort(sorted_updates.begin(), sorted_updates.end(),
                [](const auto &a, const auto &b) {
                    if (std::get<0>(a) != std::get<0>(b)) {
                        return std::get<0>(a) < std::get<0>(b);
                    }
                    // sort by update count descending
                    return std::get<2>(a) > std::get<2>(b);
                }
            );

            auto it_begin = sorted_updates.begin(), end = sorted_updates.end();
            auto it = it_begin;
            while (it != end) {
                auto field_id = std::get<0>(*it_begin);
                while (it != end && std::get<0>(*it) == field_id) {
                    ++it;
                }
                schema.update(field_id, it_begin, it);
                it_begin = it;
            }

            m_secondary_updates.clear();
            m_updates.clear();
            m_primary_type_cache.clear();
            m_secondary_type_cache.clear();
        }

    private:
        const Schema &m_schema;
        // the secondary type ID occurrence count updates
        std::vector<int> m_secondary_updates;        

        struct TypeCache: public std::vector<TypeId>
        {
            using TypeId = Schema::TypeId;
            const Schema &m_schema;
            const bool m_primary;

            TypeCache(const Schema &schema, bool primary)
                : m_schema(schema)
                , m_primary(primary)                
            {
            }

            TypeId get(unsigned int field_id)
            {
                if (field_id >= this->size()) {
                    this->resize(field_id + 1, TypeId::UNKNOWN);
                }
                auto result = (*this)[field_id];
                if (result == TypeId::UNKNOWN && field_id < m_schema.size()) {
                    if (m_primary) {
                        (*this)[field_id] = m_schema[field_id].m_primary_type_id;
                    } else {
                        (*this)[field_id] = m_schema[field_id].m_secondary_type.m_type_id;
                    }
                    result = (*this)[field_id];
                }
                return result;                
            }
        };

        mutable TypeCache m_primary_type_cache;
        mutable TypeCache m_secondary_type_cache;

        struct Hash
        {
            std::size_t operator()(const std::pair<unsigned int, TypeId> &key) const {
                return std::hash<unsigned int>()(key.first) ^ std::hash<TypeId>()(key.second);
            }
        };

        std::unordered_map<std::pair<unsigned int, TypeId>, int, Hash> m_updates;
    };
    
    Schema::Builder &Schema::getBuilder()
    {
        if (!m_builder) {
            m_builder = std::make_unique<Builder>(*this);
        }
        return *m_builder;
    }

    void Schema::add(unsigned int field_id, TypeId type_id) {
        getBuilder().collect(field_id, type_id, 1);
    }

    void Schema::remove(unsigned int field_id, TypeId type_id) {
        getBuilder().collect(field_id, type_id, -1);
    }

    std::pair<Schema::TypeId, Schema::TypeId> Schema::getType(unsigned int field_id) const
    {
        if (m_builder && !m_builder->empty()) {
            m_builder->flush(*const_cast<Schema *>(this));
        }

        if (field_id >= this->size()) {
            THROWF(db0::InputException) << "Unknown / invalid field ID: " << field_id;
        }
        return (*this)[field_id].getType();
    }

    std::vector<Schema::TypeId> Schema::getAllTypes(unsigned int field_id) const
    {
        if (m_builder && !m_builder->empty()) {
            m_builder->flush(*const_cast<Schema *>(this));
        }

        if (field_id >= this->size()) {
            THROWF(db0::InputException) << "Unknown / invalid field ID: " << field_id;
        }
        return (*this)[field_id].getAllTypes(this->getMemspace());
    }

    void Schema::update(unsigned int field_id,
        std::vector<std::tuple<unsigned int, TypeId, int> >::const_iterator begin,
        std::vector<std::tuple<unsigned int, TypeId, int> >::const_iterator end)
    {
        if (field_id >= this->size()) {
            // fill-in empty slots
            while (this->size() < field_id) {
                this->emplace_back();
            }
            // create with values
            this->emplace_back(this->getMemspace(), begin, end);
            assert(this->size() == (field_id + 1));
        } else {
            modifyItem(field_id).update(this->getMemspace(), begin, end, m_get_total);
        }
    }

    void Schema::flush()
    {
        if (m_builder && !m_builder->empty()) {
            m_builder->flush(*this);
        }
        m_builder = nullptr;        
    }

}
