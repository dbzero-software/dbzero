#include "Schema.hpp"
#include <vector>

namespace db0::object_model

{

    o_schema::o_schema(
        std::vector<std::tuple<unsigned int, TypeId, int> >::const_iterator begin,
        std::vector<std::tuple<unsigned int, TypeId, int> >::const_iterator end)
        : m_primary_type_id(std::get<1>(*begin))
    {
        assert(begin != end);
        ++begin;
        // fill-in secondary type ID statistics
        if (begin != end) {
            m_secondary_type_id = std::get<1>(*begin);
            m_secondary_type_id_count = std::get<2>(*begin);
        }        
        // fill-in the remaining type IDs
        if (begin != end) {
            // FIXME: implement
            throw std::runtime_error("o_schema::o_schema not implemented yet");
        }
    }
    
    std::pair<o_schema::TypeId, o_schema::TypeId> o_schema::getType() const {
        return { m_primary_type_id, m_secondary_type_id };
    }
    
    Schema::Schema(Memspace &memspace)
        : super_t(memspace)
    {    
    }
    
    Schema::Schema::Schema(mptr ptr)
        : super_t(ptr)
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
        
        void add(unsigned int field_id, TypeId type_id)
        {
            // note: primary types are not counted
            if (type_id != m_primary_type_cache.get(field_id)) {
                // register either with the secondary type updates or with the updates map (for all types)
                if (type_id == m_secondary_type_cache.get(field_id)) {
                    if (field_id >= m_secondary_updates.size()) {
                        m_secondary_updates.resize(field_id + 1, 0);
                    }
                    ++(m_secondary_updates[field_id]);
                } else {
                    auto key = std::make_pair(field_id, type_id);
                    auto it = m_updates.find(key);
                    if (it == m_updates.end()) {
                        m_updates[key] = 1;
                    } else {
                        ++(it->second);
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
                    if (std::get<1>(a) != std::get<1>(b)) {
                        return std::get<1>(a) < std::get<1>(b);
                    }
                    return std::get<2>(a) < std::get<2>(b);
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
                        (*this)[field_id] = m_schema[field_id].m_secondary_type_id;
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
        getBuilder().add(field_id, type_id);
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

    void Schema::update(unsigned int field_id,
        std::vector<std::tuple<unsigned int, TypeId, int> >::const_iterator begin,
        std::vector<std::tuple<unsigned int, TypeId, int> >::const_iterator end)
    {
        if (field_id >= this->size()) {
            // fill-in empty slots
            while (field_id + 1 < this->size()) {
                this->emplace_back();
            }
            // create with values
            this->emplace_back(begin, end);
        } else {
            // update existing
            // FIXME: implement
            throw std::runtime_error("Schema::update not implemented yet");
        }
    }

}
