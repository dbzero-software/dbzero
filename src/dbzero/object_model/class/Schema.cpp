#include "Schema.hpp"
#include <vector>
#include <dbzero/object_model/value/TypeUtils.hpp>

namespace db0::object_model

{

    o_type_item::o_type_item(SchemaTypeId type_id, std::uint32_t count)
        : m_type_id(type_id)
        , m_count(count)
    {        
    }

    o_type_item::o_type_item(SchemaTypeId type_id, int count)
        : m_type_id(type_id)
        , m_count(static_cast<std::uint32_t>(count))
    {
        assert(count >= 0);        
    }

    bool o_type_item::operator!() const {
        return (m_type_id == SchemaTypeId::UNDEFINED || m_count == 0);
    }

    o_type_item &o_type_item::operator=(std::tuple<unsigned int, SchemaTypeId, int> item)
    {
        m_type_id = std::get<1>(item);
        assert(std::get<2>(item) >= 0);
        m_count = std::get<2>(item);
        return *this;
    }
    
    o_schema::o_schema(
        Memspace &memspace, std::vector<std::tuple<unsigned int, SchemaTypeId, int> >::const_iterator begin,
        std::vector<std::tuple<unsigned int, SchemaTypeId, int> >::const_iterator end)
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
    
    std::pair<SchemaTypeId, SchemaTypeId> o_schema::getType() const {
        return { m_primary_type_id, m_secondary_type.m_type_id };
    }

    std::vector<SchemaTypeId> o_schema::getAllTypes(Memspace &memspace) const
    {
        std::vector<SchemaTypeId> result;
        if (m_primary_type_id == SchemaTypeId::UNDEFINED) {
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
        std::vector<std::tuple<unsigned int, SchemaTypeId, int> >::const_iterator begin,
        std::vector<std::tuple<unsigned int, SchemaTypeId, int> >::const_iterator end,
        std::uint32_t collection_size)
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

        // Reorder the primary, secondary and possibly extra types
        update(memspace, type_vector, collection_size);
    }

    void o_schema::update(Memspace &memspace, std::uint32_t collection_size)
    {
        TypeVector type_vector;
        update(memspace, type_vector, collection_size);
    }

    void o_schema::update(Memspace &memspace, TypeVector &type_vector, std::uint32_t collection_size)
    {
        // try swapping primary / secondary and extra types
        auto primary_count = collection_size - m_secondary_type.m_count - m_total_extra;
        if (m_secondary_type.m_count > primary_count) {
            // swap primary / secondary
            auto temp = m_secondary_type.m_type_id;
            auto temp_count = m_secondary_type.m_count;
            if (primary_count > 0) {
                m_secondary_type = o_type_item(m_primary_type_id, primary_count);            
            } else {
                m_secondary_type = o_type_item();
            }
            m_primary_type_id = temp;
            primary_count = temp_count;
        }

        // pruning rule (for swapping extra types)
        if (m_total_extra <= m_secondary_type.m_count || !m_type_vector_ptr) {
            return;
        }

        if (!type_vector) {
            type_vector = m_type_vector_ptr(memspace);
        }
        
        o_type_item max_item, second_max_item;
        for (auto &type_item : type_vector) {
            if (type_item.m_count > second_max_item.m_count) {
                second_max_item = type_item;
            }
            if (second_max_item.m_count > max_item.m_count) {
                std::swap(max_item, second_max_item);
            }
        }
        
        // swap primary type ID
        bool addr_changed = false;
        if (max_item.m_count > primary_count) {
            type_vector.erase(max_item, addr_changed);
            if (primary_count) {
                if (!!m_secondary_type) {
                    type_vector.insert(m_secondary_type, addr_changed);
                }
                m_secondary_type = o_type_item(m_primary_type_id, primary_count);                
            }
            m_primary_type_id = max_item.m_type_id;
            max_item = second_max_item;
        }

        // swap secondary type ID
        if (max_item.m_count > m_secondary_type.m_count) {            
            type_vector.erase(max_item, addr_changed);
            if (!!m_secondary_type) {
                type_vector.insert(m_secondary_type, addr_changed);
            }
            m_secondary_type = max_item;
        }
        if (addr_changed) {
            m_type_vector_ptr = db0::db0_ptr<TypeVector>(type_vector);
        }
    }
    
    SchemaTypeId o_schema::getPrimaryType(std::uint32_t collection_size) const
    {
        auto primary_count = collection_size - m_secondary_type.m_count - m_total_extra;
        if (primary_count >= m_secondary_type.m_count) {
            return m_primary_type_id;
        } else {            
            return m_secondary_type.m_type_id;
        }    
    }

    class Schema::Builder
    {
    public:
        Builder(Schema &schema)
            : m_schema(schema)
            , m_primary_type_cache(schema, true)
            , m_secondary_type_cache(schema, false)
        {
        }
        
        void collect(unsigned int field_id, SchemaTypeId type_id, int update)
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

        void flush(std::uint32_t collection_size)
        {
            // collect the updates and flush with the schema
            // field ID, type ID, update count
            std::vector<std::tuple<unsigned int, SchemaTypeId, int> > sorted_updates;
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
                m_schema.update(field_id, it_begin, it, collection_size);
                it_begin = it;
            }
            
            m_secondary_updates.clear();
            m_updates.clear();
            m_primary_type_cache.clear();
            m_secondary_type_cache.clear();
        }

    private:
        Schema &m_schema;
        // the secondary type ID occurrence count updates
        std::vector<int> m_secondary_updates;     

        struct TypeCache: public std::vector<SchemaTypeId>
        {            
            const Schema &m_schema;
            const bool m_primary;

            TypeCache(const Schema &schema, bool primary)
                : m_schema(schema)
                , m_primary(primary)                
            {
            }

            SchemaTypeId get(unsigned int field_id)
            {
                if (field_id >= this->size()) {
                    this->resize(field_id + 1, SchemaTypeId::UNDEFINED);
                }
                auto result = (*this)[field_id];
                if (result == SchemaTypeId::UNDEFINED && field_id < m_schema.size()) {
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
            std::size_t operator()(const std::pair<unsigned int, SchemaTypeId> &key) const {
                return std::hash<unsigned int>()(key.first) ^ std::hash<SchemaTypeId>()(key.second);
            }
        };

        std::unordered_map<std::pair<unsigned int, SchemaTypeId>, int, Hash> m_updates;
    };

    Schema::Schema()
        : super_t()        
    {
    }

    Schema::Schema(Memspace &memspace, total_func get_total)
        : super_t(memspace)
        , m_get_total(get_total)
    {    
        if (!m_get_total) {
            m_get_total = []() -> std::uint32_t {
                THROWF(db0::InputException) << "Total count function is not set!" << THROWF_END;
            };
        }
    }
    
    Schema::Schema::Schema(mptr ptr, total_func get_total)
        : super_t(ptr)
        , m_get_total(get_total)
    {    
        if (!m_get_total) {
            m_get_total = []() -> std::uint32_t {
                THROWF(db0::InputException) << "Total count function is not set!" << THROWF_END;
            };
        }
    }
    
    Schema::~Schema() {
        assert((!m_builder || m_builder->empty()) && "Schema builder is not empty, flush it before destruction!");
    }

    void Schema::postInit(total_func get_total) {
        m_get_total = get_total;
    }
    
    Schema::Builder &Schema::getBuilder() const
    {
        if (!m_builder) {
            m_builder = std::make_unique<Builder>(const_cast<Schema &>(*this));
        }
        return *m_builder;
    }

    void Schema::add(unsigned int field_id, SchemaTypeId type_id) {
        getBuilder().collect(field_id, type_id, 1);
    }

    void Schema::remove(unsigned int field_id, SchemaTypeId type_id) {
        getBuilder().collect(field_id, type_id, -1);
    }

    std::pair<SchemaTypeId, SchemaTypeId> Schema::getType(unsigned int field_id) const
    {
        flush();
        if (field_id >= this->size()) {
            THROWF(db0::InputException) << "Unknown / invalid field ID: " << field_id;
        }
        return (*this)[field_id].getType();
    }

    std::pair<SchemaTypeId, SchemaTypeId> Schema::getType(FieldID field_id) const {
        return getType(field_id.getIndex());
    }

    std::vector<SchemaTypeId> Schema::getAllTypes(unsigned int field_id) const
    {
        flush();
        if (field_id >= this->size()) {
            THROWF(db0::InputException) << "Unknown / invalid field ID: " << field_id;
        }
        return (*this)[field_id].getAllTypes(this->getMemspace());
    }
    
    std::vector<SchemaTypeId> Schema::getAllTypes(FieldID field_id) const {
        return getAllTypes(field_id.getIndex());
    }

    void Schema::update(unsigned int field_id,
        std::vector<std::tuple<unsigned int, SchemaTypeId, int> >::const_iterator begin,
        std::vector<std::tuple<unsigned int, SchemaTypeId, int> >::const_iterator end,
        std::uint32_t collection_size)
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
            modifyItem(field_id).update(this->getMemspace(), begin, end, collection_size);
        }
    }
    
    void Schema::update(std::uint32_t collection_size)
    {
        // modify only items affected by the collection size change
        unsigned int field_id = 0;

        // NOTE: collect the items to be modified first
        // otherwise the v-bvector updates may lead to iterator invalidation
        std::vector<unsigned int> updated_fields;
                
        for (const auto &item : *this) {
            if (item.getPrimaryType(collection_size) != item.m_primary_type_id) {
                updated_fields.push_back(field_id);
            }
            ++field_id;
        }
    
        for (auto updated_field : updated_fields) {
            modifyItem(updated_field).update(this->getMemspace(), collection_size);
        }
        m_last_collection_size = collection_size;
    }
    
    void Schema::rollback() {
        m_builder = nullptr;
    }
    
    void Schema::flush() const
    {
        if (m_builder && !m_builder->empty()) {
            m_last_collection_size = m_get_total ? m_get_total() : 0;
            m_builder->flush(m_last_collection_size);
        } else {
            // if accessed as read/write then reflect the collection size change
            if (this->getMemspace().getAccessType() == AccessType::READ_WRITE) {
                auto collection_size = m_get_total ? m_get_total() : 0;
                if (m_last_collection_size != collection_size) {
                    const_cast<Schema &>(*this).update(collection_size);
                }
            }
        }
        m_builder = nullptr;
    }

    db0::Address Schema::getAddress() const {
        return super_t::getAddress();
    }

    void Schema::detach() const {
        super_t::detach();
    }
    
    void Schema::commit() const
    {
        flush();
        super_t::commit();
    }
    
    SchemaTypeId Schema::getPrimaryType(unsigned int field_id) const
    {
        auto type_pair = getType(field_id);
        if (type_pair.first == SchemaTypeId::UNDEFINED || type_pair.first == SchemaTypeId::NONE) {
            return type_pair.second;
        }
        return type_pair.first;
    }

    SchemaTypeId Schema::getPrimaryType(FieldID field_id) const {
        return getPrimaryType(field_id.getIndex());
    }

    SchemaTypeId getSchemaTypeId(StorageClass storage_class)
    {
        switch (storage_class) {            
            case StorageClass::POOLED_STRING:
            case StorageClass::STR64:
                return SchemaTypeId::STRING;
            case StorageClass::DB0_SERIALIZED:
                return SchemaTypeId::BYTES;
            case StorageClass::OBJECT_LONG_WEAK_REF:
                return SchemaTypeId::WEAK_REF;
            default:
                // for all other storage classes, there's 1-1 mapping
                return static_cast<SchemaTypeId>(storage_class);
        }
    }
    
    db0::bindings::TypeId getTypeId(SchemaTypeId schema_type_id)
    {        
        // NOTE: types are directly mappable
        auto storage_class = static_cast<StorageClass>(schema_type_id);
        // NOTE: types also directly mappable
        auto pre_storage_class = static_cast<PreStorageClass>(storage_class);
        return TypeUtils::m_storage_class_mapper.getTypeId(pre_storage_class);        
    }
    
    std::string getTypeName(SchemaTypeId type_id)
    {
        switch (type_id) {
            case SchemaTypeId::UNDEFINED: return "UNDEFINED";
            case SchemaTypeId::NONE: return "None";
            case SchemaTypeId::STRING: return "str";
            case SchemaTypeId::INT: return "int";
            case SchemaTypeId::TIMESTAMP: return "timestamp";
            case SchemaTypeId::FLOAT: return "float";
            case SchemaTypeId::DATE: return "Date";
            case SchemaTypeId::DATETIME: return "DateTime";
            case SchemaTypeId::DATETIME_TZ: return "DateTimeTZ";
            case SchemaTypeId::TIME: return "Time";
            case SchemaTypeId::TIME_TZ: return "TimeTZ";
            case SchemaTypeId::DECIMAL: return "Decimal";
            case SchemaTypeId::OBJECT: return "Object";
            case SchemaTypeId::LIST: return "List";
            case SchemaTypeId::DICT: return "Dict";
            case SchemaTypeId::SET: return "Set";
            case SchemaTypeId::TUPLE: return "Tuple";
            case SchemaTypeId::CLASS: return "Class";
            case SchemaTypeId::INDEX: return "index";
            case SchemaTypeId::BYTES: return "Bytes";
            case SchemaTypeId::BYTES_ARRAY: return "BytesArray";
            case SchemaTypeId::ENUM_TYPE: return "EnumType";
            case SchemaTypeId::ENUM: return "Enum";
            case SchemaTypeId::BOOLEAN: return "bool";
            case SchemaTypeId::WEAK_REF: return "WeakProxy";
            default:
                return "!INVALID";
        }
    }

}
