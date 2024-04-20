#pragma once

#include <vector>
#include <cstdint>

namespace db0::serial

{

    // DBZero serializable collection types must register here
    enum class CollectionTypes: std::uint16_t
    {
        Invalid = 0,
        MBIndex = 1,
        VBIndex = 2,
        FT_MemoryIndex = 3,
        VSortedVector = 4
    };

    class Serializable
    {
    public:
        virtual ~Serializable() = default;
        
        /**
         * Serialize by appending bytes to the vector
         * NOTE: deserialization is done in the constructor
        */
        virtual void serialize(std::vector<std::byte> &) const = 0;
    };

    template <typename T> void write(std::vector<std::byte> &v, const T &t)
    {
        const std::byte *p = reinterpret_cast<const std::byte *>(&t);
        v.insert(v.end(), p, p + sizeof(T));
    }
    
    template <typename T> T read(std::vector<std::byte>::const_iterator &iter, std::vector<std::byte>::const_iterator end)
    {
        if (iter + sizeof(T) > end) {
            THROWF(db0::InternalException) << "Not enough bytes to read";
        }
        T t;        
        std::copy(iter, iter + sizeof(T), reinterpret_cast<std::byte *>(&t));
        iter += sizeof(T);
        return t;
    }

    /**
     * Set or get a type ID for the purpose of serialization
    */
    template <typename T> std::uint64_t typeId(std::uint64_t type_id = 0)
    {
        static std::unordered_map<std::string, std::uint64_t> type_ids;
        if (!type_id && type_ids.empty()) {
            // initialize with standard simple types, use type_ids < 100
            // !!! NOTE: do NOT modify order in this array as this would affect serialized type IDs
            using TypeList = std::tuple<
                std::int8_t, std::int16_t, std::int32_t, std::int64_t, 
                std::uint8_t, std::uint16_t, std::uint32_t, std::uint64_t, 
                float, double, std::string>;
            // compile error: binary expression in operand of fold-expression
            std::apply([&](auto... type) {
                std::size_t n { 0 };
                ((type_ids[typeid(type).name()] = ++n), ...); }, TypeList()
            );
        }
        
        if (type_id == 0) {
            // find existing type ID
            auto it = type_ids.find(typeid(T).name());
            if (it == type_ids.end()) {
                THROWF(db0::InternalException) << "Type ID not found for " << typeid(T).name();
            }
            return it->second;
        } else {
            if (type_ids.find(typeid(T).name()) != type_ids.end() && type_ids[typeid(T).name()] != type_id) {
                THROWF(db0::InternalException) << "Different type ID already set for " << typeid(T).name();
            }
            // set new type ID
            type_ids[typeid(T).name()] = type_id;
            return type_id;
        }
    }
    
}