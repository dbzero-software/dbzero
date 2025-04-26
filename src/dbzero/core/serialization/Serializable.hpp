#pragma once

#include <vector>
#include <cstdint>
#include <unordered_map>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/core/memory/Address.hpp>
#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/core/utils/bounded_buf_t.hpp>

namespace db0::serial

{

    // dbzero serializable collection types must register here
    enum class CollectionType: std::uint16_t
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
        static constexpr std::size_t UUID_SIZE = 54;
        static constexpr std::size_t SIGNATURE_SIZE = 32;
        virtual ~Serializable() = default;
        
        /**
         * Serialize by appending bytes to the vector
         * NOTE: deserialization is done in the constructor
        */
        virtual void serialize(std::vector<std::byte> &) const = 0;

        /**
         * Calculate UUID from a serialization buffer
         * @param uuid_buf buffer of at least UUID_SIZE bytes
        */
        void getUUID(char *uuid_buf) const;

        std::vector<std::byte> getUUID() const;
    };
    
    /**
     * Get (append) the signature of a serializable
     * this will first serialize the object and then calculate UUID
    */
    void getSignature(const Serializable &, std::vector<std::byte> &v);
    
    template <typename T> void write(std::vector<std::byte> &v, const T &t)
    {
        const std::byte *p = reinterpret_cast<const std::byte *>(&t);
        v.insert(v.end(), p, p + sizeof(T));
    }

    // write overlaid type, construct from args
    template <typename T, typename... Args> void write(std::byte *&at, Args &&... args)
    {        
        auto &value = T::__new(at, std::forward<Args>(args)...);
        at += value.sizeOf();
    }

    template <typename T> void writeSimple(std::byte *&at, T value) {
        write<o_simple<T> >(at, value);
    }

    template <typename T> void read(std::vector<std::byte>::const_iterator &iter,
        std::vector<std::byte>::const_iterator end, T &t)
    {
        if (iter + sizeof(T) > end) {
            THROWF(db0::InternalException) << "Not enough bytes to read";
        }
        std::copy(iter, iter + sizeof(T), reinterpret_cast<std::byte *>(&t));
        iter += sizeof(T);
    }
    
    template <typename T> T read(std::vector<std::byte>::const_iterator &iter, std::vector<std::byte>::const_iterator end)
    {
        T t;
        read(iter, end, t);
        return t;
    }

    // read overlaid type's value
    template <typename T> const T &read(std::byte *&at)
    {       
        auto &value = T::__const_ref(at);        
        at += value.sizeOf();
        return value;        
    }

    // read with additional bounds validation
    template <typename T>
    const T &read(std::byte *&at, const std::byte *end, const std::function<void()> &throw_func)
    {        
        const_bounded_buf_t buf(throw_func, at, end);
        auto &value = T::__safe_const_ref(buf);
        at += value.sizeOf();
        return value;       
    }

    template <typename T> T readSimple(std::byte *&at) {
        return read<o_simple<T> >(at);
    }
    
    template <typename T> T readSimple(std::byte *&at, const std::byte *end, const std::function<void()> &throw_func) {
        return read<o_simple<T> >(at, end, throw_func);
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
                std::uint8_t, std::uint16_t, std::uint32_t, std::uint64_t, db0::Address, db0::UniqueAddress,
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