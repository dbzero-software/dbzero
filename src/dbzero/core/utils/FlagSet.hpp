#pragma once

#include <iostream>
#include <string>
#include <initializer_list>
#include <dbzero/core/utils/preprocessor.hpp>
#include <dbzero/core/exception/Exceptions.hpp>
#include "lexical_cast.hpp"
#include "string_compare.hpp"
    
namespace db0

{

    template <typename enum_t> class FlagSetLimits
    {
    public:
        using store_t = std::underlying_type_t<enum_t>;
        static store_t max() {
            THROWF(db0::InternalException) << "Missing DECLARE_ENUM_FLAGS directive. Internal error." << THROWF_END;
        }

        static constexpr unsigned int count() {
            return 0;
        }

        static constexpr store_t all() {
            THROWF(db0::InternalException) << "Missing DECLARE_ENUM_FLAGS directive. Internal error." << THROWF_END;            
        }
    };
    
    template<typename EnumT> class [[gnu::packed]] FlagSet
    {
    public:
        using enum_t = EnumT;
        using store_t = std::underlying_type_t<enum_t>;

        // Default constructor (all 0s)
        FlagSet() = default;

        FlagSet(const FlagSet &) = default;

        // Initializer list constructor
        FlagSet(const std::initializer_list<enum_t> &initList) {
            for (enum_t t: initList) {
                m_flags |= static_cast<store_t>(t);
            }
        }

        /**
         * Construct from vector of elements (can be std::string)
         * @tparam T
         */
        template <typename T = EnumT> FlagSet(const std::vector<T> &values) {
            for (auto &value: values) {
                if constexpr (std::is_same<T, EnumT>::value) {
                    this->set(value, true);
                } else {
                    this->set(db0::lexical_cast<EnumT>(value), true);
                }
            }
        }

        // Value constructor
        explicit FlagSet(store_t value)
            // only set flags that are allowed
            : m_flags(value & FlagSetLimits<enum_t>::all())
        {}

        bool operator[](enum_t flag) const {
            return test(flag);
        }

        store_t value() const {
            return m_flags;
        }

        FlagSet &set() {
            m_flags = ~store_t(0);
            return *this;
        }

        FlagSet &set(enum_t flag, bool val = true) {
            m_flags = val ? (m_flags|static_cast<store_t>(flag)) : (m_flags&~static_cast<store_t>(flag));
            return *this;
        }

        std::size_t count() const {
            store_t bits = m_flags;
            std::size_t total = 0;
            for(; bits != 0; ++total){
                bits >>= 1;
            }
            return total;
        }

        constexpr std::size_t size() const {
            return sizeof(enum_t) * 8;
        }

        bool test(enum_t flag) const {
            return (m_flags & static_cast<store_t>(flag)) > 0;
        }

        bool any() const {
            return m_flags > 0;
        }

        bool none() const {
            return m_flags == 0;
        }

        FlagSet operator&(EnumT flag) const {
            return FlagSet(m_flags & flag);
        }

        FlagSet operator&(const FlagSet &other) const {
            return FlagSet(m_flags & other.m_flags);
        }

        FlagSet operator|(EnumT flag) const {
            return FlagSet(m_flags | static_cast<store_t>(flag));
        }

        FlagSet operator|(const FlagSet &other) const {
            return FlagSet(m_flags | other.m_flags);
        }

        FlagSet operator^(const FlagSet &other) const {
            return FlagSet(m_flags ^ other);
        }

        FlagSet operator~() const {
            return FlagSet(~m_flags);
        }

        bool operator==(const FlagSet &other) const {
            return m_flags == other.m_flags;
        }

        bool operator!=(const FlagSet &other) const {
            return m_flags != other.m_flags;
        }

        /**
         * Unpack flags into vector of specific type (can be std::string)
         * @tparam T unpacked type
         * @return vector of unpacked values
         */
        template <typename T = EnumT> std::vector<T> unpack() const {
            store_t flag = 1u;
            std::vector<T> result;
            for (auto value = m_flags;value > 0;value >>= 1, flag <<= 1) {
                if (value & 0x01) {
                    if constexpr (std::is_same<T, EnumT>::value) {
                        result.emplace_back((EnumT)flag);
                    } else {
                        result.emplace_back(db0::lexical_cast<T>((EnumT)flag));
                    }
                }
            }
            return result;
        }

        /**
         * Construct all-flags flag set (DECLARE_ENUM_VALUES must be added to work properly)
         * @return
         */
        static constexpr FlagSet all() {
            return FlagSet(FlagSetLimits<enum_t>::all());
        }

    private:
        store_t m_flags = 0;
    };

}

// Macro to define stream output and input operators for associated enum values
// Example (see also FlagSetTest)
// DECLARE_ENUM_VALUES(TestEnum, "one", "two", "three")
// remember to use DEFINE_ENUM_VALUES in .cpp file
#define DECLARE_ENUM_VALUES(EnumTypeName, Count) namespace db0 { \
    template <> class FlagSetLimits<EnumTypeName> { public: \
        using store_t = std::underlying_type_t<EnumTypeName>; \
        static store_t max() { return 0x1 << Count; } \
        static constexpr unsigned int all() { return (0x1 << Count) - 1; } \
        static constexpr unsigned int count() { return Count; } }; } namespace std { \
    std::ostream &operator<<(std::ostream &os, EnumTypeName); \
    std::istream &operator>>(std::istream &in, EnumTypeName &); }

#define DEFINE_ENUM_VALUES(EnumTypeName, ...) \
namespace std { std::ostream &operator<<(std::ostream &os, EnumTypeName option) { \
    static std::vector<std::string> str_values { __VA_ARGS__ }; \
    static_assert(NUMARGS(__VA_ARGS__) == db0::FlagSetLimits<EnumTypeName>::count()); \
    using store_t = std::underlying_type_t<EnumTypeName>; \
    store_t flag = 0x01; \
    store_t int_value = (store_t)option; \
    for (unsigned int i = 0; int_value > 0; int_value >>= 1, flag <<= 1, ++i) { \
        if (int_value & 0x01) { \
            if (i >= str_values.size()) { \
                THROWF(db0::InternalException) << "Invalid value for " << #EnumTypeName << ": " << (int)option; \
            } \
            return os << str_values[i]; \
        } \
    } \
    return os; } \
    std::istream &operator>>(std::istream &in, EnumTypeName &result) { \
        static std::vector<std::string> str_values { __VA_ARGS__ }; \
        using store_t = std::underlying_type_t<EnumTypeName>; \
        std::string str_input;std::getline(in, str_input); \
        store_t flag = 0x01; \
        for (auto &str_value: str_values) { if (db0::iequals(str_value, str_input)) { \
            result = (EnumTypeName)flag;return in; } \
            flag <<= 1; \
        } \
        THROWF(db0::InputException) << "Unrecognized flag: " << str_input << THROWF_END; } }

namespace std 

{

    template <typename EnumT> ostream &operator<<(ostream &os, const db0::FlagSet<EnumT> &flags)
    {
        os << "[";
        bool is_first = true;
        for (auto &flag: flags.unpack()) {
            if (!is_first) {
                os << ", ";
            }
            os << flag;
            is_first = false;
        }
        os << "]";
        return os;    
    }

}
