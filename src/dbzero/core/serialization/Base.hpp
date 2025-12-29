// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/core/serialization/Foundation.hpp>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/core/compiler_attributes.hpp>

namespace db0 

{
DB0_PACKED_BEGIN

    class Memspace;

    // base class for safe overlaid objects,
    // implements __safe_ref
    // T - actual implemented overlay type
    template <std::uint16_t VER, bool STORE_VER>
    class DB0_PACKED_ATTR version_base {
    };

    template <std::uint16_t VER>
    class DB0_PACKED_ATTR version_base<VER, false>{
    public:
        static constexpr bool isVerStored(){
            return false;
        }
        static constexpr std::size_t storedVerSize(){
            return 0;
        }
        static constexpr std::uint16_t implVer() {
            return 0;
        }
        std::uint16_t objVer() const {
            return 0;
        }
        template <class buf_t> static std::uint16_t objVer(buf_t at) {
            return 0;
        }
    };

    template <std::uint16_t VER>
    class DB0_PACKED_ATTR version_base<VER, true> {
    private:
        std::uint16_t storedVersion = VER;
    public:
        static constexpr bool isVerStored(){
            return true;
        }
        static constexpr std::size_t storedVerSize(){
            return sizeof(std::uint16_t);
        }
        static constexpr std::uint16_t implVer() {
            return VER;
        }
        std::uint16_t objVer() const {
            return storedVersion;
        }
        template <class buf_t> static std::uint16_t objVer(buf_t at) {
            return reinterpret_cast<const version_base*>((const std::byte*)(at))->objVer();
        }
    };

    template <class T, std::uint16_t VER=0, bool STORE_VER=true>
    class DB0_PACKED_ATTR o_base
            : private version_base<VER, STORE_VER>
    {
    public:
        typedef std::false_type has_constant_size;

    private :
        typedef o_base<T, VER, STORE_VER> this_type;
        typedef version_base<VER, STORE_VER> ver_type;

        // copy / move disallowed
        o_base(const this_type &) {
            THROWF(db0::InternalException)<< "copy overlaid type not allowed";
        }

        o_base(this_type &&) {
            THROWF(db0::InternalException)<< "move overlaid type not allowed";
        }

        this_type &operator=(const this_type &) {
            THROWF(db0::InternalException)<< "copy overlaid type not allowed";
            return *this;
        }

        this_type &operator=(this_type &&) {
            THROWF(db0::InternalException)<< "copy overlaid type not allowed";
            return *this;
        }

    protected :
        using Arranger = Foundation::Arranger;
        using Meter = Foundation::Meter;
        template<typename BufType>
        using SafeSize = Foundation::SafeSize<BufType>;
        friend class Foundation;

        // default ctor does nothing!!!
        o_base() {
            //this below trigger on dummy (but were-to-be-present) errors such like:
            // class TTT : public o_base<TT>
            //                           ^__________ you misspelled TTT
            static_assert (std::is_base_of<o_base<T, VER, STORE_VER>, T>::value, "This class should be used in CRTP context!");
        }

        // calculate instance end address - i.e. placement for first
        // variable length member instance
        std::byte *beginOfDynamicArea() {
            return reinterpret_cast<std::byte*>(this) + baseSize();
        }

        // pointer to fist class member
        std::byte *beginOfMemberArea() {
            return reinterpret_cast<std::byte*>(this) + ver_type::storedVerSize();
        }

        const std::byte *beginOfDynamicArea() const {
            return const_cast<this_type*>(this)->beginOfDynamicArea();
        }

        const std::byte *beginOfMemberArea() const {
            return const_cast<this_type*>(this)->beginOfMemberArea();
        }

        // size of fixed-size members of this class
        static constexpr std::size_t baseSize() {
            return true_size_of<T>();  //T inherits from me!
        }

        // use this to help in instantiation of variable-length members
        Arranger arrangeMembers() {
            return Foundation::arrangeMembersOf<T>(
                    reinterpret_cast<T&>(*this)
            );
        }

        static Meter measureMembers() {
            return Foundation::measureMembersOf<T>(baseSize());
        }

        template <class buf_t> static Foundation::SafeSize<buf_t> sizeOfMembers(buf_t at) {
            return Foundation::sizeOfMembers<T>(baseSize(), at, getObjVer(at));
        }

        Foundation::SafeSize<const std::byte*> sizeOfMembers() const
        {
            return sizeOfMembers(reinterpret_cast<const std::byte*>(this));
        }

        /*
         * eight method implementing dynamic jumping on members : with exception/def value, with const/noconst reference
         */
        template<typename member_t> typename member_t::type &getDynFirst(member_t type, std::uint16_t minVersion = 0) {
            if (getObjVer() < minVersion) {
                THROWF(InputException) << "Version too low to support operation. Current is: "
                    << getObjVer() << ", for minimum support you need " << minVersion;
            }
            return type.__ref(reinterpret_cast<std::byte*>(this) + baseSize());
        }

        template<typename member_t> typename member_t::type &getDynFirst(member_t type, typename member_t::type &value,
                std::uint16_t minVersion = 0)
        {
            if (getObjVer() < minVersion) {
                return value;
            }
            return type.__ref(reinterpret_cast<std::byte*>(this) + baseSize());
        }

        template<typename member_t> const typename member_t::type &getDynFirst(member_t type, std::uint16_t minVersion = 0) const {
            if(getObjVer() < minVersion){
                THROWF(InputException)<<"Version too low to support operation. Current is: "
                    << getObjVer() << ", for minimum support you need " << minVersion;
            }
            return type.__const_ref(reinterpret_cast<const std::byte*>(this) + baseSize());
        }

        template<typename member_t> const typename member_t::type &getDynFirst(member_t type, const typename member_t::type &value,
                std::uint16_t minVersion = 0) const
        {
            if (getObjVer() < minVersion) {
                return value;
            }
            return type.__const_ref(reinterpret_cast<const std::byte*>(this) + baseSize());
        }

        template<typename member_t, typename member_before_t> typename member_t::type &getDynAfter(const member_before_t &member_before,
                member_t type, std::uint16_t minVersion = 0)
        {
            if (getObjVer() < minVersion) {
                THROWF(InputException) << "Version too low to support operation. Current is: "
                    << getObjVer() << ", for minimum support you need " << minVersion;
            }
            return type.__ref((std::byte*)(&member_before) + member_before.sizeOf());
        }

        template<typename member_t, typename member_before_t> typename member_t::type &getDynAfter(const member_before_t& member_before,
                member_t type, typename member_t::type &value, std::uint16_t minVersion = 0)
        {
            if (getObjVer() < minVersion) {
                return value;
            }
            return type.__ref((std::byte*)(&member_before) + member_before.sizeOf());
        }

        template<typename member_t, typename member_before_t> const typename member_t::type &getDynAfter(const member_before_t& member_before,
                member_t type, std::uint16_t minVersion = 0) const {
            if(getObjVer() < minVersion){
                THROWF(InputException)<<"Version too low to support operation. Current is: "
                    << getObjVer() << ", for minimum support you need " << minVersion;
            }
            return type.__const_ref(reinterpret_cast<const std::byte*>(&member_before) + member_before.sizeOf());
        }

        template<typename member_t, typename member_before_t> const typename member_t::type &getDynAfter(const member_before_t &member_before,
                member_t type, const typename member_t::type &value, std::uint16_t minVersion = 0) const
        {
            if (getObjVer() < minVersion) {
                return value;
            }
            return type.__const_ref(reinterpret_cast<const std::byte*>(&member_before) + member_before.sizeOf());
        }

    public :

        void assertImplVersion() const {
            if (getObjVer() > getImplVer()) {
                THROWF(InputException) << "Referenced object from newer version ["
                    << getObjVer() << "] than this code permits [" << getImplVer() << "]";
            }
        }

        static constexpr bool isExtType() {
            return false;
        }

        /**
         * Default overlaid copy constructor (copy raw bytes)
         */
        static T &__new(void *buf, const T &other) {
            memcpy(buf, &other, other.sizeOf());
            return *reinterpret_cast<T*>(buf);
        }

        static std::size_t measure(const T &other) {
            return other.sizeOf();
        }

        // bcs of constant measure, we can safely call placement new...
        template<typename... Args>
        static T &__new(void *buf, Args&& ...args) {
            return *(new(buf) T(std::forward<Args>(args)...));
        }

        static constexpr bool getIsVerStored(){
            return ver_type::isVerStored();
        }

        static constexpr std::uint16_t getImplVer() {
            return ver_type::implVer();
        }

        template <class buf_t> static std::uint16_t getObjVer(buf_t at) {
            return ver_type::objVer(at);
        }

        std::uint16_t getObjVer() const {
            return ver_type::objVer();
        }

        static inline T &__ref(void *buf) {
#ifndef NDEBUG
            reinterpret_cast<T*>(buf)->assertImplVersion();
#endif
            return *reinterpret_cast<T*>(buf);
        }

        static inline const T &__const_ref(const void *buf) {
            return *reinterpret_cast<const T*>(buf);
        }

        inline std::size_t sizeOf() const {
            return T::safeSizeOf(reinterpret_cast<const std::byte*>(this));
        }

        /**
         * Safe object reference (buffer bounds validated by buf_t)
         */
        template <class buf_t> static T &__safe_ref(buf_t buf) 
        {
            const std::byte *_buf = buf;
            // scan members & validate bounds
            T::safeSizeOf(buf);
            return __ref(const_cast<std::byte*>(_buf));
        }
        
        /**
         * Safe const object reference (buffer bounds validated by buf_t)
         */
        template <class buf_t> static const T &__safe_const_ref(buf_t buf)
        {
            // scan members & validate bounds
            T::safeSizeOf(buf);
            return __const_ref(buf);
        }

        // default destroy does nothing
        inline void destroy(db0::Memspace &) const {}

        // function helpful when instantiating type with arranger object
        static Foundation::Type<T> type() {
            return Foundation::Type<T>();
        }

    };

DB0_PACKED_END
}
