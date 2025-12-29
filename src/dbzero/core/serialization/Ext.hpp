// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/core/serialization/Base.hpp>
#include <dbzero/core/exception/AbstractException.hpp>
#include <cstring>
#include <dbzero/core/compiler_attributes.hpp>

namespace db0

{
DB0_PACKED_BEGIN

    class Memspace;

    template <std::uint16_t VER, bool STORE_VER>
    class DB0_PACKED_ATTR ext_version_base {
    };

    template <std::uint16_t VER>
    class DB0_PACKED_ATTR ext_version_base<VER, false>{
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
        static std::uint16_t objVer(const void *){
            return 0;
        }
    };

    template <std::uint16_t VER>
    class DB0_PACKED_ATTR ext_version_base<VER, true> {
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
        static std::uint16_t objVer(const void *ptr){
            return *reinterpret_cast<const std::uint16_t*>(ptr);
        }
    };

    ////////////////////////////////
    //
    //versioning assumption:
    // - base type stays the same, only version is changing
    // - you can change base type version within the same version of derived type
    // - it will be better

    // T - this type
    // base_T - some overlaid type to extend from
    
    template <
        typename T,
        typename super_t,
        std::uint16_t VER=0, bool STORE_VER=true
    >
    class DB0_PACKED_ATTR o_ext : public super_t{
        using this_type = o_ext<T, super_t, VER, STORE_VER>;

    public:
        using has_constant_size = std::false_type;

        std::uint16_t getObjVer() const {
            return ext_version_base<VER, STORE_VER>::objVer(beginOfDynamicArea()-ext_version_base<VER, STORE_VER>::storedVerSize());
        }

        static constexpr bool getIsVerStored(){
            return ext_version_base<VER, STORE_VER>::isVerStored();
        }

        static constexpr std::uint16_t getImplVer(){
            return ext_version_base<VER, STORE_VER>::implVer();
        }

        void assertImplVersion() const {
            getSuper().assertImplVersion();
            if (getObjVer() > getImplVer()) {
                THROWF(InputException)<< " Referenced object from newer version [" << getObjVer() << "] than this code permits [" << getImplVer() << "]";
            }
        }

        static constexpr bool isExtType(){
            return true;
        }

        const super_t& getSuper() const { return static_cast<const super_t&>(*this); }
        super_t& getSuper() { return static_cast<super_t&>(*this); }

    protected :
        using Arranger = Foundation::Arranger;
        using Meter = Foundation::Meter;
        //using MemberLookup = Foundation::MemberLookup;
        friend class Foundation;
    protected :
        // NOTICE: protected constructor forward to actual overlaid base type
        // it will not be checked against its expected size in first instantiation
        template <typename... Args> o_ext(Args&& ...args)
            : super_t(std::forward<Args>(args)...)
        {

            //it is an error to have super_t o_ext'extended from another type
            //and have fixed members in T, due to versioning implementation
            static_assert (!super_t::isExtType() || !super_t::getIsVerStored() || true_size_of<super_t>()==true_size_of<T>(), "Base type is o_ext'ended and this type contains static member - versioning will overwrite it! Please dont blame Pawel on this...");


            //here we try to do sth meaningfull, create another copy in managed way
            if (getIsVerStored()) {
                *reinterpret_cast<std::uint16_t*>(beginOfDynamicArea()-ext_version_base<VER, STORE_VER>::storedVerSize()) = getImplVer();
            }
        }

        /**
         * begin address for constructing variable-length members
         */
        std::byte *beginOfDynamicArea() {
            return reinterpret_cast<std::byte*>(this) + baseSize();
        }

        const std::byte *beginOfDynamicArea () const {
            return reinterpret_cast<const std::byte*>(this) + baseSize();
        }

        template <class buf_t> static size_t safeBaseSize (buf_t at) {
            // adjust for fixed size members in derived class
            std::size_t size = super_t::__const_ref(at).sizeOf() + true_size_of<T>() + ext_version_base<VER, STORE_VER>::storedVerSize() - true_size_of<super_t>();
            // to assure this is within bounds
            at += size;
            return size;
        }

        size_t baseSize() const {
            return safeBaseSize(reinterpret_cast<const std::byte*>(this));
        }

        /**
         * use this to help in instantiation of variable-length members
         */
        Arranger arrangeMembers() {
            return Foundation::arrangeMembersOf<T>(
                reinterpret_cast<T&>(*this)
            );
        }

        template<typename... Args>
        static Meter measureMembersFromBase(Args&& ...args)
        {
            return Foundation::measureMembersOf<T>
                (true_size_of<T>() + ext_version_base<VER, STORE_VER>::storedVerSize() - true_size_of<super_t>())
                (super_t::type(), std::forward<Args>(args)...);
        }

        template <typename buf_t> static Foundation::SafeSize<buf_t> sizeOfMembers(buf_t at)
        {
            std::size_t s = safeBaseSize(at);
            return Foundation::sizeOfMembers<T>(
                s, at, ext_version_base<VER, STORE_VER>::objVer(((const std::byte*)at) + s - ext_version_base<VER, STORE_VER>::storedVerSize())
            );
        }

        /*
            eight method implementing dynamic jumping on members : with exception/def value, with const/noconst reference
        */
        template<typename member_t> typename member_t::type &getDynFirst(member_t type, std::uint16_t minVersion=0){
            if(getObjVer() < minVersion){
                THROWF(InputException)<<"Version too low to support operation. Current is: "<<getObjVer()<<", for minimum support you need "<<minVersion;
            }
            return type.__ref(reinterpret_cast<std::byte*>(this) + baseSize());
        }

        template<typename member_t> typename member_t::type &getDynFirst(member_t type, typename member_t::type &value, std::uint16_t minVersion=0){
            if(getObjVer() < minVersion){
                return value;
            }
            return type.__ref(reinterpret_cast<std::byte*>(this) + baseSize());
        }

        template<typename member_t> const typename member_t::type &getDynFirst(member_t type, std::uint16_t minVersion=0) const {
            if(getObjVer() < minVersion){
                THROWF(InputException)<<"Version too low to support operation. Current is: "<<getObjVer()<<", for minimum support you need "<<minVersion;
            }
            return type.__const_ref(reinterpret_cast<const std::byte*>(this) + baseSize());
        }
        template<typename member_t> const typename member_t::type &getDynFirst(member_t type, const typename member_t::type &value, std::uint16_t minVersion=0) const {
            if(getObjVer() < minVersion){
                return value;
            }
            return type.__const_ref(reinterpret_cast<const std::byte*>(this) + baseSize());
        }

        template<typename member_t, typename member_before_t> typename member_t::type &getDynAfter(const member_before_t& member_before, member_t type, std::uint16_t minVersion=0){
            if(getObjVer() < minVersion){
                THROWF(InputException)<<"Version too low to support operation. Current is: "<<getObjVer()<<", for minimum support you need "<<minVersion;
            }
            return type.__ref((std::byte*)(&member_before) + member_before.sizeOf());
        }
        template<typename member_t, typename member_before_t> typename member_t::type &getDynAfter(const member_before_t& member_before, member_t type, typename member_t::type &value, std::uint16_t minVersion=0){
            if(getObjVer() < minVersion){
                return value;
            }
            return type.__ref((std::byte*)(&member_before) + member_before.sizeOf());
        }
        template<typename member_t, typename member_before_t> const typename member_t::type &getDynAfter(const member_before_t& member_before, member_t type, std::uint16_t minVersion=0) const {
            if(getObjVer() < minVersion){
                THROWF(InputException)<<"Version too low to support operation. Current is: "<<getObjVer()<<", for minimum support you need "<<minVersion;
            }
            return type.__const_ref(reinterpret_cast<const std::byte*>(&member_before) + member_before.sizeOf());
        }
        template<typename member_t, typename member_before_t> const typename member_t::type &getDynAfter(const member_before_t& member_before, member_t type, const typename member_t::type &value, std::uint16_t minVersion=0) const {
            if(getObjVer() < minVersion){
                return value;
            }
            return type.__const_ref(reinterpret_cast<const std::byte*>(&member_before) + member_before.sizeOf());
        }

    public :
        static inline T &__ref(void *buf) {
            return *reinterpret_cast<T*>(buf);
        }

        static inline const T &__const_ref(const void *buf) {
            return *reinterpret_cast<const T*>(buf);
        }

        // measures space requirement of the base overlaid type
        // plus size of all fixed size members of derived type
        template <typename... Args> static std::size_t measureBase(Args&& ...args) 
        {
            std::size_t result = super_t::measure(std::forward<Args>(args)...);
            // adjust for fixed size members in derived class
            result += true_size_of<T>() - true_size_of<super_t>();
            return result;
        }
        
        inline std::size_t sizeOf() const {
            return T::safeSizeOf(reinterpret_cast<const std::byte*>(this));
        }

        /**
         * safe object reference ( buffer bounds validated by buf_t )
         */
        template <typename buf_t> static T &__safe_ref(buf_t buf) 
        {
            T::safeSizeOf(buf); // scan members & validate bounds
            return __ref(buf);
        }

        template <typename buf_t> static const T &__safe_const_ref(buf_t buf) 
        {
            T::safeSizeOf(buf); // scan members & validate bounds
            return __const_ref(buf);
        }

        // bcs of constant measure, we can safely call placement new...
        template<typename... Args> static T &__new(void *buf, Args&& ...args) 
        {
            // for now T expected size is its full size,
            // eventually T() will call o_ext(some_args from args)
            // only then this expected size will by lowered by super_t expected size
            // why then? bcs now we do not know what args go to the ctor of super_t
            return *(new(buf) T(std::forward<Args>(args)...));
        }

        inline void destroy(db0::Memspace &memspace) const {
            super_t::__const_ref(this).destroy(memspace);
        }

        // function helpful when instantiating type with arranger object
        static Foundation::Type<T> type() {
            return Foundation::Type<T>();
        }

    };

    // T - this type ( fixed size )
    // super_t - some fixed-size overlaid type to extend from
    template <typename T, typename super_t> class DB0_PACKED_ATTR o_fixed_ext : public super_t 
    {
        struct NullInitializer {};

    public:
        typedef NullInitializer Initializer;

        template<typename... Args> o_fixed_ext(Args&& ...args)
            : super_t(std::forward<Args>(args)...)
        {            
        }

        template<typename... Args>
        static T &__new(void *buf, Args&& ...args) {
            return *(new(buf) T(std::forward<Args>(args)...));
        }

        static inline T &__ref(void *buf) {
            return *reinterpret_cast<T*>(buf);
        }

        static inline const T &__const_ref(const void *buf) {
            return *reinterpret_cast<const T*>(buf);
        }

        inline std::size_t sizeOf(const void *) const {
            return sizeOf();
        }

        static std::size_t sizeOf() {
            return true_size_of<T>();
        }

        /**
         * Safe object reference (buffer bounds validated by buf_t)
         */
        template <typename buf_t> static T &__safe_ref(buf_t buf) 
        {
            // validate bounds
            buf_t _buf = buf;
            _buf += true_size_of<T>();
            return __ref(buf);
        }

        template <typename buf_t> static const T &__safe_const_ref(buf_t buf) 
        {
            // validate bounds
            buf_t _buf = buf;
            _buf += true_size_of<T>();
            return __const_ref(buf);
        }

        inline void destroy(db0::Memspace &memspace) const {
            super_t::__const_ref(this).destroy(memspace);
        }

        template <typename buf_t> static size_t safeSizeOf(buf_t at) {
            at += true_size_of<T>();
            return true_size_of<T>();
        }

        template<typename... Args>
        static inline size_t measure (Args&&...) {
            return true_size_of<T>();
        }

        static constexpr bool getIsVerStored() {
            return false;
        }

        static constexpr std::uint16_t getObjVer() {
            return 0;
        }

        static constexpr std::uint16_t getImplVer() {
            return 0;
        }

        static constexpr bool isExtType() {
            return true;
        }

        static Foundation::Type<T> type () {
            return Foundation::Type<T>();
        }

        void assertImplVersion() const {
        }
    };

DB0_PACKED_END
}