// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once
    
#include <utility>
#include <cstdint>
#include <cstddef>
#include <optional>

namespace db0 

{ 

    /** 
     * Foundation classes for building overlaid types / instances
     */
    class Foundation {
    public :

        template <class T> class Type {
        public :
            typedef T type;
            template <typename... Args> std::size_t newInstance(void *at, Args&&... args) const {
                return T::__new(at, std::forward<Args>(args)...).sizeOf();
            }

            size_t sizeOf(const void *buf) const {
                return T::__const_ref(buf).sizeOf();
            }

            T &__ref(void *buf) {
                return T::__ref(buf);
            }

            const T &__const_ref(const void *buf) const {
                return T::__const_ref(buf);
            }

            template <typename... Args> std::size_t measure(Args&&... args) const {
                return T::measure(std::forward<Args>(args)...);
            }

            template <typename buf_t> std::size_t safeSizeOf(buf_t buf) const {
                return T::__safe_const_ref(buf).sizeOf();
            }
        };

        // type arranger

        //Arranger version invariants:
        // always (checked in ctor)
        //   implVer == objVer
        // at building object, when constructing obj (checked in operator [])
        //   implVer >= (oldCurrImplVer+1 == currImplVer)
        // at end of building object (checked in LAST dtor)
        //   implVer == currImplVer == objVer
        //

        enum class AssertVersionMode{
            precondition,
            condition,
            postcondition
        };


        class Arranger {
            const std::byte *from;
            std::byte *at;

        public:
            Arranger& operator = (Arranger&& );
            ~Arranger();

            //we can only transfer responsibility to check further, no copy!
            Arranger(Arranger&& rhs) noexcept ;

            Arranger(const Arranger& rhs) = delete;
            Arranger& operator = (const Arranger& rhs) = delete;

            Arranger(const std::byte *_from, std::byte *_at) noexcept;
    
            std::size_t getSizeToHere() const;

            Arranger operator [](std::uint16_t newImplVersion);

            // instantiate new member of type member_type
            // member_t - must be Type<> object of some overlaid type
            template <class member_type, typename... Args> Arranger operator()(member_type type, Args&&... args) {
                at += type.newInstance(at, std::forward<Args>(args)...);
                return std::move(*this);
            }

            operator std::size_t() const;

            inline std::byte* ptr()
            {
                return at;
            }
        };

        //Arrange overlaid members in specific instance
        template <typename T> static Arranger arrangeMembersOf(T &instance, std::size_t _expectedSize=0) {
            (void)_expectedSize;
            return Arranger(
                reinterpret_cast<const std::byte*>(&instance), instance.beginOfDynamicArea()
            );
        }

        //SafeSizeOf version invariants:
        // always (checked in ctor)
        //   implVer >= objVer
        // when measuring obj  (checked in operator [])
        //   implVer >= (oldCurrImplVer+1 == currImplVer)
        // at end of building object (checked in dtor)
        //   implVer == currImplVer
        //

        class SafeSizeBase{

        protected:
            std::size_t   sizeSoFar;
            std::uint16_t currImplVer,
                        objVer;

        private:
            void assertPrecondition(std::uint16_t _implVer);

        public:
            SafeSizeBase& operator = (SafeSizeBase&& rhs) = default;
            ~SafeSizeBase();

            SafeSizeBase(const SafeSizeBase& ) = delete;
            SafeSizeBase& operator = (const SafeSizeBase& ) = delete;
            SafeSizeBase(SafeSizeBase&& rhs) noexcept ;

            SafeSizeBase(
                size_t _sizeSoFar
                , std::uint16_t _currImplVer
                , std::uint16_t _objVer
                , std::uint16_t _implVer
            ) noexcept (false);

    protected:
            void handleNewImplVersion(std::uint16_t newImplVersion) ;

    public:
            operator std::size_t() const ;
        };

        template <typename buf_t> class SafeSize : public SafeSizeBase {
            buf_t         buf;

        public:
            ~SafeSize() = default ;

            SafeSize(const SafeSize& ) = delete;
            SafeSize& operator = (const SafeSize& ) = delete;
            SafeSize(SafeSize&& rhs) = default;
            SafeSize& operator = (SafeSize&& rhs) = default;

            SafeSize(
                std::size_t _sizeSoFar
                , buf_t _buf
                , std::uint16_t _currImplVer
                , std::uint16_t _objVer
                , std::uint16_t _implVer
            ) noexcept (false)
                : SafeSizeBase(
                    _sizeSoFar, _currImplVer, _objVer, _implVer
                ), buf(std::move(_buf))
            {}

            SafeSize operator[](std::uint16_t newImplVersion) {
                handleNewImplVersion(newImplVersion);
                return std::move(*this);
            }

            /**
             * include size of instance of specific member type
             */
            template <class member_type> SafeSize operator()(member_type type) {
                if (objVer >= currImplVer) {
                    sizeSoFar += type.safeSizeOf(&buf[sizeSoFar]);
                }
                // new safeSize, that now is responsible to check everything
                return std::move(*this);
            }

            SafeSize operator()(std::size_t size_of_member)
            {
                if (objVer >= currImplVer) {
                    sizeSoFar += size_of_member;
                }
                // new safeSize, that now is responsible to check everything
                return std::move(*this);
            }
        };

        /**
         * Measure (safely) overlaid non-fixed members in specific instance
         */
        template <typename T, typename buf_t> static SafeSize<buf_t> sizeOfMembers(size_t base_size, buf_t at, uint16_t obj_version) {
            return SafeSize<buf_t>(
                base_size, at, 0, obj_version, T::getImplVer()
            );
        }

        //Meter version invariants:
        // when measuring obj (checked in operator [])
        //   implVer >= (oldCurrImplVer+1 == currImplVer)
        // at end of building object (checked in dtor)
        //   implVer == currImplVer
        //

        class Meter {
            std::size_t sizeSoFar;
        public :
            Meter& operator = (Meter&& );
            ~Meter();

        public :
            /**
             * we can only transfer responsibility to check further, no copy!
             */
            Meter(const Meter & ) = delete;
            Meter& operator = (const Meter & ) = delete;
            Meter(Meter&& rhs) noexcept ;

            Meter(std::size_t sizeSoFar) noexcept;

            Meter operator [] (std::uint16_t newImplVersion);

            /**
             * measure instance of specific type
             */
            template <class member_type, typename... Args> Meter operator()(member_type type, Args&&... args) {
                sizeSoFar += type.measure(std::forward<Args>(args)...);
                return std::move(*this);
            }

            /**
             * Measure known size
            */
            Meter operator()(std::size_t size) {
                sizeSoFar += size;
                return std::move(*this);
            }

            operator std::size_t() const;
        };

        /**
         * Measure (safely) overlaid non-fixed members in specific instance
         */
        template <typename T> static Meter measureMembersOf(std::size_t base_size) {
            return Meter(base_size);
        }

    };

    template<typename T>
    constexpr std::size_t true_size_of() {
        return sizeof(T) - std::is_empty<T>();
    }

}
