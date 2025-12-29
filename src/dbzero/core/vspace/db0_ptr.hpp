// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/core/memory/Memspace.hpp>
#include <dbzero/core/serialization/Fixed.hpp>
#include <dbzero/core/compiler_attributes.hpp>

namespace db0

{

DB0_PACKED_BEGIN

    template <typename T> class db0_unique_ptr;
    template <typename T> struct db0_ptr_reinterpret_cast;

    /**
     * A convenience o_fixed class wrapping overlay object in templated v_object.
     */
    template <typename T>
    class DB0_PACKED_ATTR db0_ptr : public o_fixed<db0_ptr<T> > 
    {
    public:
        /**
         * Default constructor - creates null object.
         */
        db0_ptr() = default;

        /**
         * Constructor - creates instance of the v_object based on the underlying overlay object.
         */
        template<typename... Args>
        db0_ptr(db0::Memspace &memspace, Args&&... args)
        {
            T v_space_object(memspace, std::forward<Args>(args)...);
            m_address = v_space_object.getAddress();
        }
        
        db0_ptr(const T &v_space_object)
            : m_address(v_space_object.getAddress())
        {
        }

        /**
         * Creates instance of the v_object based on the underlying overlay object.
         */
        template<typename... Args>
        T create(db0::Memspace &memspace, Args&&... args)
        {
            if (m_address.isValid()) {
                THROWF(db0::InternalException) << "recreating of the v_object is not supported";
            }

            T v_space_object(memspace, std::forward<Args>(args)...);
            m_address = v_space_object.getAddress();
            return v_space_object;
        }

        /**
        * Returns instance of the v_object based on the underlying overlay object.
        */
        T operator()(db0::Memspace &memspace) const
        {
            if (!m_address.isValid()) {
                THROWF(db0::InternalException) << "Use of the uninitialized v_object";
            }
            return T(memspace.myPtr(m_address));
        }

        std::shared_ptr<T> getSharedPtr(db0::Memspace &memspace) const
        {
            if (!m_address.isValid()) {
                THROWF(db0::InternalException) << "use of the uninitialized v_object";
            }            
            return std::shared_ptr<T>(new T(memspace.myPtr(m_address)));
        }

        bool isNull() const {
            return !m_address.isValid();
        }

        /**
         * Returns true if db0_ptr holds valid - not null - pointer.
         */
        explicit operator bool() const {
            return m_address.isValid();
        }

        Address getAddress() const {
            return m_address;
        }

        /**
         * Assign new value (existing instance not destroyed)
         * @param v_space_object
         * @return
         */
        db0_ptr<T> &operator=(const T &v_space_object) {
            m_address = v_space_object.getAddress();
            return *this;
        }

        bool operator==(const db0_ptr<T> &other) const {
            return m_address == other.m_address;
        }

        bool operator==(const T &v_space_object) const {
            return m_address == v_space_object.getAddress();
        }

        bool operator<(const db0_ptr<T> &other) const {
            return m_address < other.m_address;
        }

        bool operator>(const db0_ptr<T> &other) const {
            return m_address > other.m_address;
        }

        bool operator!=(const db0_ptr<T> &other) const {
            return m_address != other.m_address;
        }

    protected:
        Address m_address = {};
        friend class db0_unique_ptr<T>;
        friend struct db0_ptr_reinterpret_cast<T>;
        friend struct std::hash<db0::db0_ptr<T>>;

        db0_ptr<T>(Address address)
            : m_address(address)
        {
        }
    };

    template <typename T> struct db0_ptr_reinterpret_cast
    {
        db0_ptr<T> operator()(Address address) {
            return db0_ptr<T>(address);
        }
    };

    /**
     * @brief db0_ptr version which will guarantee unique instance
     * @tparam T
     */
    template <typename T> class DB0_PACKED_ATTR db0_unique_ptr : public db0_ptr<T>
    {
    public :
        db0_unique_ptr() = default;

        template<typename... Args>
        db0_unique_ptr(db0::Memspace &memspace, Args&&... args)
            : db0_ptr<T>(memspace, std::forward<Args>(args)...)
        {
        }

        db0_unique_ptr(const T &v_space_object)
            : db0_ptr<T>(v_space_object)
        {
        }

        db0_unique_ptr(const db0_ptr<T> &ptr)
            : db0_ptr<T>(ptr)
        {
        }

        // assignment forbidden
        db0_unique_ptr<T> &operator=(const db0_unique_ptr<T> &) = delete;

        // copy forbidden
        db0_unique_ptr<T>(const db0_unique_ptr<T> &) = delete;

        // move constructor
        db0_unique_ptr<T>(db0_unique_ptr<T>&& other) = delete;

        // move assignment
        db0_unique_ptr<T>& operator=(db0_unique_ptr<T>&& other) = delete;

        /**
         * Assign new value, destroying existing instance
         */
        db0_unique_ptr<T> &operator=(const T &v_space_object)
        {
            if (this->m_address != v_space_object.getAddress()) {
                this->destroy(v_space_object.getMemspace());
                this->m_address = v_space_object.getAddress();
            }
            return *this;
        }

        /**
         * Assign new value, destroying existing instance
         */
        db0_unique_ptr<T> &assign(db0::Memspace &memspace, db0_unique_ptr<T> &&ptr)
        {
            if (this->m_address != ptr.getAddress()) {
                this->destroy(memspace);
                this->m_address = ptr.release().getAddress();
            }
            return *this;
        }

        /**
         * Release to regular db0_ptr
         * @return
         */
        db0_ptr<T> release()
        {
            auto result = db0_ptr<T>(this->m_address);
            this->m_address = {};
            return result;
        }

        /**
         * Reset instance
         */
        void reset(db0::Memspace &memspace) 
        {
            if (this->m_address) {
                this->destroy(memspace);
                this->m_address = {};
            }
        }

        /**
         * Destroys created object.
         */
        void destroy(db0::Memspace &memspace) const
        {
            if (this->m_address.isNull()) {
                return;
            }

            T v_space_object = (*this)(memspace);
            v_space_object.destroy();
        }

        bool operator==(const db0_unique_ptr<T> &other) const {
            return this->m_address == other.m_address;
        }
    };

    template <typename T, typename... Args> db0_ptr<T> make_db0_ptr(db0::Memspace &memspace, Args&&... args) {
        return db0_ptr<T>(memspace, std::forward<Args>(args)...);
    }

    template <typename T, typename... Args> db0_unique_ptr<T> make_db0_unique_ptr(db0::Memspace &memspace, Args&&... args) {
        return db0_unique_ptr<T>(memspace, std::forward<Args>(args)...);
    }

DB0_PACKED_END

} 

namespace std

{

    // db0_ptr hash function
    template <typename T> struct hash<db0::db0_ptr<T> > {
        std::size_t operator()(const db0::db0_ptr<T> &ptr) const {
            return std::hash<std::uint64_t>()(ptr.getAddress().getValue());
        }
    };
    
}

