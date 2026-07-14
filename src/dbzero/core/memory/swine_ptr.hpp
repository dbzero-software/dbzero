// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdint>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/core/serialization/Types.hpp>
#include <atomic>
#include <cassert>

namespace db0

{

    template <typename RefCountT> struct o_swine_count:
    public o_fixed<o_swine_count<RefCountT> >
    {
        std::atomic<std::uint64_t> m_ref_count = 0;
        std::atomic<std::uint64_t> m_weak_count = 0;
    };
    
    template <typename T, typename RefCountT = std::uint64_t> class weak_swine_ptr;

    /**
     * A simple smart-pointer which combines functionality or
     * (S)hared_ptr / (W)aeak_ptr and (IN)trusive_ptr (E) just added to sound funny
     * The original purpose of swine_ptr was to allow storing raw Fixture* pointers in objects which might
     * outlive the Fixture instance they point to
     */
    template <typename T, typename RefCountT = std::uint64_t> class swine_ptr
    {
    public: 
        using SwineCountT = o_swine_count<RefCountT>;

        swine_ptr() = default;

        swine_ptr(SwineCountT *count_ptr)
            : m_count_ptr(count_ptr)
        {
            if (m_count_ptr != nullptr) {
                ++(m_count_ptr->m_ref_count);
            }            
        }

        swine_ptr(const swine_ptr<T, RefCountT> &other)
            : m_count_ptr(other.m_count_ptr)
        {
            if (m_count_ptr != nullptr) {
                ++(m_count_ptr->m_ref_count);
            }            
        }

        ~swine_ptr()
        {
            if (m_count_ptr == nullptr) {
                return;
            }
            assert(m_count_ptr->m_ref_count > 0);
            if (--(m_count_ptr->m_ref_count) == 0) {
                // increment weak ref because T may hold dependent weak references
                ++(m_count_ptr->m_weak_count);
                T *ptr = reinterpret_cast<T*>(m_count_ptr + 1);
                // destroy T instance but hold the buffer allowing weak_ptr owners
                // to still keep T* pointer (but not access it)
                ptr->~T();
                // release weak ref
                if (--(m_count_ptr->m_weak_count) == 0) {
                    // release allocated memory if there're no weak_ptr owners
                    delete[] reinterpret_cast<std::byte*>(m_count_ptr);
                }
            }
        }
        
        inline T *get() const
        {
            if (m_count_ptr == nullptr) {
                return nullptr;
            }
            if (m_count_ptr->m_ref_count == 0) {
                return nullptr;
            }
            assert(m_count_ptr->m_ref_count > 0);
            return reinterpret_cast<T*>(m_count_ptr + 1);
        }

        inline T *operator->() const {
            return get();
        }
        
        bool operator==(const swine_ptr<T> &other) const {
            return m_count_ptr == other.m_count_ptr;
        }
        
        void operator=(const swine_ptr<T> &other)
        {
            this->~swine_ptr();
            m_count_ptr = other.m_count_ptr;
            if (m_count_ptr != nullptr) {
                ++(m_count_ptr->m_ref_count);
            }
        }
        
        bool operator!=(const swine_ptr<T> &other) const {
            return m_count_ptr != other.m_count_ptr;
        }
        
        T &operator*() const {
            return *get();
        }

        bool operator!() const {
            return m_count_ptr == nullptr;
        }

        operator bool() const {
            return m_count_ptr != nullptr;
        }
        
        /**
         * Take weak reference to T, which needs to be released with release_weak
        */
        void take_weak()
        {
            if (m_count_ptr == nullptr) {
                return;
            }
            ++(m_count_ptr->m_weak_count);
        }

        static void take_weak(T *ptr)
        {
            SwineCountT *count_ptr = reinterpret_cast<SwineCountT*>(ptr) - 1;
            ++(count_ptr->m_weak_count);
        }

        static void release_weak(T *ptr)
        {
            SwineCountT *count_ptr = reinterpret_cast<SwineCountT*>(ptr) - 1;
            if (--(count_ptr->m_weak_count) == 0) {
                if (count_ptr->m_ref_count == 0) {
                    // release allocated memory
                    delete[] reinterpret_cast<std::byte*>(count_ptr);
                }
            }
        }

        /**
         * May return nullptr if the underlying object has been deleted
        */
        static swine_ptr<T, RefCountT> lock_weak(T *ptr)
        {
            SwineCountT *count_ptr = reinterpret_cast<SwineCountT*>(ptr) - 1;
            if (count_ptr->m_ref_count == 0) {
                return {};
            }
            return swine_ptr<T, RefCountT>(count_ptr);
        }

        // The throwing version of lock_weak
        static swine_ptr<T, RefCountT> safe_lock_weak(T *ptr)
        {
            SwineCountT *count_ptr = reinterpret_cast<SwineCountT*>(ptr) - 1;
            if (!count_ptr->m_ref_count) {
                THROWF(db0::InputException) << "Object no longer available";
            }
            return swine_ptr<T, RefCountT>(count_ptr);
        }
        
        RefCountT use_count() const
        {
            if (m_count_ptr == nullptr) {
                return 0;
            }
            return m_count_ptr->m_ref_count;
        }
        
        // weak_swine_ptr cast operator
        operator weak_swine_ptr<T, RefCountT>() {
            return weak_swine_ptr<T, RefCountT>(*this);
        }
        
    protected:
        friend class weak_swine_ptr<T, RefCountT>;

        // Compares if the weak pointer originates from the same swine_ptr<T>
        bool compare_weak(T *ptr) const
        {
            if (m_count_ptr == nullptr) {
                return false;
            }
            SwineCountT *count_ptr = reinterpret_cast<SwineCountT*>(ptr) - 1;
            return m_count_ptr == count_ptr;
        }
        
    private:
        SwineCountT *m_count_ptr = nullptr;
    };
    
    template <typename T, typename RefCountT> class weak_swine_ptr
    {
    public:
        weak_swine_ptr() = default;
        weak_swine_ptr(swine_ptr<T, RefCountT> &ptr)
            : m_ptr(ptr.get())
        {
            ptr.take_weak();
        }
        
        weak_swine_ptr(const weak_swine_ptr<T, RefCountT> &other)
            : m_ptr(other.m_ptr)
        {
            if (m_ptr != nullptr) {
                swine_ptr<T, RefCountT>::take_weak(m_ptr);
            }
        }

        weak_swine_ptr(weak_swine_ptr<T, RefCountT> &&other)
            : m_ptr(other.m_ptr)
        {
            other.m_ptr = nullptr;
        }

        ~weak_swine_ptr()
        {
            if (m_ptr != nullptr) {
                swine_ptr<T, RefCountT>::release_weak(m_ptr);
            }
        }
        
        swine_ptr<T, RefCountT> lock() const noexcept {
            return swine_ptr<T, RefCountT>::lock_weak(m_ptr);
        }
        
        // the "lock" version raising an exception if the object is not available
        swine_ptr<T, RefCountT> safe_lock() const {
            return swine_ptr<T, RefCountT>::safe_lock_weak(m_ptr);
        }

        void operator=(const weak_swine_ptr<T, RefCountT> &other)
        {
            this->~weak_swine_ptr();
            m_ptr = other.m_ptr;
            if (m_ptr != nullptr) {
                swine_ptr<T, RefCountT>::take_weak(m_ptr);
            }
        }
        
        void operator=(const swine_ptr<T, RefCountT> &other)
        {
            this->~weak_swine_ptr();
            m_ptr = other.get();
            if (m_ptr != nullptr) {
                swine_ptr<T, RefCountT>::take_weak(m_ptr);
            }
        }

        // is default operator (aka NOT initialized)
        bool operator!() const {
            return m_ptr == nullptr;
        }

        bool operator==(const weak_swine_ptr<T, RefCountT> &other) const {
            return m_ptr == other.m_ptr;
        }
        
        bool operator!=(const weak_swine_ptr<T, RefCountT> &other) const {
            return m_ptr != other.m_ptr;
        }

        bool operator==(const swine_ptr<T, RefCountT> &other) const {
            return other.compare_weak(m_ptr);
        }
        
        bool operator!=(const swine_ptr<T, RefCountT> &other) const {
            return !other.compare_weak(m_ptr);
        }

    private:
        mutable T *m_ptr = nullptr;
    };
    
    template <typename T, typename... Args> swine_ptr<T> make_swine(Args&&... args)
    {
        using SwineCountT = typename swine_ptr<T>::SwineCountT;
        void *ptr = new std::byte[SwineCountT::sizeOf() + sizeof(T)];
        // construct T using placement new
        new (ptr) SwineCountT();
        new (static_cast<std::byte*>(ptr) + SwineCountT::sizeOf()) T(std::forward<Args>(args)...);
        return swine_ptr<T>(reinterpret_cast<SwineCountT*>(ptr));
    }

}