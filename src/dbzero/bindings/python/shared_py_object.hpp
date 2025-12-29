// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <Python.h>
#include <iostream>
#include <thread>

// Extended inc-ref, handles additional ref-counter for memo objects
// must dec-ref with PyEXT_DECREF
#define PyEXT_INCREF(ptr) db0::python::incExtRef(ptr)
#define PyEXT_DECREF(ptr) db0::python::decExtRef(ptr)
// returns the number of extended references only
#define PyEXT_REFCOUNT(ptr, default) db0::python::getExtRefcount(ptr, default)

// take ownership of a PyObject (exception-safe)
#define Py_OWN(ptr) db0::python::shared_py_object<decltype(ptr)>(ptr, false)
#define Py_BORROW(ptr) db0::python::shared_py_object<decltype(ptr)>(ptr, true)
// converts a borrowed reference to a new reference
#define Py_NEW(ptr) Py_INCREF(ptr), ptr

namespace db0::python

{
    
    void incExtRef(PyObject *);
    void decExtRef(PyObject *);
    unsigned int getExtRefcount(PyObject *, unsigned int default_count);

    // @tparam ExtRef flag indicating if should be counted as an "external" reference
    template <typename T, bool ExtRef = false> class shared_py_object
    {
    public:
        using self_t = shared_py_object<T, ExtRef>;
        static constexpr bool hasExtRefs = ExtRef;

        inline shared_py_object() = default;
        inline shared_py_object(T py_object, bool incref = true)
            : m_py_object(py_object)
        {
            if (m_py_object) {
                if (incref) {
                    Py_INCREF(py_object);
                }
                if constexpr (ExtRef) {
                    PyEXT_INCREF(py_object);
                }
            }
        }
        
        // ExtRef -> non/ExtRef conversion
        template <bool U = !ExtRef, typename std::enable_if<U, int>::type = 0>
        shared_py_object(shared_py_object<T, true> &&other)
            : m_py_object(other.m_py_object)
        {
            static_assert(!ExtRef, "Member only available for non-ExtRef conversion");
            //static_assert(other.hasExtRefs, "Source object must have ExtRef");
            if (m_py_object) {
                PyEXT_DECREF(m_py_object);
            }
            other.m_py_object = nullptr;
        }
        
        shared_py_object(const self_t &other)
            : m_py_object(other.m_py_object)
        {
            if (m_py_object) {
                Py_INCREF(m_py_object);
                if constexpr (ExtRef) {
                    PyEXT_INCREF(m_py_object);
                }
            }
        }
        
        shared_py_object(self_t &&other)
            : m_py_object(other.m_py_object)
        {
            other.m_py_object = nullptr;
        }

        inline ~shared_py_object() {
            this->_destruct();
        }
        
        inline T get() const {
            return m_py_object;
        }

        inline T operator->() const {
            return m_py_object;
        }

        // same as operator->, but we assume non-null has been pre-verified (safer for debugging)
        inline T operator*() const
        {
            assert(m_py_object != nullptr);
            return m_py_object;
        }
        
        inline bool operator!() const {
            return m_py_object == nullptr;
        }
        
        // 'steal' a reference from the shared object
        // NOTE: steals only the regular reference, ext-refs cannot be stolen
        inline T steal()
        {
            if (!m_py_object) {
                return nullptr;
            }
            auto result = m_py_object;
            if constexpr (ExtRef) {
                PyEXT_DECREF(m_py_object);
            }
            m_py_object = nullptr;
            return result;
        }
        
        inline bool operator==(const self_t &other) const {
            return m_py_object == other.m_py_object;
        }
        
        inline bool operator!=(const self_t &other) const {
            return m_py_object != other.m_py_object;
        }
        
        self_t &operator=(const self_t &other)
        {
            this->_destruct();
            m_py_object = other.m_py_object;
            if (m_py_object) {
                Py_INCREF(m_py_object);
                if constexpr (ExtRef) {
                    PyEXT_INCREF(m_py_object);
                }
            }
            return *this;
        }
        
        self_t &operator=(self_t &&other)
        {
            this->_destruct();
            m_py_object = other.m_py_object;
            other.m_py_object = nullptr;
            return *this;
        }

        void reset()
        {
            this->_destruct();
            m_py_object = nullptr;            
        }
        
    private:
        friend class shared_py_object<T, !ExtRef>;
        T m_py_object = nullptr;

        void _destruct()
        {
            if (m_py_object) {
                if constexpr (ExtRef) {
                    PyEXT_DECREF(m_py_object);
                }
                Py_DECREF(m_py_object);                
            }
        }
    };
    
    // PyTypeObject specialization
    template <> class shared_py_object<PyTypeObject*>
    {
    public:
        inline shared_py_object() = default;
        inline shared_py_object(PyTypeObject *py_type, bool incref = true)
            : m_py_type(py_type)
        {
            // only heap types need to be incref-ed
            if (m_py_type && m_py_type->tp_flags & Py_TPFLAGS_HEAPTYPE && incref) {
                Py_INCREF(py_type);
            }
        }
        
        shared_py_object(const shared_py_object &other)
            : m_py_type(other.m_py_type)
        {
            if (m_py_type && m_py_type->tp_flags & Py_TPFLAGS_HEAPTYPE) {
                Py_INCREF(m_py_type);
            }
        }
        
        shared_py_object(shared_py_object &&other)
            : m_py_type(other.m_py_type)
        {
            other.m_py_type = nullptr;
        }

        inline ~shared_py_object()
        {
            if (m_py_type && m_py_type->tp_flags & Py_TPFLAGS_HEAPTYPE) {
                Py_DECREF(m_py_type);
            }            
        }
        
        inline PyTypeObject *get() const {
            return m_py_type;
        }

        inline PyTypeObject *operator*() const
        {
            assert(m_py_type != nullptr);
            return m_py_type;
        }
        
        inline bool operator!() const {
            return m_py_type == nullptr;
        }

        inline PyTypeObject* steal()
        {
            auto result = m_py_type;
            m_py_type = nullptr;
            return result;
        }

        shared_py_object &operator=(const shared_py_object &other)
        {
            this->~shared_py_object();
            m_py_type = other.m_py_type;
            if (m_py_type && m_py_type->tp_flags & Py_TPFLAGS_HEAPTYPE) {
                Py_INCREF(m_py_type);
            }
            return *this;
        }

        shared_py_object &operator=(shared_py_object &&other)
        {
            this->~shared_py_object();
            m_py_type = other.m_py_type;
            other.m_py_type = nullptr;
            return *this;
        }

    private:
        PyTypeObject *m_py_type = nullptr;
    };
    
    template <typename T, typename K> shared_py_object<T> shared_py_cast(shared_py_object<K> &&obj) {
        return shared_py_object<T>(static_cast<T>(obj.steal()), false);
    }
    
}

namespace std

{

    // Hash of the shared_py_object
    template <typename T> struct hash<db0::python::shared_py_object<T>>
    {
        std::size_t operator()(const db0::python::shared_py_object<T> &obj) const noexcept {
            return std::hash<T>()(obj.get());
        }
    };
    
    template <typename T> struct hash<db0::python::shared_py_object<T, true> >
    {
        std::size_t operator()(const db0::python::shared_py_object<T, true> &obj) const noexcept {
            return std::hash<T>()(obj.get());
        }
    };

}