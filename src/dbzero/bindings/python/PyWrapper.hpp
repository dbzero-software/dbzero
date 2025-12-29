// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <Python.h>
#include <cstdint>
#include <memory>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/object_model/ObjectCatalogue.hpp>

namespace db0::python 

{
        
    /**
     * Adds a mixed-in (but dynamically initialized)
     * member of type T into the PyObject struct.
     **/
    template <typename T, bool is_object_base=true, typename BaseT = PyObject>
    struct PyWrapper: public BaseT
    {
        // placeholder for the actual instance (since we're unable to calculate sizeof at compile time)
        std::array<char, 1u> m_ext_storage;
        using ExtT = T;

        inline const T &ext() const {
            return *reinterpret_cast<const T*>(&m_ext_storage);
        }
        
        inline T &modifyExt()
        {
            // calculate instance offset
            auto &result = *reinterpret_cast<T*>(&m_ext_storage);
            // only for ObjectBase derived classes
            if constexpr (is_object_base) {
                // the implementation registers the underlying object for detach (on rollback)
                // but only if atomic operation is in progress
                result.beginModify(this);
            }
            return result;
        }

        static constexpr std::size_t sizeOf() {
            // adjust size to include actual T size 
            return sizeof(PyWrapper<T, is_object_base, BaseT>) + sizeof(T) - sizeof(m_ext_storage);
        }
        
        void destroy() {
            ext().~T();
        }
        
        // Construct a new wrapped instance of type T
        template <typename... Args> T &makeNew(Args &&...args) 
        {
            auto &result = modifyExt();
            new ((void*)&result) T(std::forward<Args>(args)...);
            return result;
        }
        
        // Unload a new wrapped instance of type T
        template <typename... Args> const T &unload(Args &&...args) const
        {
            const auto &ext = this->ext();
            // NOTE: this is a dbzero non-mutating operation (thus use of ext)
            new ((void*)&ext) T(std::forward<Args>(args)...);
            return ext;
        }
    };
    
    struct PyObjectWithDict: public PyObject
    {        
        PyObject *m_py_dict = nullptr;
    };
    
    // This is a wrapper with additional __dict__ slot
    // which is required for Python versions before 3.11 (before managed dicts were introduced)
    template <typename T, bool is_object_base=true>    
    struct PyWrapperWithDict: public PyWrapper<T, is_object_base, PyObjectWithDict>    
    {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Winvalid-offsetof"
        static constexpr std::size_t getDictOffset() {
            return offsetof(PyObjectWithDict, m_py_dict);
        }
#pragma GCC diagnostic pop
    };
    
    template <typename T> struct Shared
    {
        std::shared_ptr<T> m_ptr;
        Shared(std::shared_ptr<T> ptr): m_ptr(ptr) {}

        T *operator->() {
            return m_ptr.get();
        }

        const T *operator->() const {
            return m_ptr.get();
        }

        T &operator*() {
            return *m_ptr;
        }

        const T &operator*() const {
            return *m_ptr;
        }

        static void makeNew(void *at_ptr, std::shared_ptr<T> ptr) {
            new (at_ptr) Shared(ptr);
        }
    };
    
    template <typename T, bool is_object_base=true>
    struct PySharedWrapper: public PyWrapper<Shared<T>, is_object_base>
    {
        using super_t = PyWrapper<Shared<T>, is_object_base>;

        inline T &modifyExt()
        {
            auto &_ptr = super_t::modifyExt().m_ptr;
            if (!_ptr) {
                THROWF(db0::InternalException) << "Instance of type: " << db0::object_model::get_type_name<T>() << " is no longer accessible";
            }
            return *_ptr;            
        }

        inline const T &ext() const
        {
            auto &_ptr = super_t::ext().m_ptr;
            if (!_ptr) {
                THROWF(db0::InternalException) << "Instance of type: " << db0::object_model::get_type_name<T>() << " is no longer accessible";
            }
            return *_ptr;
        }
        
        template <typename... Args> void makeNew(Args &&...args) {
            Shared<T>::makeNew(&super_t::modifyExt(), std::make_shared<T>(std::forward<Args>(args)...));
        }
        
        template <typename AsType, typename... Args> void makeNewAs(Args &&...args) {
            Shared<T>::makeNew(&super_t::modifyExt(), std::make_shared<AsType>(std::forward<Args>(args)...));
        }

        void makeNew(std::shared_ptr<T> ptr) {
            // note, here we don't call modifyExt, as the instance is already created
            Shared<T>::makeNew((void*)&super_t::ext(), ptr);
        }
        
        std::shared_ptr<T> getSharedPtr() const {
            return super_t::ext().m_ptr;
        }
        
        void reset() {
            super_t::modifyExt().m_ptr.reset();
        }
    };
    
    // Common drop implementation for wrapper db0 types
    // @tparam T underlying db0 type (derived from ObjectBase) must implement a static makeNull method
    template <typename T> void PyWrapper_drop(T *ptr)    
    {
        using ExtT = typename T::ExtT;
        // db0 instance does not exist
        if (!ptr->ext().hasInstance()) {
            return;
        }
        
        if (ptr->ext().hasRefs()) {
            PyErr_SetString(PyExc_RuntimeError, "delete failed: object has references");
            return;    
        }
        
        // create a null placeholder in place of the original instance to mark as deleted
        auto &lang_cache = ptr->ext().getFixture()->getLangCache();
        ptr->destroy();
        // make null instance (no construction arguments provided)
        ptr->makeNew();        
        // remove instance from the lang cache
        lang_cache.erase(ptr->ext().getAddress());
    }
    
}