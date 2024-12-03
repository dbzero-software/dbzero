#pragma once

#include <Python.h>
#include <cstdint>
#include <memory>
#include <dbzero/core/exception/Exceptions.hpp>

namespace db0::python 

{

    /**
     * Adds a mixed-in (but dynamically initialized)
     * member of type T into the PyObject struct.
     **/
    template <typename T, bool is_object_base=true> struct PyWrapper: public PyObject
    {
        using ExtT = T;

        inline const T &ext() const {
            return *reinterpret_cast<const T*>((char*)this + Py_TYPE(this)->tp_basicsize - sizeof(T));
        }
        
        inline T &modifyExt()
        {
            // calculate instance offset
            auto &result = *reinterpret_cast<T*>((char*)this + Py_TYPE(this)->tp_basicsize - sizeof(T));
            // only for ObjectBase derived classes
            if constexpr (is_object_base) {
                // the implementation registers the underlying object for detach (on rollback)
                // but only if atomic operation is in progress
                result.beginModify(this);
            }
            return result;
        }

        static constexpr std::size_t sizeOf() {
            return sizeof(PyObject) + sizeof(T);
        }
        
        void destroy() {
            ext().~T();
        }
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
                THROWF(db0::InternalException) << "Instance of type: " << typeid(T).name() << " is no longer accessible";
            }
            return *_ptr;            
        }

        inline const T &ext() const
        {
            auto &_ptr = super_t::ext().m_ptr;
            if (!_ptr) {
                THROWF(db0::InternalException) << "Instance of type: " << typeid(T).name() << " is no longer accessible";
            }
            return *_ptr;
        }
        
        template <typename... Args> void makeNew(Args &&...args) {
            Shared<T>::makeNew(&super_t::modifyExt(), std::make_shared<T>(std::forward<Args>(args)...));
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
        ExtT::makeNull((void*)(&ptr->ext()));
        // remove instance from the lang cache
        lang_cache.erase(ptr->ext().getAddress());
    }    
    
}