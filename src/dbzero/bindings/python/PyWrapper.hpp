#pragma once

#include <Python.h>
#include <cstdint>
#include <memory>

namespace db0::python 

{

    /**
     * Adds a mixed-in (but dynamically initialized)
     * member of type T into the PyObject struct.
     **/
    template <typename T> struct PyWrapper: public PyObject
    {
        inline T &ext() {
            // calculate instance offset
            return *reinterpret_cast<T*>((char*)this + Py_TYPE(this)->tp_basicsize - sizeof(T));
        }

        const T &ext() const {
            return *reinterpret_cast<const T*>((char*)this + Py_TYPE(this)->tp_basicsize - sizeof(T));
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

    template <typename T> struct PySharedWrapper: public PyWrapper<Shared<T> >
    {
        using super_t = PyWrapper<Shared<T> >;
        inline T &ext() {
            return *super_t::ext();
        }

        const T &ext() const {
            return *super_t::ext();
        }

        template <typename... Args> void makeNew(Args &&...args) {
            Shared<T>::makeNew(&super_t::ext(), std::make_shared<T>(std::forward<Args>(args)...));
        }
        
        void makeNew(std::shared_ptr<T> ptr) {
            Shared<T>::makeNew(&super_t::ext(), ptr);
        }
    };
    
}