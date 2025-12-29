// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <memory>
#include <initializer_list>
#include <cassert>

#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/core/utils/shared_void.hpp>

namespace db0

{

	template <int I, typename T> struct morphology;

    template <typename Interface>
    using getInterfacePtr = Interface (*)(std::shared_ptr<void>);

	/**
	 * Interface factory function
	 */
	template <typename Interface, typename T> Interface createInterface(std::shared_ptr<void>);
    template <typename Interface, typename Head, typename ... T> void createFactories(getInterfacePtr<Interface>*);

	/**
	 * Create interface factories and write to specific array
	 * @param where array to write class factories (must be size sizeof...(T))
	 */
	// template <typename Interface, typename... T> void createFactories(getInterfacePtr<Interface> *where);

	/**
	 * Multimorph is class which enables multi-form storage in BN and providing access
	 * to actual implementation via common Interface type
	 * Interface - should be collection of function pointers (which can be called with different implementations
	 */
	template <typename Interface, typename... T> class Multimorph 
    {
		using self_t = Multimorph<Interface, T...>;
		// actual instance of specific morphology
		std::shared_ptr<void> m_instance;
		int m_morph_id = -1;
		// interface factories related to specific morphology
		getInterfacePtr<Interface> m_interfaces[sizeof...(T)];

		static int max_id() {
			return sizeof...(T) - 1;
		}

	public :

		Multimorph()
		{
			// initialize interface factories
			createFactories<Interface, T...>(m_interfaces);
		}

		template <int I> typename morphology<I, self_t>::type &ref()
		{
			assert(m_morph_id==I);
			return *reinterpret_cast<typename morphology<I, self_t>::type*>(m_instance.get());
		}

		int getMorphologyId() const {
			return m_morph_id;
		}

		template <int I, typename... Args> void create(Args&&... args)
		{
            using type_I = typename morphology<I, self_t>::type;
			m_instance = db0::make_shared_void<type_I>(std::forward<Args>(args)...);
			m_morph_id = I;
		}

		/**
         * Construct morphology passing brace enclosed list
         */
		template <int I, typename item_t> void create(std::initializer_list<item_t> args) 
		{
            using type_I = typename morphology<I, self_t>::type;
            // create with deleter
            m_instance = db0::make_shared_void<type_I>(args);
			m_morph_id = I;
		}

		/**
         * Change morphology to next type using args (existing morphology destroyed)
         * K - current morphology ID
         * @return interface of the resulting morphology
         */
		template <int I, int K, typename... Args> Interface morphTo(Args&&... args) 
		{
			if (isNull()) {
				THROWF(db0::InternalException)
					<< "Multimorph instance has not been initialized, internal error";
			}
			assert(m_morph_id==K);
			assert(m_morph_id < self_t::max_id());
			using type_I = typename morphology<I, self_t>::type;
			// create with deleter
			m_instance = db0::make_shared_void<type_I>(std::forward<Args>(args)...);
			m_morph_id = I;
			return m_interfaces[m_morph_id](m_instance);
		}

		/**
         * Change morphology to next type using copy constructor + additional args (args)
         * K - current morphology ID
         * @return interface of the resulting morphology
         */
		template <int I, int K, typename... Args> Interface morphToWithCopy(Args&&... args)
		{
			if (isNull()) {
				THROWF(db0::InternalException)
					<< "Multimorph instance has not been initialized, internal error";
			}
			assert(m_morph_id==K);
			assert(m_morph_id < self_t::max_id());
			// copy constructor accepting additional arguments "Args" must be available (otherwise will not compile)
            using type_I = typename morphology<I, self_t>::type;
			m_instance = db0::make_shared_void<type_I>(ref<K>(), std::forward<Args>(args)...);
			m_morph_id = I;
			return m_interfaces[m_morph_id](m_instance);
		}

		/**
         * Assume current morphology is I - 1
         */
		template <int I, typename... Args> Interface morphToWithCopy(Args&&... args) {
			return morphToWithCopy<I, I - 1, Args...>(std::forward<Args>(args)...);
		}

		/**
         * Assume current morphology is I - 1
         */
		template <int I, typename... Args> Interface morphTo(Args&&... args) {
			return morphTo<I, I - 1, Args...>(std::forward<Args>(args)...);
		}

		/**
         * Pull interface for current morphology
         * Interface will be created passing reference to current morphology
         */
		Interface getInterface() {
			assert(!isNull());
			return m_interfaces[m_morph_id](m_instance);
		}

		bool isNull() const {
			return (m_morph_id < 0);
		}

		std::shared_ptr<void> getInstance () const {
		    return m_instance;
		}
	};
	
	// recursive case
	template <int I, typename Interface, typename Head, typename... Tail> struct morphology<I, Multimorph<Interface, Head, Tail...> >
		: morphology<I - 1, Multimorph<Interface, Tail...> > { };

	// base case
	template <typename Interface, typename Head, typename... Tail>
	struct morphology<0, Multimorph<Interface, Head, Tail...> > {
		typedef Head type;
	};

	template <typename Interface, typename T>
	inline Interface createInterface(std::shared_ptr<void> ref) {
		return Interface(static_cast<T*>(nullptr), ref);
	}

    // base case
    template <typename Interface>
    void createFactories(getInterfacePtr<Interface>*) {
    }

	// recursive case
	template <typename Interface, typename Head, typename ... T>
	void createFactories(getInterfacePtr<Interface>* where) {
		*where = createInterface<Interface, Head>; // factory method pointer
		createFactories<Interface, T...>(where + 1);
	}

} 
