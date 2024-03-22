#pragma once

#include <unordered_set>

#include "mb_index_def.hpp"
#include "bindex_types.hpp"
#include "bindex_interface.hpp"
#include "bindex_iterator.hpp"

namespace db0

{

    class Memspace;

	/**
	 * Morphing BIndex is data structure implementation which combines multiple
	 * implementations specialized in small or large collections and which can dynamically change its morphology over time
	 * (using Multimorph utility)
	 * IMPORTANT NOTICE: one has to store both type and pointer to use proper morphology
	 * @tparam AddrT must be convertible to/from ItemT
	*/
	template <typename ItemT, typename AddrT = std::uint64_t, typename Compare = std::less<ItemT> >
	class MorphingBIndex
    {
	public :
        using self_t = MorphingBIndex<ItemT, Compare>;
        using definition_t = bindex::MorphingBIndexDefinition<ItemT, AddrT, Compare>;
        using item_t = typename definition_t::item_t;
        using item_comp_t = typename definition_t::item_comp_t;

        using interface_t = bindex::interface::Impl<definition_t>;
        using iterator_t = bindex::iterator::Impl<item_t>;
		using CallbackT = std::function<void(item_t)>;

        /**
         * 0 = empty_index (0 elements)
         * 1 = itty_index (1 element stored in place of pointer)
         * 2 = array2_t (2 elements)
         * 3 = array3_t (3 elements)
         * 4 = array4_t (4 elements)
         * 5 = vector_t (up to N = 32 elements)
         * 6 = bindex_t (from N + 1 to no limit)
         */
        using morph_t = typename definition_t::Containers::template Multimorph<interface_t>;

        // iterator types
        using iterator_types = std::tuple<
            bindex::iterator::empty_joinable_const<AddrT> ,
            bindex::iterator::itty_joinable_const<item_t, AddrT, item_comp_t> ,
            bindex::iterator::array_joinable_const<item_t, 2, AddrT, item_comp_t> ,
            bindex::iterator::array_joinable_const<item_t, 3, AddrT, item_comp_t> ,
            bindex::iterator::array_joinable_const<item_t, 4, AddrT, item_comp_t> ,
            bindex::iterator::sorted_vector_joinable_const<item_t, AddrT, item_comp_t> ,
            bindex::iterator::bindex_joinable_const<item_t, AddrT, item_comp_t>
        >;
		
		/**
         * Create instance as null / invalid
         */
		MorphingBIndex() = default;

		/**
         * Construct new empty using specific initial morphology (allowed initial morphology: empty, sorted_vector, bindex)
         * @param SV_SIZE_LIMIT maximum number of elements to store
         * in type v_sorted_vector (more than N will be stored in v_bindex) \
         * actual size limit may be greater than SV_SIZE_LIMIT
         * @param memspace location where all index data should be placed
         */
		MorphingBIndex(Memspace &memspace, bindex::type initial_type = bindex::empty, std::uint32_t SV_SIZE_LIMIT = 48)
			: m_memspace_ptr(&memspace)
			, m_sv_limit(suggestMaxSize(SV_SIZE_LIMIT))
		{
			if (initial_type==bindex::empty) {
				createNew<bindex::empty>();
			} else if (initial_type==bindex::sorted_vector) {
				createNew<bindex::sorted_vector>();
			} else if (initial_type==bindex::bindex) {
				createNew<bindex::bindex>();
			} else {
				assert(false);
				THROWF(db0::InternalException) << "not allowed as initial morphology:" << initial_type;
			}
		}
        
		/**
		 * Create as IttyIndex, initialize with initial element
		 * @param memspace
		 * @param key initial element
		 * @param SV_SIZE_LIMIT
		 */
		MorphingBIndex(Memspace &memspace, const item_t &key, std::uint32_t SV_SIZE_LIMIT = 48)
			: m_memspace_ptr(&memspace)
			, m_sv_limit(suggestMaxSize(SV_SIZE_LIMIT))
		{
		    createNew<bindex::itty>(key);
		}
		
        MorphingBIndex(MorphingBIndex &&other)
            : m_memspace_ptr(other.m_memspace_ptr)
            , m_sv_limit(other.m_sv_limit)
            , m_morph(std::move(other.m_morph))
            , m_interface(std::move(other.m_interface))
        {
        }

        MorphingBIndex(const MorphingBIndex &other)
            : m_memspace_ptr(other.m_memspace_ptr)
            , m_sv_limit(other.m_sv_limit)
            , m_morph(other.m_morph)
            , m_interface(other.m_interface)
        {
        }

        MorphingBIndex &operator=(const MorphingBIndex &other)
		{
            this->m_memspace_ptr = other.m_memspace_ptr;
            this->m_sv_limit = other.m_sv_limit;
            this->m_morph = other.m_morph;
            this->m_interface = other.m_interface;
            return *this;
        }

        MorphingBIndex &operator=(MorphingBIndex &&other)
		{
            this->m_memspace_ptr = other.m_memspace_ptr;
            this->m_sv_limit = other.m_sv_limit;
            this->m_morph = std::move(other.m_morph);
            this->m_interface = std::move(other.m_interface);
            return *this;
		}

		/**
         * Get actual storage index implementation type
         */
		bindex::type getIndexType() const {
			return static_cast<bindex::type>(m_morph.getMorphologyId());
		}

		/// check for valid non-null instance
		operator bool() const {
		    return m_memspace_ptr;
		}

		/**
         * Opens instance existing under pointer (ptr)
         * @param ptr instance pointer (note that no mptr will exist for empty or itty index)
         * @param type instance type (must be known)
         */
		MorphingBIndex(Memspace &memspace, AddrT addr, bindex::type type, std::uint32_t SV_SIZE_LIMIT = 48)
			: m_memspace_ptr(&memspace)
			, m_sv_limit(suggestMaxSize(SV_SIZE_LIMIT))
		{
			openExisting(addr, type);
		}

		/**
         * Assess index type by number of elements for it to hold
         */
		bindex::type assessIndexType(std::size_t final_size) const
		{
			// depending on final size assess resulting index type
			switch (final_size)
			{
				case 0:
					return bindex::empty;

				case 1:
					return bindex::itty;

				case 2:
					return bindex::array_2;

				case 3:
					return bindex::array_3;

				case 4:
					return bindex::array_4;

				default:
					return (final_size <= m_sv_limit) ? bindex::sorted_vector : bindex::bindex;
			}
		}
		
		/**
         * @param key_count number of unique keys to erase
         */
		bindex::type getIndexTypeAfterErase(std::size_t key_count)
		{
			bindex::type index_type = getIndexType();
			if (index_type!=bindex::bindex) {
				size_t final_size = m_interface.size() - key_count;
				return assessIndexType(final_size);
			}
			return bindex::bindex;
		}

		/**
         * @param unique_count number of unique items attempted to insert
         * @return type after insertion / number of unique items (up to N_SV_LIMIT) in source collection
         */
		bindex::type getIndexTypeAfterInsertion(std::size_t unique_count)
        {
			bindex::type index_type = getIndexType();
			if (index_type!=bindex::bindex) {
				size_t final_size = m_interface.size() + unique_count;
				return assessIndexType(final_size);
			}
			return bindex::bindex;
		}
		
		/**
		 * Update existing item without modifying it's key part (important)
		 * this operation does not affect the collection type
		 * @return false if item does not exist / not updated
		*/
		bool updateExisting(const item_t &new_item)
		{
            return m_interface.updateExisting(new_item);
		}

		/**
		 * Erase single element if this exists
		 */
        void erase(const item_t &item)
        {
            bindex::type type = getIndexType();
            // if type is either v_bindex or v_sorted_vector then just erase (no morphing)
            if (type==bindex::bindex || type==bindex::sorted_vector) {
                return m_interface.erase(item);
            } else {
                // erase key if it exists in collection
                if (m_interface.findExisting(item)) {
                    bindex::type type_after_erase = getIndexTypeAfterErase(1u);
                    if (type_after_erase==bindex::empty) {
                        // morph to empty_index (this will erase all elements)
                        morphToEmpty();
                    } else if (type_after_erase==bindex::itty) {
                        // morph to itty_index passing 1 element
                        morphToAndErase<bindex::itty>(item);
                    } else if (type_after_erase==bindex::array_2) {
                        // morph and insert unique
                        morphToAndErase<bindex::array_2>(item);
                    } else if (type_after_erase==bindex::array_3) {
                        morphToAndErase<bindex::array_3>(item);
                    } else if (type_after_erase==bindex::array_4) {
                        morphToAndErase<bindex::array_4>(item);
                    }
                }
            }
		}
		
		/**
		 * Check if item exists in collection
		*/
		bool contains(const item_t &item) const
		{
			return m_interface.findExisting(item);
		}

		/**
		 * Find item by it's key part and update with full value if found
		 * @return true if item was found and retrieved
		 */		
		bool findOne(item_t &item) const
		{
			return m_interface.findOne(item);
		}
		
		/**
         * Erase multiple elements (if they exist) in single call
         * @return Number of elements erased
         */
		template<typename InputIterator>
		std::size_t bulkErase(InputIterator begin, InputIterator end, CallbackT *callback_ptr = nullptr)
		{
			if (begin == end) {
                return 0;
            }

            bindex::type type = getIndexType();
            // if type is either v_bindex or v_sorted_vector then just erase (no morphing)
            if (type == bindex::bindex || type == bindex::sorted_vector) {
                return m_interface.bulkErase(begin, end, callback_ptr);
            } else {
                // count the number of existing keys
                std::size_t key_count = m_interface.countExisting(begin, end, m_sv_limit + 1);
                // operation only makes sense if there're any elements to remove
                if (key_count > 0) {
                    bindex::type type_after_erase = getIndexTypeAfterErase(key_count);
                    if (type_after_erase == bindex::empty) {
                        // morph to empty_index (this will erase all elements)
                        morphToEmpty();
						if (callback_ptr) {
							for (auto it = begin; it != end; ++it) {
								(*callback_ptr)(*it);
							}
						}
                    } else if (type_after_erase==bindex::itty) {
                        // morph to itty_index passing 1 element
                        morphToAndErase<bindex::itty>(begin, end, callback_ptr);
                    } else if (type_after_erase==bindex::array_2) {
                        // morph and insert unique
                        morphToAndErase<bindex::array_2>(begin, end, callback_ptr);
                    } else if (type_after_erase==bindex::array_3) {
                        morphToAndErase<bindex::array_3>(begin, end, callback_ptr);
                    } else if (type_after_erase==bindex::array_4) {
                        morphToAndErase<bindex::array_4>(begin, end, callback_ptr);
                    }
                }
                return key_count;
            }
		}
		
		/**
		 * @return true if the underlying type (and address) has changed
		 */		
		bool insert(const item_t &item)
        {
            bindex::type type = getIndexType();
            bindex::type type_after_insertion = getIndexTypeAfterInsertion(1u);
            /// only morph when upgrade is necessary, do not degrade type when inserting
			bool result = type_after_insertion > type;
            if (result) {
                // just morph to variable length collection
                if (type_after_insertion==bindex::bindex) {
                    morphTo<bindex::bindex>();
                } else if (type_after_insertion==bindex::sorted_vector) {
                    morphTo<bindex::sorted_vector>();
                } else {
                    // morphing to fixed size collection will require passing elements
                    if (type_after_insertion==bindex::itty) {
                        // morph to itty_index passing this 1 element
                        assert(type==bindex::empty);
                        morphToAndInsert<bindex::itty>(item);
                    } else if (type_after_insertion==bindex::array_2) {
                        // morph and insert unique
                        morphToAndInsert<bindex::array_2>(item);
                    } else if (type_after_insertion==bindex::array_3) {
                        morphToAndInsert<bindex::array_3>(item);
                    } else if (type_after_insertion==bindex::array_4) {
                        morphToAndInsert<bindex::array_4>(item);
                    } else {
                        assert(false);
                    }
                    return result;
                }
            }
            m_interface.insert(item);
			return result;
		}
		
		/**
		 * @param callback optional function to be called for each unique inserted item
         * @return items requested / items actually inserted
         */
		template <typename InputIterator>
		std::pair<std::uint32_t, std::uint32_t> bulkInsertUnique(InputIterator begin, InputIterator end,
			CallbackT *callback_ptr = nullptr)
		{
			if (begin == end) {
				return std::make_pair(0,0);
			}

			bindex::type type = getIndexType();
			// query sorted vector to check on how many unique elements there're in source buffer (not more than N_SV_LIMI)
			std::size_t unique_count = m_interface.countUnique(begin, end, m_sv_limit + 1);
			// noting to add, just leave
			if (unique_count == 0) {
				return std::make_pair(std::distance(begin, end), 0);
			}

			bindex::type type_after_insertion = getIndexTypeAfterInsertion(unique_count);
			/// only morph when upgrade is necessary, do not degrade type when inserting
			if (type_after_insertion > type) {
				// just morph to variable length collection
				if (type_after_insertion == bindex::bindex) {
					morphTo<bindex::bindex>();
				} else if (type_after_insertion == bindex::sorted_vector) {
					morphTo<bindex::sorted_vector>();
				} else {
					// morphing to fixed size collection will require passing elements
					std::size_t diff = 0;
					if (type_after_insertion == bindex::itty) {
						// morph to itty_index passing this 1 element
						assert(type==bindex::empty);
						diff = morphToAndInsert<bindex::itty>(begin, end, callback_ptr);
					} else if (type_after_insertion == bindex::array_2) {
						// morph and insert unique
						diff = morphToAndInsert<bindex::array_2>(begin, end, callback_ptr);
					} else if (type_after_insertion==bindex::array_3) {
						diff = morphToAndInsert<bindex::array_3>(begin, end, callback_ptr);
					} else if (type_after_insertion==bindex::array_4) {
						diff = morphToAndInsert<bindex::array_4>(begin, end, callback_ptr);
					} else {
						assert(false);
					}
					return std::make_pair(std::distance(begin, end), diff);
				}
			}
			return m_interface.bulkInsertUnique(begin, end, callback_ptr);
		}

		bool isNull() const {
			return m_morph.isNull();
		}

		bool empty() const {
			return m_interface.empty();
		}

		/**
         * @return collection size, SLOW!!! (v_bindex must calculate)
         */
		std::uint64_t size() const {
			return m_interface.size();
		}

		/**
         * @return estimated (not necessarily exact) collection size, fast
         */
		std::uint64_t getEstimatedSize() const {
			return m_interface.getEstimatedSize();
		}

		/**
         * Get actual (calculated) limit
         */
		std::uint32_t getSortedVectorSizeLimit() const {
			return m_sv_limit;
		}

		/**
         * Calculate size (BN storage used) in bytes of the entire collection in current morphology
         * @return size [byte]
         */
		std::uint64_t calculateStorageSize() const {
			return m_interface.sizeOf();
		}

		/**
         * This object can be relocated by insert / erase
         */
		AddrT getAddress() const {
			return m_interface.getAddress();
		}

		class joinable_const_iterator
        {
		public:

            joinable_const_iterator() =default;

			/**
             * T collection iterator type
             */
			template <typename T> joinable_const_iterator(const T &, std::shared_ptr<void> ref)
				: m_iterator(static_cast<T*>(nullptr), ref)
			{
			}

			/**
             * Copy constructor (need to create deep copy)
             */
			joinable_const_iterator(const joinable_const_iterator &it)
				: m_iterator(it.m_iterator.clone())
			{
			}

			joinable_const_iterator(const iterator_t &iterator)
				: m_iterator(iterator.clone())
			{
			}

            joinable_const_iterator(joinable_const_iterator &&other)
                : m_iterator(std::move(other.m_iterator))
            {
            }

            joinable_const_iterator& operator=(const joinable_const_iterator &other) 
            {
                this->m_iterator = other.m_iterator.clone();
                return *this;
            }

            joinable_const_iterator& operator=(joinable_const_iterator &&other) 
            {
                this->m_iterator = std::move(other.m_iterator);
                return *this;
            }

			void operator++() {
				++m_iterator;
			}

			void operator--() {
				--m_iterator;
			}

			const item_t &operator*() const {
				return *m_iterator;
			}

			bool is_end() const {
				return m_iterator.is_end();
			}

			bool join(const item_t &key, int direction = - 1) {
				return m_iterator.join(key, direction > 0);
        	}
            
			void joinBound(const item_t &key) {
				m_iterator.joinBound(key);
			}

			bool limitBy(const item_t &key) {
				return m_iterator.limitBy(key);
			}

			const item_t &getLimit() const {
				return m_iterator.getLimit();
			}

			bool hasLimit() const {
				return m_iterator.hasLimit();
			}

			std::pair<item_t, bool> peek(const item_t &key) const {
				return m_iterator.peek(key);
			}

			void stop() {
			    m_iterator.stop();
			}

			/**
			 * Render this instance invalid, release all underlying DBZero resources
			 */
			void reset() {
			    m_iterator.reset();
			}

        private:
            // morphology specific iterator interface
            iterator_t m_iterator;
        };

		joinable_const_iterator beginJoin(int direction) const
        {
			bindex::type index_type = getIndexType();
			switch (index_type) {
				case bindex::empty : {
					return joinable_const_iterator(
					    nullIteratorRef<bindex::empty>(), m_interface.beginJoin(direction));
				}
					break;
				case bindex::itty : {
					return joinable_const_iterator(
					    nullIteratorRef<bindex::itty>(), m_interface.beginJoin(direction));
				}
					break;
				case bindex::array_2 : {
					return joinable_const_iterator(
					    nullIteratorRef<bindex::array_2>(), m_interface.beginJoin(direction));
				}
					break;
				case bindex::array_3 : {
					return joinable_const_iterator(
					    nullIteratorRef<bindex::array_3>(), m_interface.beginJoin(direction));
				}
					break;
				case bindex::array_4 : {
					return joinable_const_iterator(
					    nullIteratorRef<bindex::array_4>(), m_interface.beginJoin(direction));
				}
					break;
				case bindex::sorted_vector : {
					return joinable_const_iterator(
					    nullIteratorRef<bindex::sorted_vector>(), m_interface.beginJoin(direction));
				}
					break;
				case bindex::bindex : {
					return joinable_const_iterator(
					    nullIteratorRef<bindex::bindex>(), m_interface.beginJoin(direction));
				}
					break;
				default :
					break;
			}
			THROWF(db0::InternalException) << "invalid index type" << THROWF_END;
		}

		/**
         * Member provided for v_object interface compatibility
         */
		mptr myPtr(std::uint64_t address, FlagSet<AccessOptions> access_mode = {}) const {
			return m_memspace_ptr->myPtr(address, access_mode);
		}

		/**
		 * Destroy existing instance
		 */
		void destroy() {
			m_interface.destroy(*m_memspace_ptr);
		}

	private :
        /**
         * where all index instances are placed
         */
        db0::Memspace *m_memspace_ptr = nullptr;

        /**
         * size limit for sorted vector based storage
         */
        unsigned int m_sv_limit = 0;
        morph_t m_morph;
        interface_t m_interface;

        /**
         * Create as specific morphology
         */
        template <bindex::type I = bindex::empty, typename... T> void createNew(T&&... args) 
        {
            if (!isNull()) {
                THROWF(db0::InternalException) 
					<< "createNew can only be called on null instance of " << typeid(*this).name();
            }
            m_morph.template create<I>(*m_memspace_ptr, std::forward<T>(args)...);
            m_interface = m_morph.getInterface();
        }

        /**
         * Open as specific morphology
         */
        template <bindex::type I> void openExisting(AddrT addr) 
		{
            if (!isNull()) {
                THROWF(db0::InternalException) 
					<< "openExisting can only be called on null instance of " << typeid(*this).name();
            }
            m_morph.template create<I>(std::make_pair(m_memspace_ptr, addr));
            m_interface = m_morph.getInterface();
        }
		
        /**
         * Non-template version of openExisting         
         */
        void openExisting(AddrT addr, bindex::type index_type) 
        {
            switch (index_type) {
                case bindex::empty : {
                    openExisting<bindex::empty>(addr);
                }
                    break;
                case bindex::itty : {
                    openExisting<bindex::itty>(addr);
                }
                    break;
                case bindex::array_2 : {
                    openExisting<bindex::array_2>(addr);
                }
                    break;
                case bindex::array_3 : {
                    openExisting<bindex::array_3>(addr);
                }
                    break;
                case bindex::array_4 : {
                    openExisting<bindex::array_4>(addr);
                }
                    break;
                case bindex::sorted_vector : {
                    openExisting<bindex::sorted_vector>(addr);
                }
                    break;
                case bindex::bindex : {
                    openExisting<bindex::bindex>(addr);
                }
                    break;
                default : {
                    THROWF(db0::InternalException) << "unknown morphing bindex type";
                }
                    break;
            }
        }

		/**
         * Helper member to provide null reference for type enum
         */
		template <int I> static typename std::tuple_element<I, iterator_types>::type &nullIteratorRef() {
			return *reinterpret_cast<typename std::tuple_element<I, iterator_types>::type*>(0);
		}

		/**
         * Calculate optimal size for storing "n_elements" elements
         * @param n_elements number of elements
         * @return recommended sorted vector size
         */
		std::uint32_t suggestMaxSize(size_t n_elements) const 
        {
			std::uint32_t size = 6;
			while (size < n_elements) {
				size <<= 1; // 12, 24,
			}
			return size;
		}

		/**
         * @return true is value does NOT exist in collection between begin / end (less than 4 elements_
         */
		bool isUnique(const item_t *begin, const item_t *end, item_t value) 
        {
			while (begin!=end) {
				if (*begin==value) {
					return false;
				}
				++begin;
			}
			return true;
		}
		
        template <bindex::type T> void morphToAndInsert(const item_t &item)
        {
            bindex::type index_type = getIndexType();
            // make sure this is NOT performed on sorted_vector or bindex
            if (index_type==bindex::sorted_vector || index_type==bindex::bindex) {
                assert(false);
                THROWF(db0::InternalException) << "Operation not allowed";
            }
            item_t data[4];
            // pull data from collection
            item_t *placeholder = data + m_interface.copyAll(data);
            *placeholder = item;
            ++placeholder;
            morphToAndSet<T>(data, placeholder);
        }

		/**
         * Morph to T type fixed size collection (copy data from current morphology)
         * passing additional data to fixed size collection
		 * @param callback_ptr optional function to be called for each unique inserted item
         * @return number of unique items inserted
         */
		template<bindex::type T, typename InputIterator>
		std::size_t morphToAndInsert(InputIterator begin, InputIterator end, CallbackT *callback_ptr = nullptr)
        {			
			// make sure this is NOT performed on sorted_vector or bindex
			assert(getIndexType() != bindex::sorted_vector && getIndexType() != bindex::bindex);

			item_t data[4];
			item_t *placeholder = data;
			// pull data from collection
			placeholder += m_interface.copyAll(placeholder);
			item_t *in_end = placeholder;
			// and also add unique items from request
			std::uint32_t unique_count = 0;
			while (begin != end) {
				// store only unique items
				if (isUnique(data, in_end, *begin)) {
					// must not exceed 4 unique elements
					assert(placeholder < data + (sizeof(data) / sizeof(item_t)));
					*placeholder = *begin;
					++placeholder;
					++unique_count;
					if (callback_ptr) {
						(*callback_ptr)(*begin);
					}
				}
				++begin;
			}
			morphToAndSet<T>(data, placeholder);
			return unique_count;
		}

        template <bindex::type T> void morphToAndSet(const item_t *begin, const item_t *end) 
        {
            // destroy existing v-space instance
            m_interface.destroy(*m_memspace_ptr);
            // .. and forward all items retained to new collection type (morph)
            switch (getIndexType()) {
                case bindex::empty : {
                    m_interface = m_morph.template morphTo<T, bindex::empty>(*m_memspace_ptr, begin, end);
                }
                    break;
                case bindex::itty : {
                    m_interface = m_morph.template morphTo<T, bindex::itty>(*m_memspace_ptr, begin, end);
                }
                    break;
                case bindex::array_2 : {
                    m_interface = m_morph.template morphTo<T, bindex::array_2>(*m_memspace_ptr, begin, end);
                }
                    break;
                case bindex::array_3 : {
                    m_interface = m_morph.template morphTo<T, bindex::array_3>(*m_memspace_ptr, begin, end);
                }
                    break;
                case bindex::array_4 : {
                    m_interface = m_morph.template morphTo<T, bindex::array_4>(*m_memspace_ptr, begin, end);
                }
                    break;
                default :
                    break;
            }
        }

        template <bindex::type T> void morphToAndErase(const item_t &item)
        {
            bindex::type index_type = getIndexType();
            // make sure this is NOT performed on sorted_vector or bindex
            if (index_type==bindex::sorted_vector || index_type==bindex::bindex) {
                assert(false);
                THROWF(db0::InternalException) << "Operation not allowed";
            }
            item_t data[4];
            item_t final_data[4];
            item_t *in_end = data;
            // pull data from collection
            in_end += m_interface.copyAll(data);
            item_t *out_end = final_data;
            item_t *in = data;
            // retain only items not destined to erase
            while (in!=in_end) {
                if (*in != item) {
                    *out_end = *in;
                    ++out_end;
                }
                ++in;
            }
            morphToAndSet<T>(final_data, out_end);
        }

		/**
         * Morph to T type fixed size collection (remove items from current morphology)
         */
		template<bindex::type T, typename InputIterator>
		void morphToAndErase(InputIterator begin, InputIterator end, CallbackT *callback_ptr = nullptr)
		{
			std::unordered_set<item_t> erase_items;
			while (begin != end) {
				erase_items.insert(*begin);
				++begin;
			}

			// make sure this is NOT performed on sorted_vector or bindex
			assert(getIndexType() != bindex::sorted_vector && getIndexType() != bindex::bindex);

			item_t data[4];
			item_t final_data[4];
			item_t *in_end = data;
			// pull data from collection
			in_end += m_interface.copyAll(data);
			item_t *out_end = final_data;
			item_t *in = data;
			// retain only items not destined to erase
			while (in != in_end) {
				if (erase_items.find(*in) == erase_items.end()) {
					*out_end = *in;
					++out_end;
				} else {
					// notify on erased item
					if (callback_ptr) {
						(*callback_ptr)(*in);
					}
				}
				++in;
			}
            morphToAndSet<T>(final_data, out_end);
        }
		
		/**
         * Dedicated implementation for morphing to empty index (erase all)
         */
		void morphToEmpty()
        {
			Memspace &memspace = *m_memspace_ptr;
			// destroy existing v-space instance
			m_interface.destroy(memspace);
			bindex::type index_type = getIndexType();
			// morph to empty_index (all data will be erased)
			if (index_type==bindex::bindex) {
				m_interface = m_morph.template morphTo<bindex::empty, bindex::bindex>(memspace);
			} else if (index_type==bindex::sorted_vector) {
				m_interface = m_morph.template morphTo<bindex::empty, bindex::sorted_vector>(memspace);
			} else if (index_type==bindex::itty) {
				m_interface = m_morph.template morphTo<bindex::empty, bindex::itty>(memspace);
			} else if (index_type==bindex::array_2) {
				m_interface = m_morph.template morphTo<bindex::empty, bindex::array_2>(memspace);
			} else if (index_type==bindex::array_3) {
				m_interface = m_morph.template morphTo<bindex::empty, bindex::array_3>(memspace);
			} else if (index_type==bindex::array_4) {
				m_interface = m_morph.template morphTo<bindex::empty, bindex::array_4>(memspace);
			}
		}

		/**
         * Morph to T type collection (copy data from current morphology)
         * this operation is only supported for morphing to either: sorted vector or bindex
         */
		template <bindex::type T> void morphTo()
        {
			Memspace &memspace = *m_memspace_ptr;
			bindex::type index_type = getIndexType();
			if (index_type==bindex::bindex) {
				THROWF(db0::InternalException) << "Unable to morph from v_bindex (downgrade not supported)";
			} else if (index_type==bindex::sorted_vector) {
				// copy from sorted vector (create bindex by copy)
				auto old_interface = m_interface;
				auto old_instance = m_morph.getInstance();
				m_interface = m_morph.template morphToWithCopy<T, bindex::sorted_vector>();
				// destroy v-space instance
				old_interface.destroy(memspace);
			} else if (index_type==bindex::empty) {
				// create empty v_bindex
				m_interface = m_morph.template morphTo<T, bindex::empty>(memspace);
			} else {
				item_t data[4];
				// pull all data from existing collection (max. 4 items)
				std::size_t size = m_interface.copyAll(data);
				// destroy existing v-space instance as new will be created right next
				m_interface.destroy(memspace);
				if (index_type==bindex::itty) {
					m_interface = m_morph.template morphTo<T, bindex::itty>(memspace, data, data + size);
				} else if (index_type==bindex::array_2) {
					m_interface = m_morph.template morphTo<T, bindex::array_2>(memspace, data, data + size);
				} else if (index_type==bindex::array_3) {
					m_interface = m_morph.template morphTo<T, bindex::array_3>(memspace, data, data + size);
				} else if (index_type==bindex::array_4) {
					m_interface = m_morph.template morphTo<T, bindex::array_4>(memspace, data, data + size);
				} else {
					assert(false);
				}
			}
		}
	};

} 
