#include "Index.hpp"
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/object_model/tags/TypedObjectIterator.hpp>
#include <dbzero/core/collections/full_text/SortedIterator.hpp>
#include <dbzero/core/utils/uuid.hpp>

namespace db0::object_model

{

    GC0_Define(Index)
    using TypeId = db0::bindings::TypeId;

    o_index::o_index(std::uint32_t instance_id, IndexType type, IndexDataType data_type)
        : m_instance_id(instance_id)
        , m_type(type)
        , m_data_type(data_type)
    {
    }
    
    o_index::o_index(const o_index &other)
        : m_instance_id(other.m_instance_id)
        , m_type(other.m_type)
        , m_data_type(other.m_data_type)        
    {
    }
    
    IndexDataType getIndexDataType(TypeId type_id)
    {
        switch (type_id) {
            case TypeId::INTEGER:
                return IndexDataType::Int64;
            case TypeId::DATETIME:
                return IndexDataType::UInt64;
            default:
                THROWF(db0::InputException) << "Unsupported index key type: " 
                    << static_cast<std::uint16_t>(type_id) << THROWF_END;
        }
    }

    Index::Index(db0::swine_ptr<Fixture> &fixture)
        : super_t(fixture, db0::createInstanceId(), IndexType::RangeTree, IndexDataType::Auto)
        , m_data_type((*this)->m_data_type)
    {
    }

    Index::Index(db0::swine_ptr<Fixture> &fixture, std::uint64_t address)
        : super_t(super_t::tag_from_address(), fixture, address)
        , m_data_type((*this)->m_data_type)
    {
    }

    Index::Index(db0::swine_ptr<Fixture> &fixture, const Index &other)
        : super_t(tag_as_temp(), fixture, *other.getData())
        , m_data_type(other.m_data_type)
    {
        switch (m_data_type) {
            case IndexDataType::Int64: {
                makeRangeTree(other.getRangeTree<std::int64_t>());
                break;
            }

            case IndexDataType::UInt64: {
                makeRangeTree(other.getRangeTree<std::uint64_t>());
                break;
            }

            // flush using default / provisional data type
            case IndexDataType::Auto: {
                makeRangeTree(other.getRangeTree<DefaultT>());
                break;
            }

            default:
                THROWF(db0::InputException) << "Unsupported index data type: " << static_cast<std::uint16_t>(m_data_type);        
        }
    }

    Index *Index::makeNew(void *at_ptr, db0::swine_ptr<Fixture> &fixture) {
        return new (at_ptr) Index(fixture);
    }

    Index *Index::unload(void *at_ptr, db0::swine_ptr<Fixture> &fixture, std::uint64_t address) {
        return new (at_ptr) Index(fixture, address);
    }
    
    void Index::flush()
    {
        // no instance due to move
        if (!hasInstance()) {
            return;
        }
        // apply modified data type
        if ((*this)->m_data_type != m_data_type) {
            modify().m_data_type = m_data_type;
        }

        if (m_index_builder) {
            switch (m_data_type) {
                case IndexDataType::Int64: {
                    getIndexBuilder<std::int64_t>().flush(getRangeTree<std::int64_t>());                    
                    break;
                }

                case IndexDataType::UInt64: {
                    getIndexBuilder<std::uint64_t>().flush(getRangeTree<std::uint64_t>());
                    break;
                }

                // flush using default / provisional data type
                case IndexDataType::Auto: {
                    getIndexBuilder<DefaultT>(true).flush(getRangeTree<DefaultT>());
                    break;
                }

                default:
                    THROWF(db0::InputException) << "Unsupported index data type: " << static_cast<std::uint16_t>(m_data_type);
            
            }
        }            
    }

    std::size_t Index::size() const
    {
        const_cast<Index*>(this)->flush();
        if (!(*this)->m_index_addr) {
            return 0;
        }

        switch (m_data_type) {
            case IndexDataType::Int64: {
                return getRangeTree<std::int64_t>().size();
                break;
            }

            case IndexDataType::UInt64: {
                return getRangeTree<std::uint64_t>().size();
                break;
            }

            default:
                THROWF(db0::InputException) << "Unsupported index data type: " 
                    << static_cast<std::uint16_t>(m_data_type) << THROWF_END;
        }
    }
    
    void Index::add(ObjectPtr key, ObjectPtr value)
    {
        assert(hasInstance());
        auto &type_manager = LangToolkit::getTypeManager();
        // special handling of null / None values
        if (type_manager.isNull(key)) {
            addNull(value);
            return;
        }

        auto data_type = m_data_type;
        if (data_type == IndexDataType::Auto) {
            // update to a concrete data type
            data_type = updateIndexBuilder(type_manager.getTypeId(key));
        }

        switch (data_type) {
            case IndexDataType::Int64: {
                getIndexBuilder<std::int64_t>().add(type_manager.extractInt64(key), value); 
                break;
            }

            case IndexDataType::UInt64: {
                getIndexBuilder<std::uint64_t>().add(type_manager.extractUInt64(key), value);
                break;
            }

            default:
                THROWF(db0::InputException) << "Index of type " 
                    << static_cast<std::uint16_t>(m_data_type)
                    << " does not allow adding key type: " << LangToolkit::getTypeName(key) << THROWF_END;
        }            
    }
    
    void Index::remove(ObjectPtr key, ObjectPtr value)
    {
        assert(hasInstance());
        auto &type_manager = LangToolkit::getTypeManager();
        // special handling of null / None values
        if (type_manager.isNull(key)) {
            removeNull(value);
            return;
        }

        auto data_type = m_data_type;
        if (data_type == IndexDataType::Auto) {
            // update to a concrete data type
            data_type = updateIndexBuilder(type_manager.getTypeId(key));
        }

        switch (data_type) {
            case IndexDataType::Int64: {
                getIndexBuilder<std::int64_t>().remove(type_manager.extractInt64(key), value); 
                break;
            }

            case IndexDataType::UInt64: {
                getIndexBuilder<std::uint64_t>().remove(type_manager.extractUInt64(key), value);
                break;
            }

            default:
                THROWF(db0::InputException) << "Index of type " 
                    << static_cast<std::uint16_t>(m_data_type)
                    << " does not allow keys of type: " << LangToolkit::getTypeName(key) << THROWF_END;
        }            
    }

    std::unique_ptr<Index::IteratorFactory> Index::range(ObjectPtr min, ObjectPtr max, bool null_first) const
    {
        assert(hasInstance());
        const_cast<Index*>(this)->flush();        
        switch (m_data_type) {
            case IndexDataType::Int64: {
                return rangeQuery<std::int64_t>(min, true, max, true, null_first);
            }
            break;            

            case IndexDataType::UInt64: {
                return rangeQuery<std::uint64_t>(min, true, max, true, null_first);
            }
            break;

            default:
                THROWF(db0::InputException) << "Unsupported index data type: " 
                    << static_cast<std::uint16_t>(m_data_type) << THROWF_END;
        }        
    }
    
    std::unique_ptr<db0::SortedIterator<std::uint64_t> >
    Index::sort(const ObjectIterator &iter, bool asc, bool null_first) const
    {
        assert(hasInstance());
        const_cast<Index*>(this)->flush();
        std::unique_ptr<db0::SortedIterator<std::uint64_t> > sort_iter;
        if (iter.isSorted()) {
            // sort by additional criteria
            switch (m_data_type) {
                case IndexDataType::Int64: {
                    return sortSortedQuery<std::int64_t>(iter.beginSorted(), asc, null_first);
                }
                break;

                case IndexDataType::UInt64: {
                    return sortSortedQuery<std::uint64_t>(iter.beginSorted(), asc, null_first);
                }
                break;

                default:
                    THROWF(db0::InputException) << "Unsupported index data type: " 
                        << static_cast<std::uint16_t>(m_data_type) << THROWF_END;
            }
        } else {
            // sort a full-text query
            // FIXME: incorporate observers in the sorted iterator
            std::vector<std::unique_ptr<QueryObserver> > observers;
            switch (m_data_type) {
                case IndexDataType::Int64: {
                    return sortQuery<std::int64_t>(iter.beginFTQuery(observers), asc, null_first);
                }
                break;

                case IndexDataType::UInt64: {
                    return sortQuery<std::uint64_t>(iter.beginFTQuery(observers), asc, null_first);
                }
                break;

                default:
                    THROWF(db0::InputException) << "Unsupported index data type: " 
                        << static_cast<std::uint16_t>(m_data_type) << THROWF_END;
            }
        }
    }
    
    void Index::addNull(ObjectPtr obj_ptr)
    {
        assert(hasInstance());
        switch (m_data_type) {
            // use provisional data type for Auto
            case IndexDataType::Auto: {
                return getIndexBuilder<DefaultT>(true).addNull(obj_ptr);
                break;
            }

            case IndexDataType::Int64: {
                return getIndexBuilder<std::int64_t>().addNull(obj_ptr);
                break;
            }

            case IndexDataType::UInt64: {
                return getIndexBuilder<std::uint64_t>().addNull(obj_ptr);
                break;
            }

            default:
                THROWF(db0::InputException) << "Unsupported index data type: " 
                    << static_cast<std::uint16_t>(m_data_type) << THROWF_END;
        }
    }
    
    // extract optional value
    template <> std::optional<std::int64_t> Index::extractOptionalValue<std::int64_t>(ObjectPtr value) const
    {
        auto &type_manager = LangToolkit::getTypeManager();
        if (type_manager.isNull(value)) {
            return std::nullopt;    
        }
        return type_manager.extractInt64(value);
    }

    template <> std::optional<std::uint64_t> Index::extractOptionalValue<std::uint64_t>(ObjectPtr value) const
    {
        auto &type_manager = LangToolkit::getTypeManager();
        if (type_manager.isNull(value)) {
            return std::nullopt;    
        }
        return type_manager.extractUInt64(type_manager.getTypeId(value), value);
    }

    void Index::preCommit() {
        flush();
    }

    void Index::preCommitOp(void *ptr) {
        static_cast<Index*>(ptr)->preCommit();
    }

    IndexDataType Index::updateIndexBuilder(TypeId type_id)
    {
        auto index_data_type = getIndexDataType(type_id);
        if (!m_index_builder) {
            return index_data_type;
        }
        
        if (m_data_type == IndexDataType::Auto) {
            // convert from the provisional to a concrete data type
            switch (index_data_type) {
                case IndexDataType::Int64: {
                    updateIndexBuilder<DefaultT, std::int64_t>();
                    break;
                }    

                case IndexDataType::UInt64: {
                    updateIndexBuilder<DefaultT, std::uint64_t>();
                    break;
                }

                default:
                    THROWF(db0::InputException) << "Unsupported index key type: " << static_cast<std::uint16_t>(type_id) << THROWF_END;
            }
            return m_data_type;
        }

        // validate if concrete type is matching
        if (m_data_type != index_data_type) {
            THROWF(db0::InputException) << "Index key type mismatch: " 
                << static_cast<std::uint16_t>(type_id) << " != " 
                << static_cast<std::uint16_t>(m_data_type) << THROWF_END;
        }
        return index_data_type;
    }

    void Index::removeNull(ObjectPtr obj_ptr)
    {
        switch (m_data_type) {
            // use provisional data type for Auto
            case IndexDataType::Auto: {
                return getIndexBuilder<DefaultT>(true).removeNull(obj_ptr);
                break;
            }

            case IndexDataType::Int64: {
                return getIndexBuilder<std::int64_t>().removeNull(obj_ptr);
                break;
            }

            case IndexDataType::UInt64: {
                return getIndexBuilder<std::uint64_t>().removeNull(obj_ptr);
                break;
            }

            default:
                THROWF(db0::InputException) << "Unsupported index data type: " 
                    << static_cast<std::uint16_t>(m_data_type) << THROWF_END;
        }
    }

    void Index::moveTo(db0::swine_ptr<Fixture> &fixture)
    {
        assert(hasInstance());
        this->flush();
        Index new_index(fixture, *this);
        // remove instance from lang cache
        auto &lang_cache = this->getFixture()->getLangCache();
        lang_cache.erase(getAddress());
        this->destroy();
        *this = std::move(new_index);
    }
    
    void Index::operator=(Index &&other)
    {
        other.flush();
        super_t::operator=(std::move(other));
        m_data_type = other.m_data_type;
        m_index = other.m_index;
        other.m_index_builder = nullptr;
        other.m_index = nullptr;
        assert(!other.hasInstance());
    }

}