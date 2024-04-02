#include "Index.hpp"
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/object_model/tags/TypedObjectIterator.hpp>
#include <dbzero/core/collections/full_text/SortedIterator.hpp>
#include <dbzero/core/utils/uuid.hpp>

namespace db0::object_model

{

    GC0_Define(Index)

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

    Index *Index::makeNew(void *at_ptr, db0::swine_ptr<Fixture> &fixture) {
        return new (at_ptr) Index(fixture);
    }

    Index *Index::unload(void *at_ptr, db0::swine_ptr<Fixture> &fixture, std::uint64_t address) {
        return new (at_ptr) Index(fixture, address);
    }

    void Index::flush()
    {        
        // the purpose of callback is to incRef to objects when a new tag is assigned
        auto &type_manager = LangToolkit::getTypeManager();
        std::function<void(std::uint64_t)> add_callback = [&](std::uint64_t address) {
            auto it = m_object_cache.find(address);
            assert(it != m_object_cache.end());
            type_manager.extractObject(it->second.get()).incRef();
        };
        
        // apply modified data type
        if ((*this)->m_data_type != m_data_type) {
            modify().m_data_type = m_data_type;
        }

        if (m_index_builder) {
            switch (m_data_type) {
                case IndexDataType::Int64: {
                    auto &builder = getRangeTreeBuilder<std::int64_t>();
                    builder.flush(getRangeTree<std::int64_t>(), &add_callback);
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

            default:
                THROWF(db0::InputException) << "Unsupported index data type: " 
                    << static_cast<std::uint16_t>(m_data_type) << THROWF_END;
        }
    }
    
    void Index::add(ObjectPtr key, ObjectPtr value)
    {
        auto &type_manager = LangToolkit::getTypeManager();
        auto object_addr = type_manager.extractObject(value).getAddress();
        auto key_type_id = type_manager.getTypeId(key);
        switch (key_type_id) {
            case db0::bindings::TypeId::INTEGER: {
                auto key_value = type_manager.extractInt64(key);
                getRangeTreeBuilder<std::int64_t>().add(key_value, object_addr);
                break;
            }
            
            // special handling of None values
            case db0::bindings::TypeId::NONE: {
                addNull(object_addr);
                break;
            }

            default:
                THROWF(db0::InputException) << "Unsupported key type: " << key_type_id;
        }

        // cache object locally
        if (m_object_cache.find(object_addr) == m_object_cache.end()) {
            m_object_cache.emplace(object_addr, value);
        }
    }

    void Index::range(ObjectIterator *at_ptr, ObjectPtr min, ObjectPtr max) const
    {
        const_cast<Index*>(this)->flush();
        std::unique_ptr<IteratorFactory> iter_factory;
        switch (m_data_type) {
            case IndexDataType::Int64: {
                iter_factory = rangeQuery<std::int64_t>(min, true, max, true);
                break;
            }

            default:
                THROWF(db0::InputException) << "Unsupported index data type: " 
                    << static_cast<std::uint16_t>(m_data_type) << THROWF_END;
        }
        new (at_ptr) ObjectIterator(this->getFixture(), std::move(iter_factory));
    }

    void Index::sort(const ObjectIterator &iter, ObjectIterator *at_ptr) const
    {
        const_cast<Index*>(this)->flush();
        std::unique_ptr<db0::SortedIterator<std::uint64_t> > sort_iter;
        if (iter.isSorted()) {
            // sort by additional criteria
            switch (m_data_type) {
                case IndexDataType::Int64: {
                    sort_iter = sortSortedQuery<std::int64_t>(iter.beginSorted());
                    break;
                }

                default:
                    THROWF(db0::InputException) << "Unsupported index data type: " 
                        << static_cast<std::uint16_t>(m_data_type) << THROWF_END;
            }
        } else {
            // sort a full-text query
            switch (m_data_type) {
                case IndexDataType::Int64: {
                    sort_iter = sortQuery<std::int64_t>(iter.beginFTQuery());
                    break;
                }

                default:
                    THROWF(db0::InputException) << "Unsupported index data type: " 
                        << static_cast<std::uint16_t>(m_data_type) << THROWF_END;
            }
        }
        // placement new
        new (at_ptr) ObjectIterator(this->getFixture(), std::move(sort_iter));
    }
    
    void Index::addNull(std::uint64_t value)
    {
        switch (m_data_type) {
            case IndexDataType::Int64: {
                return getRangeTreeBuilder<std::int64_t>().addNull(value);
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
        if (!value) {
            return std::nullopt;        
        }
        return LangToolkit::getTypeManager().extractInt64(value);
    }

    void Index::preCommit() {
        flush();
    }

    void Index::preCommitOp(void *ptr) {
        static_cast<Index*>(ptr)->preCommit();
    }

}