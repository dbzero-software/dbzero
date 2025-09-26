#include "FT_TagProduct.hpp"
#include "FT_ANDIterator.hpp"

namespace db0

{

    template <typename key_t>
    FT_TagProduct<key_t>::FT_TagProduct(const FT_IteratorT &objects, const FT_IteratorT &tags, tag_factory_func tag_func, 
            int direction)
        : m_direction(direction)
        , m_tag_func(tag_func)
        , m_tags(tags.beginTyped(direction))
        , m_objects(objects.beginTyped(direction))        
    {
        initNextTag(m_current_key);
    }
    
    template <typename key_t>
    FT_TagProduct<key_t>::FT_TagProduct(std::unique_ptr<FT_IteratorT> &&objects, std::unique_ptr<FT_IteratorT> &&tags,
            tag_factory_func tag_func, int direction)
        : m_direction(direction)
        , m_tag_func(tag_func)
        , m_tags(std::move(tags))
        , m_objects(std::move(objects))        
    {
        initNextTag(m_current_key);
    }
    
    template <typename key_t>
    void FT_TagProduct<key_t>::initNextTag(TP_Vector<key_t> &key_ref)
    {
        while (!m_tags->isEnd()) {
            m_tags->getKey(key_ref[1]);
            auto tag_index = m_tag_func(key_ref[1], m_direction);
            if (tag_index) {
                m_current = std::make_unique<FT_JoinANDIterator<key_t> >(std::move(tag_index), 
                    m_objects->beginTyped(m_direction), m_direction
                );
                if (!m_current->isEnd()) {
                    m_current->getKey(key_ref[0]);
                    return;
                }
            }
            m_tags->next();
        }
        m_current = nullptr;        
    }

    template <typename key_t>
    bool FT_TagProduct<key_t>::isEnd() const
    {
        return !m_current;
    }

    template <typename key_t>
    const std::type_info &FT_TagProduct<key_t>::typeId() const
    {
        return typeid(*this);
    }
    
    template <typename key_t>
    void FT_TagProduct<key_t>::next(void *buf) {        
        this->_next(buf);
    }
    
    template <typename key_t>
    void FT_TagProduct<key_t>::_next(void *buf)
    {
        assert(!isEnd());
        if (buf) {
            *(KeyT*)buf = m_current_key;
        }
        m_current->next();
        if (m_current->isEnd()) {
            m_tags->next();
            initNextTag(m_current_key);
        } else {
            m_current->getKey(m_current_key[0]);
        }
    }

    template <typename key_t>
    bool FT_TagProduct<key_t>::_next(TP_Vector<key_t> &key_ref)
    {
        assert(!isEnd());
        return true;
    }

    template <typename key_t>
    void FT_TagProduct<key_t>::operator++()
    {
        assert(m_direction > 0);
        this->_next(nullptr);
    }

    template <typename key_t>
    void FT_TagProduct<key_t>::operator--() 
    {
        assert(m_direction < 0);
        this->_next(nullptr);        
    }

    template <typename key_t>
	const key_t *FT_TagProduct<key_t>::getKey() const {
        return m_current_key;
    }
    
    template <typename key_t>
    void FT_TagProduct<key_t>::getKey(KeyStorageT &key) const {
        key = m_current_key;
    }
    
    template <typename key_t>
    bool FT_TagProduct<key_t>::join(KeyT key, int direction) 
    {
        assert(!isEnd());
        bool overflow = false;
        for (;;) {
            if (!m_tags->join(key[1], direction)) {
                // no such tag, end iteration
                m_current = nullptr;
                return false;
            }
            if (m_tags->swapKey(m_current_key[1])) {
                initNextTag(m_current_key);
                overflow = true;
            }
            if (!m_current) {
                return false;
            }
            if (overflow || m_current->join(key[0], direction)) {
                m_current->getKey(m_current_key[0]);
                return true;
            } else {
                // continue with the next tag                
                m_tags->next();
                if (m_tags->isEnd()) {
                    m_current = nullptr;
                    return false;
                }
                overflow = true;
            }
        }
    }
    
    template <typename key_t>
	void FT_TagProduct<key_t>::joinBound(KeyT) {
        throw std::runtime_error("joinBound() not implemented");
    }

    template <typename key_t>
	std::pair<const key_t*, bool> FT_TagProduct<key_t>::peek(KeyT) const {
        throw std::runtime_error("peek() not implemented");
    }    
    
    template <typename key_t>
    bool FT_TagProduct<key_t>::isNextKeyDuplicated() const {
        throw std::runtime_error("isNextKeyDuplicated() not implemented");
    }
    
    template <typename key_t>
	std::unique_ptr<FT_Iterator<const key_t*, TP_Vector<key_t> > > 
    FT_TagProduct<key_t>::beginTyped(int direction) const
    {
        throw std::runtime_error("beginTyped() not implemented");
    }
        
    template <typename key_t>
	bool FT_TagProduct<key_t>::limitBy(KeyT)
    {
        throw std::runtime_error("limitBy() not implemented");
    }
    
    template <typename key_t>
    void FT_TagProduct<key_t>::stop() {
        throw std::runtime_error("stop() not implemented");
    }

    template <typename key_t>
    FTIteratorType FT_TagProduct<key_t>::getSerialTypeId() const {
        throw std::runtime_error("getSerialTypeId() not implemented");
    }
    
    template <typename key_t>
    double FT_TagProduct<key_t>::compareToImpl(const FT_IteratorBase &it) const {
        throw std::runtime_error("compareToImpl() not implemented");    
    }
    
    template <typename key_t>
    void FT_TagProduct<key_t>::getSignature(std::vector<std::byte> &) const {
        throw std::runtime_error("getSignature() not implemented");    
    }

    template <typename key_t>
    void FT_TagProduct<key_t>::serializeFTIterator(std::vector<std::byte> &) const {
        throw std::runtime_error("not implemented");
    }
    
    template <typename key_t>
    std::ostream &FT_TagProduct<key_t>::dump(std::ostream & /*os*/) const {
        throw std::runtime_error("Not implemented");
    }

    template class FT_TagProduct<std::uint64_t>;
    template class FT_TagProduct<db0::UniqueAddress>;

}   
