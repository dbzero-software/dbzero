#include "FT_SpanIterator.hpp"

namespace db0

{
    
    template <typename KeyT>
    FT_SpanIterator<KeyT>::FT_SpanIterator(std::unique_ptr<FT_Iterator<KeyT> > &&inner_it, unsigned int span_shift,
        int direction)
        : m_inner_it(std::move(inner_it))
        , m_span_shift(span_shift)
        , m_span_size(1u << span_shift)
        , m_direction(direction)        
    {    
    }
    
    template <typename KeyT>
    const std::type_info &FT_SpanIterator<KeyT>::typeId() const {
        return typeid(*this);
    }

    template <typename KeyT> 
    bool FT_SpanIterator<KeyT>::isEnd() const
    {
        return m_inner_it->isEnd();
    }

    template <typename KeyT> 
    void FT_SpanIterator<KeyT>::next(void *buf)
    {
        m_inner_it->next(buf);
    }    

    template <typename KeyT> void FT_SpanIterator<KeyT>::operator++()
    {        
        m_inner_it->operator++();
    }    

    template <typename KeyT> void FT_SpanIterator<KeyT>::operator--()
    {        
        m_inner_it->operator--();
    }

    template <typename KeyT> KeyT FT_SpanIterator<KeyT>::getKey() const
    {
        if (m_key) {
            return *m_key;
        }
        return m_inner_it->getKey();        
    }
    
    template <typename KeyT>
    std::unique_ptr<FT_IteratorBase> FT_SpanIterator<KeyT>::begin() const
    {
        throw std::runtime_error("Not implemented");
    }    

    template <typename KeyT>
    std::unique_ptr<FT_Iterator<KeyT> > FT_SpanIterator<KeyT>::beginTyped(int direction) const
    {
        throw std::runtime_error("Not implemented");
    }
    
    template <typename KeyT>
    bool FT_SpanIterator<KeyT>::join(KeyT join_key, int direction)
    {
        return m_inner_it->join(join_key, direction);
    }

    template <typename KeyT>
    void FT_SpanIterator<KeyT>::joinBound(KeyT join_key)
    {
        throw std::runtime_error("Not implemented");
    }

    template <typename KeyT>
    std::pair<KeyT, bool> FT_SpanIterator<KeyT>::peek(KeyT join_key) const 
    {
        throw std::runtime_error("Not implemented");
    }
    
    template <typename KeyT>
    bool FT_SpanIterator<KeyT>::isNextKeyDuplicated() const
    {
        throw std::runtime_error("Not implemented");
    }
    
    template <typename KeyT>
    bool FT_SpanIterator<KeyT>::limitBy(KeyT key)
    {
        throw std::runtime_error("Not implemented");
    }
    
    template <typename KeyT>
    void FT_SpanIterator<KeyT>::stop()
    {
        throw std::runtime_error("Not implemented");
    }
            
    template <typename KeyT>
    double FT_SpanIterator<KeyT>::compareToImpl(const FT_IteratorBase &it) const
    {
        throw std::runtime_error("Not implemented");
    }

    template <typename KeyT>
    std::ostream& FT_SpanIterator<KeyT>::dump(std::ostream&) const {
        throw std::runtime_error("Not implemented");
    }

    template <typename KeyT>
    void FT_SpanIterator<KeyT>::getSignature(std::vector<std::byte>&) const {
        throw std::runtime_error("Not implemented");
    }

    template <typename KeyT>
    db0::FTIteratorType FT_SpanIterator<KeyT>::getSerialTypeId() const {
        throw std::runtime_error("Not implemented");
    }

    template <typename KeyT>
    void FT_SpanIterator<KeyT>::serializeFTIterator(std::vector<std::byte>&) const {
        throw std::runtime_error("Not implemented");
    }

    template class FT_SpanIterator<std::uint64_t>;
    template class FT_SpanIterator<UniqueAddress>;

}