#pragma once

#include "TP_Vector.hpp"
#include "FT_Iterator.hpp"

namespace db0

{
    
    // The tag-product operator combines a result iterator (yielding objects)
    // with the tag iterator (yielding tags). It generates a composite key consiting of object + tag
    // with tag assumed as the unique identifier
    template <typename key_t = std::uint64_t>
    class FT_TagProduct final: public FT_Iterator<const key_t*, TP_Vector<key_t> >
    {
    public:
        using KeyT = const key_t*;
        using KeyStorageT = TP_Vector<key_t>;
        using FT_IteratorT = FT_Iterator<key_t>;
        // the tag-inverted index factory
        // @return nullptr if the tag does not have an associated index
        using tag_factory_func = std::function<std::unique_ptr<FT_IteratorT>(key_t, int direction)>;

        FT_TagProduct(const FT_IteratorT &objects, const FT_IteratorT &tags, tag_factory_func tag_func, 
            int direction = -1);
        FT_TagProduct(std::unique_ptr<FT_IteratorT> &&objects, std::unique_ptr<FT_IteratorT> &&tags,
            tag_factory_func tag_func, int direction = -1);
        
		bool isEnd() const override;
        
        const std::type_info &typeId() const override;

        void next(void *buf = nullptr) override;

		void operator++() override;

		void operator--() override;

        // NOTE: return value lifetime rules apply
		KeyT getKey() const override;
        
        void getKey(KeyStorageT &) const override;
        
		bool join(KeyT, int direction = -1) override;
        
		void joinBound(KeyT) override;

        // NOTE: return value lifetime rules apply
		std::pair<KeyT, bool> peek(KeyT) const override;
        
        bool isNextKeyDuplicated() const override;
        
		std::unique_ptr<FT_Iterator<KeyT, KeyStorageT> > beginTyped(int direction = -1) const override;
        
		bool limitBy(KeyT) override;
        
		std::ostream &dump(std::ostream &os) const override;
		
        void stop() override;
        
        FTIteratorType getSerialTypeId() const override;
        
        double compareToImpl(const FT_IteratorBase &it) const override;
        
        void getSignature(std::vector<std::byte> &) const override;
        
    protected:
        const int m_direction;
        tag_factory_func m_tag_func;
        std::unique_ptr<FT_IteratorT> m_tags;
        std::unique_ptr<FT_IteratorT> m_objects;
        // Curent pair (tag + objects)
        std::unique_ptr<FT_IteratorT> m_current;
        TP_Vector<key_t> m_current_key;
        key_t m_next_tag;
                
        void initNextTag();
        void _next(void *buf = nullptr);

        void serializeFTIterator(std::vector<std::byte> &) const override;        
    };

    extern template class FT_TagProduct<UniqueAddress>;
    extern template class FT_TagProduct<std::uint64_t>;

}