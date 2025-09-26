#include <gtest/gtest.h>
#include <utils/TestBase.hpp>
#include <dbzero/core/collections/full_text/FT_FixedKeyIterator.hpp>
#include <dbzero/core/collections/full_text/FT_TagProduct.hpp>
#include <dbzero/core/collections/full_text/TP_Vector.hpp>
#include <dbzero/core/collections/full_text/FT_ANDIterator.hpp>

namespace tests

{
    
	using namespace db0;
    using UniqueAddress = db0::UniqueAddress;

    class FT_TagProductTest: public testing::Test
    {
    public:
        using FT_IteratorT = FT_Iterator<std::uint64_t>;
        using FactoryFunc = std::function<std::unique_ptr<FT_IteratorT>(std::uint64_t, int)>;

        FactoryFunc makeFactory(const std::unordered_map<std::uint64_t, std::vector<uint64_t> > &index)
        {
            return [index](std::uint64_t key, int direction) {
                auto it = index.find(key);
                if (it == index.end()) {
                    return std::unique_ptr<FT_IteratorT>(nullptr);
                }
                return std::unique_ptr<FT_IteratorT>(new FT_FixedKeyIterator<std::uint64_t>(
                    it->second.data(), it->second.data() + it->second.size(), direction
                ));
            };
        }

        // NOTE: index captures thre relationships between objects & tags
        FT_TagProduct<std::uint64_t> makeTagProduct(const std::vector<std::uint64_t> &objects,
            const std::vector<std::uint64_t> &tags, const std::unordered_map<std::uint64_t, std::vector<uint64_t> > &index)
        {
            return FT_TagProduct<std::uint64_t>(
                std::make_unique<FT_FixedKeyIterator<std::uint64_t>>(objects.data(), objects.data() + objects.size()),
                std::make_unique<FT_FixedKeyIterator<std::uint64_t>>(tags.data(), tags.data() + tags.size()),
                makeFactory(index)
            );
        }
        
    protected:
        std::unordered_map<std::uint64_t, std::vector<std::uint64_t> > m_index_1 {
            { 0, { 0, 1, 2, 3 }},
            { 1, { 3, 4, 5 }}
        };
        std::unordered_map<std::uint64_t, std::vector<std::uint64_t> > m_index_2 {
            { 9, { 0, 1, 2, 3 }},
            { 6, { 3, 4, 5 }}
        };
    };
    
    TEST_F( FT_TagProductTest, testTP_IterateOverResults )
    {                
        std::vector<std::uint64_t> objects { 1, 2, 3 };
        std::vector<std::uint64_t> tags { 0, 1 };
        
        auto cut = makeTagProduct(objects, tags, m_index_1);
        unsigned int count = 0;
        const std::uint64_t *key;
        while (!cut.isEnd()) {
            cut.next(&key);      
            ++count;
        }
        ASSERT_EQ(count, 4);
    }
    
    TEST_F( FT_TagProductTest, testTP_JoinOperation )
    {                
        std::vector<std::uint64_t> objects { 1, 2, 3 };
        std::vector<std::uint64_t> tags { 6, 9 };
        
        // join key / expected key
        std::vector<std::pair<TP_Vector<std::uint64_t>, TP_Vector<std::uint64_t>>> join_data {
            {{2, 15}, {2, 9}},
            {{2, 7}, {3, 6}},
            {{3, 6}, {3, 6}}
        };

        auto cut = makeTagProduct(objects, tags, m_index_2);
        for (auto &key: join_data) {
            ASSERT_FALSE(cut.isEnd());
            ASSERT_TRUE(cut.join(key.first));
            ASSERT_EQ(key.second, cut.getKey());
        }
        
        // iteration past bounds
        auto last_key = TP_Vector<std::uint64_t>({1, 6});
        ASSERT_FALSE(cut.join(last_key));
    }
    
}   