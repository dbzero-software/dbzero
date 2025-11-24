#pragma once

#include "BlockIOStream.hpp"
#include "ChangeLog.hpp"
#include <dbzero/core/serialization/Base.hpp>
#include <dbzero/core/collections/rle/RLE_Sequence.hpp>

namespace db0

{
    
    /**
     * The BlockIOStream specialization to collect change-logs as separate chunks
     * @tparam HeaderT the optional header type of change-log chunks
    */
    template <typename o_change_log_t = db0::o_change_log<> >
    class ChangeLogIOStream: public BlockIOStream
    {
    public:
        /**
         * Note that checksums are always enabled for the ChangeLogStream
        */
        ChangeLogIOStream(CFile &m_file, std::uint64_t begin, std::uint32_t block_size,
            std::function<std::uint64_t()> tail_function = {}, AccessType = AccessType::READ_WRITE);
        ChangeLogIOStream(BlockIOStream &&);
        
        /**
         * This method encodes the provided change log vector and appends it as a separate chunk.
         * The operation replaces the last stored change log chunk.
         * @param data the change log data to be appended         
        */        
        template <typename... Args>
        const o_change_log_t &appendChangeLog(ChangeLogData &&data, Args&&... args);
        
        /**
         * Read a single change-log chunk from the stream.
         * The operation overwrites result of the previous read (unless nullptr is returned)        
         * @return the change-log sequence or nullptr if end of the stream reached
        */
        const o_change_log_t *readChangeLogChunk();
        
        // Read chunk, bring your own buffer
        const o_change_log_t *readChangeLogChunk(std::vector<char> &buffer);
        
        /**
         * Get last read or written change log chunk
        */
        const o_change_log_t *getLastChangeLogChunk() const;
        
        class Reader
        {
        public:
            Reader(ChangeLogIOStream &);

            const o_change_log_t *readChangeLogChunk();

            // initialize reading from the beginning
            void reset();
            
        private:
            ChangeLogIOStream &m_stream;
            std::list<std::vector<char> > m_buffers;
            std::list<std::vector<char> >::const_iterator m_it_next_buffer;
        };
        
        // The buffering proxy for write operations
        // changes are only reflected with the underlying stream on "flush"
        class Writer
        {
        public:
            void appendChangeLog(const o_change_log_t &);
            void flush();

        private:
        };
        
        // Retrieves a caching reaader, which allows multiple scan over the same data
        Reader getStreamReader();

    private:
        const o_change_log_t *m_last_change_log_ptr = nullptr;
        std::vector<char> m_buffer;
    };
    
    template <typename o_change_log_t>
    template <typename... Args>
    const o_change_log_t &ChangeLogIOStream<o_change_log_t>::appendChangeLog(ChangeLogData &&data, Args&&... args)
    {
        auto size_of = o_change_log_t::measure(data, std::forward<Args>(args)...);
        if (m_buffer.size() < size_of) {
            m_buffer.resize(size_of);
        }
        
        o_change_log_t::__new(m_buffer.data(), data, std::forward<Args>(args)...);
        // append change log as a separate chunk
        BlockIOStream::addChunk(size_of);
        BlockIOStream::appendToChunk(m_buffer.data(), size_of);
        m_last_change_log_ptr = &o_change_log_t::__const_ref(m_buffer.data());
        assert(m_last_change_log_ptr->sizeOf() == size_of);

        return *m_last_change_log_ptr;
    }
    
    extern template class ChangeLogIOStream<>;
    
}