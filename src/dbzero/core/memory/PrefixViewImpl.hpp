#pragma once

#include <dbzero/core/exception/Exceptions.hpp>

namespace db0

{

    template <typename PrefixImplT> class PrefixViewImpl: public Prefix
    {
    public:
        PrefixViewImpl(std::shared_ptr<PrefixImplT> prefix, std::uint64_t state_num)
            : Prefix(prefix->getName())
            , m_state_num(state_num)
            , m_prefix(prefix)
        {
        }

        MemLock mapRange(std::uint64_t address, std::size_t size, FlagSet<AccessOptions> = {}) const override;

        std::uint64_t size() const override;

        std::uint64_t getStateNum() const override;
        
        std::size_t getPageSize() const override;

        std::uint64_t commit() override;

        std::uint64_t getLastUpdated() const override;

        void close() override;
        
        std::uint64_t refresh() override;

        AccessType getAccessType() const override;

        std::shared_ptr<Prefix> getSnapshot(std::optional<std::uint64_t> state_num = {}) const override;

        BaseStorage &getStorage() const override;

    private:
        // state number is immutable
        const std::uint64_t m_state_num;
        std::shared_ptr<PrefixImplT> m_prefix;
    };

    template <typename PrefixImplT>
    MemLock PrefixViewImpl<PrefixImplT>::mapRange(std::uint64_t address, std::size_t size, FlagSet<AccessOptions> access_mode) const
    {        
        return m_prefix->mapRange(address, size, m_state_num, access_mode);
    }

    template <typename PrefixImplT>
    std::size_t PrefixViewImpl<PrefixImplT>::getPageSize() const
    {
        return m_prefix->getPageSize();
    }

    template <typename PrefixImplT>
    AccessType PrefixViewImpl<PrefixImplT>::getAccessType() const
    {
        return m_prefix->getAccessType();
    }
    
    template <typename PrefixImplT>
    std::uint64_t PrefixViewImpl<PrefixImplT>::size() const
    {
        return m_prefix->size();
    }

    template <typename PrefixImplT>
    std::uint64_t PrefixViewImpl<PrefixImplT>::getStateNum() const
    {
        return m_state_num;
    }

    template <typename PrefixImplT>
    std::uint64_t PrefixViewImpl<PrefixImplT>::commit()
    {
        THROWF(db0::InternalException)
            << "PrefixViewImpl::commit: cannot commit snapshot" << THROWF_END;        
    }

    template <typename PrefixImplT>
    std::uint64_t PrefixViewImpl<PrefixImplT>::getLastUpdated() const
    {
        return m_prefix->getLastUpdated();
    }

    template <typename PrefixImplT>
    void PrefixViewImpl<PrefixImplT>::close()
    {       
        // close does nothing
    }

    template <typename PrefixImplT>
    std::uint64_t PrefixViewImpl<PrefixImplT>::refresh()
    {
        // refresh does nothing
        return 0;
    }

    template <typename PrefixImplT>
    std::shared_ptr<Prefix> PrefixViewImpl<PrefixImplT>::getSnapshot(std::optional<std::uint64_t>) const
    {
        THROWF(db0::InternalException) 
            << "PrefixViewImpl::getSnapshot: cannot create snapshot from snapshot" << THROWF_END;
    }
    
    template <typename PrefixImplT>
    BaseStorage &PrefixViewImpl<PrefixImplT>::getStorage() const {
        return m_prefix->getStorage();
    }
    
}
