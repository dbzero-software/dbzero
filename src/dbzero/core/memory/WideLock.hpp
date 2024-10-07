#pragma once
    
#include "DP_Lock.hpp"

namespace db0

{

    /**
     * WideLock consists of the wide (unaligned) range + the residual lock
     * the starting address is always page-aligned
    */
    class WideLock: public DP_Lock
    {
    public:
        WideLock(StorageContext, std::uint64_t address, std::size_t size, FlagSet<AccessOptions>,
            std::uint64_t read_state_num, std::uint64_t write_state_num, std::shared_ptr<DP_Lock> res_lock, 
            bool create_new = false);
        
        /**
         * Create a copied-on-write lock from an existing lock
        */
        WideLock(const WideLock &, std::uint64_t write_state_num, FlagSet<AccessOptions> access_mode, 
            std::shared_ptr<DP_Lock> res_lock);
        
        void flush() override;
        // Flush the residual part only of the wide lock
        void flushResidual();
        
    private:
        std::shared_ptr<DP_Lock> m_res_lock;
    };

}