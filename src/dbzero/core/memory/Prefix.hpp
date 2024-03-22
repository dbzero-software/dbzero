#pragma once

#include <dbzero/core/memory/MemLock.hpp>
#include <dbzero/core/memory/AccessOptions.hpp>


namespace db0

{

    /**
     * The Prefix interface represents a single DB0 Prefix space
     * Prefix can either be mutable or immutable, commit can be performed on a mutable Prefix only
     * Prefix is associated with a specific state ID, which is mutated during commit
    */
    class Prefix
    {
    public:
        Prefix(std::string name);

        virtual ~Prefix() = default;

        /**
         * Maps a specific address range into memory of the current process
         *          
         * @param address the address to start from
         * @param size the range size (in bytes)
         * @return the memory lock object or exception thrown on invalid address / out of available range
        */
        virtual MemLock mapRange(std::uint64_t address, std::size_t size, FlagSet<AccessOptions> = {}) const = 0;
        
        /**
         * Get size of the storage used
         * 
         * @return size in bytes
        */
        virtual std::uint64_t size() const = 0;
        
        virtual std::size_t getPageSize() const = 0;
        
        virtual std::uint64_t getStateNum() const = 0;
        
        /**
         * Commit all local changes made since last commit
         * 
         * @return the state ID after commit
        */
        virtual std::uint64_t commit() = 0;

        virtual void close() = 0;
        
        /**
         * Get last update timestamp
        */
        virtual std::uint64_t getLastUpdated() const = 0;

        /**
         * Update read-only prefix data to the latest changes done by other processes
         * @return 0 if no changes have been applied, otherwise last update timestamp
        */
        virtual std::uint64_t refresh() = 0;

        virtual AccessType getAccessType() const = 0;

        const std::string &getName() const;
        
        /**
         * Get the read-only snapshot of the prefix (i.e. a view based on current state number)
         */
        virtual std::shared_ptr<Prefix> getSnapshot() const = 0;

    private:
        const std::string m_name;
    };

}