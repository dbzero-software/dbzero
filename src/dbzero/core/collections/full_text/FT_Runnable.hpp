#pragma once

#include <memory>
#include <dbzero/core/serialization/Serializable.hpp>

namespace db0

{

    class Snapshot;
    class FT_IteratorBase;
    
    // FT_Runnable implementations must register here
    enum class FTRunnableType: std::uint16_t
    {
        Invalid = 0,
        Index = 1,
        And = 2,
        Or = 3,
        AndNot = 4,
        Sorted = 5,
        Range = 6,
        Null = 7
    };
    
    /**
     * The FT_Runnable is a factory interface to construct FT_IteratorBase
     * from user provided arguments (i.e. the parametrized query concept)
    */
    class FT_Runnable: public db0::serial::Serializable
    {
    public:
        virtual ~FT_Runnable() = default;

        // FIXME: allow passing user arguments
        virtual std::unique_ptr<FT_IteratorBase> run(db0::Snapshot &) const = 0;
        
        virtual FTRunnableType typeId() const = 0;
    };
    
    class FT_NullRunnable: public FT_Runnable
    {
    public:
        std::unique_ptr<FT_IteratorBase> run(db0::Snapshot &) const override;

        FTRunnableType typeId() const override;

        void serialize(std::vector<std::byte> &) const override;
    };

}