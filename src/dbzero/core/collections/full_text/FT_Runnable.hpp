#pragma once

namespace db0

{

    class Snapshot;
    class FT_IteratorBase;
    
    /**
     * The FT_Runnable is a factory interface to construct FT_IteratorBase
     * from user provided arguments (i.e. the parametrized query concept)
    */
    class FT_Runnable
    {
    public:
        virtual ~FT_Runnable() = default;

        // FIXME: allow passing user arguments
        virtual std::unique_ptr<FT_IteratorBase> run(db0::Snapshot &) const = 0;
    };
    
}