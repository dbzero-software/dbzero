#include "null_stream.hpp"

namespace db0::utils

{

	int NullBuffer::overflow(int c)
    {
        return c;
    }
    
    NullBuffer nullBuffer; 
    std::ostream nullStream(&nullBuffer);

}
