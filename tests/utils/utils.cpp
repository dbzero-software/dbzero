#include "utils.hpp"
#include <sys/stat.h>
#include <algorithm>
#include <iostream>
#include <sstream>
#include <fstream>
#include <string>
#include <filesystem>

namespace db0::tests

{

    bool file_exists(const char *filename) {
        struct stat buffer;   
        return (stat(filename, &buffer) == 0);
    }

    void drop(const char *filename) {
        if (file_exists(filename)) {
            std::remove(filename);
        }
    }
     
    std::vector<char> randomPage(std::size_t size) {
        std::vector<char> result(size);
        for (auto &c: result) {
            c = rand();
        }
        return result;
    }

    bool equal(const std::vector<char> &v1, const std::vector<char> &v2)
    {
        if (v1.size() != v2.size()) {
            return false;
        }
        for (unsigned int i = 0; i < v1.size(); ++i) {
            if (v1[i] != v2[i]) {
                return false;
            }
        }
        return true;
    }

    std::string randomToken(int min_len, int max_len)
    {
        int len = min_len + rand()%(max_len-min_len);
        std::stringstream _str;        
        while ((len--) > 0) {
            _str << 'a' + rand()%('z'-'a');
        }
        return _str.str();
    }
    
}