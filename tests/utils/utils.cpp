// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

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
    
    std::vector<std::vector<std::uint32_t>> loadArray(const std::string &file_name)
    {
        std::vector<std::vector<std::uint32_t>> result;
        std::ifstream file(file_name);
        
        if (!file.is_open()) {
            throw std::runtime_error("Failed to open file: " + file_name);
        }
        
        std::string line;
        while (std::getline(file, line)) {
            // Skip empty lines
            if (line.empty()) {
                continue;
            }
            
            std::vector<std::uint32_t> row;
            std::stringstream ss(line);
            std::string value;
            
            while (std::getline(ss, value, ',')) {
                // Trim whitespace
                value.erase(0, value.find_first_not_of(" \t\r\n"));
                value.erase(value.find_last_not_of(" \t\r\n") + 1);
                
                if (!value.empty()) {
                    row.push_back(std::stoul(value));
                }
            }
            
            if (!row.empty()) {
                result.push_back(row);
            }
        }
        
        file.close();
        return result;
    }
    
}