// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <optional>
#include <dbzero/object_model/LangConfig.hpp>

namespace db0

{

    using LangToolkit = db0::object_model::LangConfig::LangToolkit;
    template <typename T> std::optional<T> get(typename LangToolkit::ObjectPtr, const std::string &key);
    
    // Wraps a Python dict object and provides getters for configuration variables
    class Config
    {
    public:        
        using ObjectPtr = LangToolkit::ObjectPtr;
        using ObjectSharedPtr = LangToolkit::ObjectSharedPtr;

        Config(ObjectPtr lang_config);
        ~Config();

        const ObjectSharedPtr& getRawConfig() const;

        template <typename T> std::optional<T> get(const std::string &key) const {
            return db0::get<T>(m_lang_config.get(), key);
        }

        template <typename T> T get(const std::string &key, T default_value) const {
            auto value = get<T>(key);
            return value ? *value : default_value;
        }
        
        bool hasKey(const std::string &key) const;
        
    private:
        ObjectSharedPtr m_lang_config;
    };
    
}