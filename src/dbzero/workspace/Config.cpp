// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "Config.hpp"

namespace db0

{
    
    Config::Config(ObjectPtr lang_config)
        : m_lang_config(lang_config)
    {
    }

    Config::~Config()
    {
        // NOTE: language interpreter may be invalid at this point
        if (!LangToolkit::isValid()) {
            // just drop the pointer without releasing
            m_lang_config.steal();
        }
    }
    
    const Config::ObjectSharedPtr& Config::getRawConfig() const {
        return m_lang_config;
    }
    
    bool Config::hasKey(const std::string &key) const {
        return LangToolkit::hasKey(m_lang_config.get(), key);
    }

    // long specialization
    template <> std::optional<long> get<long>(typename LangToolkit::ObjectPtr lang_dict, const std::string &key)
    {
        if (!lang_dict) {
            return std::nullopt;
        }
        return LangToolkit::getLong(lang_dict, key);
    }
    
    // unsigned long long specialization
    template <> std::optional<unsigned long long> get<unsigned long long>(
        typename LangToolkit::ObjectPtr lang_dict, const std::string &key)
    {
        if (!lang_dict) {
            return std::nullopt;
        }
        return LangToolkit::getUnsignedLongLong(lang_dict, key);
    }
    
    // unsigned int specialization
    template <> std::optional<unsigned int> get<unsigned int>(
        typename LangToolkit::ObjectPtr lang_dict, const std::string &key)
    {
        if (!lang_dict) {
            return std::nullopt;
        }
        return LangToolkit::getUnsignedInt(lang_dict, key);
    }

    // bool specialization
    template <> std::optional<bool> get<bool>(typename LangToolkit::ObjectPtr lang_dict, const std::string &key)
    {
        if (!lang_dict) {
            return std::nullopt;
        }
        return LangToolkit::getBool(lang_dict, key);
    }

    // string specialization
    template <> std::optional<std::string> get<std::string>(typename LangToolkit::ObjectPtr lang_dict, const std::string &key)
    {
        if (!lang_dict) {
            return std::nullopt;
        }
        return LangToolkit::getString(lang_dict, key);
    }

}