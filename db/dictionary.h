#ifndef HLCUP2018_DICTIONARY_H
#define HLCUP2018_DICTIONARY_H

#include <string>
#include <vector>
#include <unordered_map>
#include <iostream>

#define DICT_NONE UINT16_MAX

namespace hlcup {
    class dictionary {
    public:
        uint16_t Add(const char *value) {
            const auto it = _valuesMap.find(value);
            if (it != _valuesMap.cend()) {
                return it->second;
            }

            if (_values.size() == UINT16_MAX - 1) {
                std::cerr << "Reached max dictionary size" << std::endl;
                return DICT_NONE;
            }

            auto id = static_cast<uint16_t>(_values.size());
            _values.emplace_back(value);
            _valuesMap[value] = id;

            return id;
        }

        const char *Get(uint16_t id) {
            return _values[id].c_str();
        };

        const std::string &GetString(uint16_t id) {
            return _values[id];
        };

        uint16_t GetByValue(const char *value) {
            const auto it = _valuesMap.find(value);
            if (it != _valuesMap.cend()) {
                return it->second;
            }
            return DICT_NONE;
        }

        size_t Size() {
            return _values.size();
        }

    private:
        std::vector<std::string> _values;
        std::unordered_map<std::string, uint16_t> _valuesMap;
    };
}


#endif //HLCUP2018_DICTIONARY_H
