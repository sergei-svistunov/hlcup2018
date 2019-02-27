#ifndef HLCUP2018_GROUPS_H
#define HLCUP2018_GROUPS_H

#include <memory>
#include <boost/container/flat_map.hpp>
#include "Account.h"

namespace hlcup {

    struct container_hash {
        std::size_t operator()(std::vector<uint16_t> const &c) const {
            std::size_t res = 0;
            for (const auto &s: c)
                res ^= std::_Hash_impl::hash(s);
            return res;
        }
    };

    class groups {
    public:
        groups();

        void Add(const Account &account, bool dec);

        void FillRes(std::vector<std::pair<uint32_t, std::string>> &res, const std::vector<uint16_t> &fields,
                     std::vector<std::pair<uint16_t, uint16_t>> &filter);

    private:
        std::vector<uint16_t> _fields;
        std::vector<uint16_t> _permutations;

        uint16_t getFieldValue(const Account &account, uint16_t field);

        std::string getFieldStringValue(uint16_t field, uint16_t value) const;

        pthread_mutex_t _kvMtx;
        std::unordered_map<uint16_t, boost::container::flat_map<std::vector<uint16_t>, uint32_t>> _keysValues;
        std::unordered_map<uint16_t, std::unordered_map<uint16_t, boost::container::flat_map<std::vector<uint16_t>, uint32_t>>>
                _keysValuesJoined, _keysValuesStatus, _keysValuesSex, _keysValuesCountry, _keysValuesCity, _keysValuesInterest, _keysValuesBirth;
        std::unordered_map<uint32_t, std::unordered_map<uint16_t, boost::container::flat_map<std::vector<uint16_t>, uint32_t>>>
                _keysValuesStatusJoined, _keysValuesStatusBirth, _keysValuesStatusSex, _keysValuesSexJoined, _keysValuesSexBirth;
    };

}

#endif //HLCUP2018_GROUPS_H
