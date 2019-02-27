#include "groups.h"

#include "DB.h"

hlcup::groups::groups() {
    _fields = {
//            FIELD_FNAME,
//            FIELD_SNAME,
//            FIELD_PHONE,
//            FIELD_BIRTH,
            FIELD_COUNTRY,
            FIELD_CITY,
            FIELD_SEX,
            FIELD_STATUS,
            FIELD_INTERESTS,
////            FIELD_PREMIUM,
////            FIELD_LIKES,
////            FIELD_EMAIL,
//            FIELD_JOINED
    };

    _permutations.emplace_back(0);

    for (const auto &f : _fields) {
        std::vector<uint16_t> newValues;
        for (const auto &v: _permutations) {
            uint8_t sz = 0;
            for (auto &f: _fields)
                if (v & f)
                    ++sz;

            if (sz < 2)
                newValues.emplace_back(v | f);
        }
        for (const auto &v: newValues)
            _permutations.emplace_back(v);
    }

//    std::cerr << "GROUPS" << std::endl;
//    for (auto &v: _permutations) {
//        for (auto &f: _fields) {
//            if (v & f)
//                std::cerr << f << ", ";
//        }
//        std::cerr << std::endl;
//    }
//    std::cerr << "GROUPS END" << std::endl;

    pthread_mutex_init(&_kvMtx, nullptr);
}

uint16_t hlcup::groups::getFieldValue(const hlcup::Account &account, uint16_t field) {
    const time_t birth = account._birth;
    const time_t joined = account._joined;

    switch (field) {
        case FIELD_FNAME:
            return account._fname;
        case FIELD_SNAME:
            return account._sname;
//        case FIELD_PHONE:
//            return account._phone;
        case FIELD_BIRTH:
            return static_cast<uint16_t>(gmtime(&birth)->tm_year + 1900);
        case FIELD_COUNTRY:
            return account._country;
        case FIELD_CITY:
            return account._city;
        case FIELD_SEX:
            return static_cast<uint16_t>(account._man);
        case FIELD_STATUS:
            return account._status;
        case FIELD_JOINED:
            return static_cast<uint16_t>(gmtime(&joined)->tm_year + 1900);
        default:
            return UINT16_MAX;
    }
}

std::string hlcup::groups::getFieldStringValue(const uint16_t field, const uint16_t value) const {
    switch (field) {
        case FIELD_BIRTH:
            return std::to_string(value);
        case FIELD_COUNTRY:
            return DB::_dictCountry.Get(value);
        case FIELD_CITY:
            return DB::_dictCity.Get(value);
        case FIELD_SEX:
            return value ? "m" : "f";
        case FIELD_STATUS:
            return DB::_dictStatus.Get(value);
        case FIELD_INTERESTS:
            return DB::_dictInterests.Get(value);
        case FIELD_JOINED:
            return std::to_string(value);
        default:
            return "UNKNOWN";
    }
}

void hlcup::groups::Add(const hlcup::Account &account, bool dec) {
    for (const auto &p: _permutations) {
        std::vector<std::vector<uint16_t>> keys;
        keys.emplace_back();
        for (const auto &f: _fields) {
            if ((p & f) == 0)
                continue;
            if (f == FIELD_INTERESTS) {
                auto key = keys[0];
                keys.clear();
                for (const auto &interest: account._interests) {
                    auto newKey = key;
                    newKey.emplace_back(interest);
                    keys.emplace_back(newKey);
                }

            } else {
                for (auto &key : keys)
                    key.emplace_back(getFieldValue(account, f));
            }
        }

        pthread_mutex_lock(&_kvMtx);
        for (auto &key : keys) {
            uint32_t statusJoined = (static_cast<uint32_t>(getFieldValue(account, FIELD_STATUS)) << 16) |
                                    getFieldValue(account, FIELD_JOINED);
            uint32_t statusBirth = (static_cast<uint32_t>(getFieldValue(account, FIELD_STATUS)) << 16) |
                                   getFieldValue(account, FIELD_BIRTH);
            uint32_t statusSex = (static_cast<uint32_t>(getFieldValue(account, FIELD_STATUS)) << 16) |
                                 getFieldValue(account, FIELD_SEX);
            uint32_t sexJoined = (static_cast<uint32_t>(getFieldValue(account, FIELD_SEX)) << 16) |
                                 getFieldValue(account, FIELD_JOINED);
            uint32_t sexBirth = (static_cast<uint32_t>(getFieldValue(account, FIELD_SEX)) << 16) |
                                getFieldValue(account, FIELD_BIRTH);

            if (dec) {
                --_keysValues[p][key];
                --_keysValuesJoined[getFieldValue(account, FIELD_JOINED)][p][key];
                --_keysValuesStatus[getFieldValue(account, FIELD_STATUS)][p][key];
                --_keysValuesSex[getFieldValue(account, FIELD_SEX)][p][key];
                if (account._country)
                    --_keysValuesCountry[getFieldValue(account, FIELD_COUNTRY)][p][key];
                if (account._city)
                    --_keysValuesCity[getFieldValue(account, FIELD_CITY)][p][key];
                for (const auto &interest: account._interests)
                    --_keysValuesInterest[interest][p][key];
                --_keysValuesBirth[getFieldValue(account, FIELD_BIRTH)][p][key];
                --_keysValuesStatusJoined[statusJoined][p][key];
                --_keysValuesStatusBirth[statusBirth][p][key];
                --_keysValuesStatusSex[statusSex][p][key];
                --_keysValuesSexJoined[sexJoined][p][key];
                --_keysValuesSexBirth[sexBirth][p][key];
            } else {
                ++_keysValues[p][key];
                ++_keysValuesJoined[getFieldValue(account, FIELD_JOINED)][p][key];
                ++_keysValuesStatus[getFieldValue(account, FIELD_STATUS)][p][key];
                ++_keysValuesSex[getFieldValue(account, FIELD_SEX)][p][key];
                if (account._country)
                    ++_keysValuesCountry[getFieldValue(account, FIELD_COUNTRY)][p][key];
                if (account._city)
                    ++_keysValuesCity[getFieldValue(account, FIELD_CITY)][p][key];
                for (const auto &interest: account._interests)
                    ++_keysValuesInterest[interest][p][key];
                ++_keysValuesBirth[getFieldValue(account, FIELD_BIRTH)][p][key];
                ++_keysValuesStatusJoined[statusJoined][p][key];
                ++_keysValuesStatusBirth[statusBirth][p][key];
                ++_keysValuesStatusSex[statusSex][p][key];
                ++_keysValuesSexJoined[sexJoined][p][key];
                ++_keysValuesSexBirth[sexBirth][p][key];
            }
        }
        pthread_mutex_unlock(&_kvMtx);
    }
}

void hlcup::groups::FillRes(std::vector<std::pair<uint32_t, std::string>> &res, const std::vector<uint16_t> &fields,
                            std::vector<std::pair<uint16_t, uint16_t>> &filter) {

    std::unordered_map<uint16_t, boost::container::flat_map<std::vector<uint16_t>, uint32_t>> *keysValues = &_keysValues;

    if (filter.size() == 1) {
        std::unordered_map<uint16_t, std::unordered_map<uint16_t, boost::container::flat_map<std::vector<uint16_t>, uint32_t>>>::iterator it;

        switch (filter[0].first) {
            case FIELD_JOINED:
                it = _keysValuesJoined.find(filter[0].second);
                if (it == _keysValuesJoined.end())
                    return;
                break;

            case FIELD_STATUS:
                it = _keysValuesStatus.find(filter[0].second);
                if (it == _keysValuesStatus.end())
                    return;
                break;

            case FIELD_SEX:
                it = _keysValuesSex.find(filter[0].second);
                if (it == _keysValuesSex.end())
                    return;
                break;

            case FIELD_COUNTRY:
                it = _keysValuesCountry.find(filter[0].second);
                if (it == _keysValuesCountry.end())
                    return;
                break;

            case FIELD_CITY:
                it = _keysValuesCity.find(filter[0].second);
                if (it == _keysValuesCity.end())
                    return;
                break;

            case FIELD_INTERESTS:
                it = _keysValuesInterest.find(filter[0].second);
                if (it == _keysValuesInterest.end())
                    return;
                break;

            case FIELD_BIRTH:
                it = _keysValuesBirth.find(filter[0].second);
                if (it == _keysValuesBirth.end())
                    return;
                break;
        }

        keysValues = &it->second;
    } else if (filter.size() == 2) {
        uint16_t comb = 0;
        for (const auto &f: filter)
            comb |= f.first;

        std::unordered_map<uint32_t, std::unordered_map<uint16_t, boost::container::flat_map<std::vector<uint16_t>, uint32_t>>>::iterator it;

        switch (comb) {
            case FIELD_STATUS | FIELD_JOINED:
                it = _keysValuesStatusJoined.find(
                        filter[0].first == FIELD_STATUS
                        ? (static_cast<uint32_t>(filter[0].second) << 16) | filter[1].second
                        : (static_cast<uint32_t>(filter[1].second) << 16) | filter[0].second
                );
                if (it == _keysValuesStatusJoined.end())
                    return;
                break;

            case FIELD_STATUS | FIELD_BIRTH:
                it = _keysValuesStatusBirth.find(
                        filter[0].first == FIELD_STATUS
                        ? (static_cast<uint32_t>(filter[0].second) << 16) | filter[1].second
                        : (static_cast<uint32_t>(filter[1].second) << 16) | filter[0].second
                );
                if (it == _keysValuesStatusBirth.end())
                    return;
                break;

            case FIELD_STATUS | FIELD_SEX:
                it = _keysValuesStatusSex.find(
                        filter[0].first == FIELD_STATUS
                        ? (static_cast<uint32_t>(filter[0].second) << 16) | filter[1].second
                        : (static_cast<uint32_t>(filter[1].second) << 16) | filter[0].second
                );
                if (it == _keysValuesStatusSex.end())
                    return;
                break;

            case FIELD_SEX | FIELD_JOINED:
                it = _keysValuesSexJoined.find(
                        filter[0].first == FIELD_SEX
                        ? (static_cast<uint32_t>(filter[0].second) << 16) | filter[1].second
                        : (static_cast<uint32_t>(filter[1].second) << 16) | filter[0].second
                );
                if (it == _keysValuesSexJoined.end())
                    return;
                break;

            case FIELD_SEX | FIELD_BIRTH:
                it = _keysValuesSexBirth.find(
                        filter[0].first == FIELD_SEX
                        ? (static_cast<uint32_t>(filter[0].second) << 16) | filter[1].second
                        : (static_cast<uint32_t>(filter[1].second) << 16) | filter[0].second
                );
                if (it == _keysValuesSexBirth.end())
                    return;
                break;
        }

        keysValues = &it->second;
    }

    uint16_t fieldsKey = 0;
    for (const auto &f: fields) {
        fieldsKey |= f;
    }

    auto valuesIt = keysValues->find(fieldsKey);
    if (valuesIt == keysValues->end())
        return;

    std::unordered_map<uint16_t, uint8_t> fieldPos;
    uint8_t curPos = 0;
    for (const auto &f: _fields) {
        if ((f & fieldsKey) == 0)
            continue;
        fieldPos[f] = curPos;
//        std::cerr << f << ": " << static_cast<uint16_t>(curPos) << std::endl;
        ++curPos;
    }

    for (auto const &p : valuesIt->second) {
        if (p.second == 0)
            continue;

        std::string resKey;
        bool first = true;
        for (const auto &f : fields) {
            if (first)
                first = false;
            else
                resKey += "|";
            resKey += getFieldStringValue(f, p.first[fieldPos[f]]);
        }

        res.emplace_back(p.second, resKey);
    }
}

