#include "DB.h"
#include "groups.h"

#include <zip.h>
#include <cstring>
#include <chrono>

hlcup::dictionary hlcup::DB::_dictStatus;
hlcup::dictionary hlcup::DB::_dictInterests;
hlcup::dictionary hlcup::DB::_dictCountry;
hlcup::dictionary hlcup::DB::_dictCity;
hlcup::dictionary hlcup::DB::_dictFName;
hlcup::dictionary hlcup::DB::_dictSName;

hlcup::groups hlcup::DB::_groups;

hlcup::DB::DB() {
}

void hlcup::DB::Load(const char *filename) {
    auto start = std::chrono::high_resolution_clock::now();

    _accounts = new Account[ACCOUNTS_CNT];
    _ts = static_cast<uint64_t>(time(nullptr));

    _dictStatus.Add("свободны");
    _dictStatus.Add("всё сложно");
    _dictStatus.Add("заняты");
    _dictCountry.Add("");
    _dictCity.Add("");
    _dictFName.Add("");
    _dictSName.Add("");

    _indBySex[0].reserve(ACCOUNTS_CNT / 2 + 1000);
    _indBySex[1].reserve(ACCOUNTS_CNT / 2 + 1000);

    _indByStatus.resize(3);

    _indByNotNullCity.reserve(ACCOUNTS_CNT * 2 / 3);
    _indByNotNullCountry.reserve(ACCOUNTS_CNT * 2 / 3);
    _indByNotNullPhone.reserve(ACCOUNTS_CNT * 2 / 3);
    _indByNotNullFName.reserve(ACCOUNTS_CNT * 2 / 3);
    _indByNotNullSName.reserve(ACCOUNTS_CNT * 2 / 3);

    std::string optionsFile(filename);
    optionsFile.resize(optionsFile.size() - 8);
    optionsFile.append("options.txt");
    auto f = fopen(optionsFile.c_str(), "r");

    if (f != nullptr) {
        char *line = nullptr;
        size_t len = 0;
        if (getline(&line, &len, f) == 0)
            std::cerr << "Cannot read TS" << std::endl;
        _ts = static_cast<uint64_t>(strtol(line, nullptr, 10));
        std::cerr << "TS: " << _ts << "\t" << time(nullptr) << std::endl;
        fclose(f);
    }

    int err;
    auto za = zip_open(filename, 0, &err);
    if (za == nullptr) {
        perror("Cannot open zip");
        exit(1);
    }

    // Read options
    for (zip_uint64_t i = 0; i < zip_get_num_entries(za, 0); i++) {
        struct zip_stat sb{};
        if (zip_stat_index(za, i, 0, &sb) != 0) {
            perror("zip stat");
            exit(1);
        }

        if (strcmp(sb.name, "options.txt") == 0) {
            auto zf = zip_fopen_index(za, i, 0);
            if (!zf) {
                perror("zip_fopen_index");
                exit(1);
            }
            auto buf = new char[sb.size + 1];
            buf[sb.size] = 0;
            zip_fread(zf, buf, sb.size);
            zip_fclose(zf);
            _ts = static_cast<uint64_t>(strtol(buf, nullptr, 10));
            std::cerr << "TS: " << _ts << "\t" << time(nullptr) << std::endl;
            delete[] buf;
            break;
        }
    }

    for (zip_uint64_t i = 0; i < zip_get_num_entries(za, 0); i++) {
        struct zip_stat sb{};
        if (zip_stat_index(za, i, 0, &sb) != 0) {
            perror("zip stat");
            exit(1);
        }

        if (strncmp(sb.name, "accounts", 8) != 0) {
            continue;
        }

        for (auto offset = 0; offset < 1; ++offset) {
            auto zf = zip_fopen_index(za, i, 0);
            if (!zf) {
                perror("zip_fopen_index");
                exit(1);
            }
            auto buf = new char[sb.size + 1];
            buf[sb.size] = 0;
            auto cnt = zip_fread(zf, buf, sb.size);
            if (cnt != sb.size) {
                perror("not full read");
                exit(1);
            }
            zip_fclose(zf);

            loadAccounts(buf, offset);
            delete[] buf;
        }
    }

    std::cerr << "MaxId: " << _maxId << std::endl;

    for (auto i = _maxId; i > 0; --i)
        if (_accounts[i]._notNull) {
            _indByEmail.insert(_accounts[i]._email);
            InsertIndex(_accounts[i]);
        }

    auto finish = std::chrono::high_resolution_clock::now();
    std::cerr << "Loaded in "
              << double(std::chrono::duration_cast<std::chrono::nanoseconds>(finish - start).count()) / 1000000000.0
              << "seconds\n";

    zip_close(za);
}

void hlcup::DB::loadAccounts(std::string data, int offset) {
    auto start = data.find('[', 0);
    if (start == std::string::npos) {
        perror("Not found [");
        exit(1);
    }

    ++start;
    while (true) {
        start = data.find('{', start);
        if (start == std::string::npos) {
            break;
        }

        size_t quotes = 1;
        auto end = start;
        while (quotes) {
            end = data.find_first_of("{}", end + 1);
            if (data[end] == '}')
                quotes--;
            else {
                quotes++;
            }
        }

        auto json = data.substr(start, end - start + 1);
//        std::cerr << json << std::endl;

        Account account;
        if (!account.UmnarshalJSON(json.c_str(), json.length())) {
            std::cerr << json << std::endl;
            break;
        }

        if (offset) {
            account._id += offset * 10000;
            for (auto &like : account._likes) {
                like.first += offset * 10000;
            }
        }

        if (account._id > _maxId)
            _maxId = account._id;

        _accounts[account._id] = account;

//        InsertIndex(account);

        start = end + 1;
    }
}

void hlcup::DB::InsertIndex(const hlcup::Account &account) {
    _indBySex[account._man].insert(account._id);

    _indByDomain[strchr(account._email, '@') + 1].insert(account._id);

    if (account._status >= _indByStatus.size())
        _indByStatus.resize(account._status + 1);
    _indByStatus[account._status].insert(account._id);

    if (account._fname >= _indByFName.size())
        _indByFName.resize(account._fname + 1);
    _indByFName[account._fname].insert(account._id);
    if (account._fname)
        _indByNotNullFName.insert(account._id);

    if (account._sname >= _indBySName.size())
        _indBySName.resize(account._sname + 1);
    _indBySName[account._sname].insert(account._id);
    if (account._sname)
        _indByNotNullSName.insert(account._id);

    uint16_t phone = 0;
    if (account._phone[0]) {
        phone = (uint16_t) strtol(strchr(account._phone, '(') + 1, nullptr, 10);
    }
    _indByPhone[phone].insert(account._id);
    if (phone)
        _indByNotNullPhone.insert(account._id);

    if (account._country >= _indByCountry.size())
        _indByCountry.resize(account._country + 1);
    _indByCountry[account._country].insert(account._id);
    if (account._country)
        _indByNotNullCountry.insert(account._id);

    if (account._city >= _indByCity.size())
        _indByCity.resize(account._city + 1);
    _indByCity[account._city].insert(account._id);
    if (account._city)
        _indByNotNullCity.insert(account._id);

    const time_t birth = account._birth;
    _indByBirthYear[gmtime(&birth)->tm_year + 1900].insert(account._id);

    const time_t joined = account._joined;
    _indByJoinedYear[gmtime(&joined)->tm_year + 1900].insert(account._id);

    for (auto &interest : account._interests) {
        if (interest >= _indByInterests.size())
            _indByInterests.resize(static_cast<size_t>(interest + 1));
        _indByInterests[interest].insert(account._id);
    }

    for (auto &like : account._likes) {
        _accounts[like.first]._likedMe.insert(account._id);
    }

    bool nowPremium = account._premium.start <= _ts && _ts <= account._premium.finish;
    if (account._premium.start != 0) {
        _indByHadPremium[true].insert(account._id);
        if (nowPremium)
            _indByNowPremium.insert(account._id);
    } else {
        _indByHadPremium[false].insert(account._id);
    }

    for (const auto &interest: account._interests)
        _indInterestPremiumStatus[interest][nowPremium][account._status].insert(account._id);
    _groups.Add(account, false);
}

void hlcup::DB::UpdateIndex(const hlcup::Account &account) {
    auto id = account._id;

    if (account._fields & FIELD_EMAIL && strcmp(account._email, _accounts[id]._email) != 0) {
        auto oldDomain = strchr(_accounts[id]._email, '@') + 1;
        auto newDomain = strchr(account._email, '@') + 1;
        if (strcmp(oldDomain, newDomain) != 0) {
            auto it = _indByDomain.find(oldDomain);
            if (it != _indByDomain.end()) {
                it->second.erase(id);
                if (it->second.empty())
                    _indByDomain.erase(it);
            }
            _indByDomain[newDomain].insert(id);
        }

        _accounts[id]._email = account._email;
    }

    _groups.Add(_accounts[id], true);

    if (account._fields & (FIELD_STATUS | FIELD_INTERESTS | FIELD_PREMIUM)) {
        auto nowPremium = _accounts[id]._premium.start <= _ts && _ts <= _accounts[id]._premium.finish;
        for (const auto &interest: _accounts[id]._interests)
            _indInterestPremiumStatus[interest][nowPremium][_accounts[id]._status].erase(account._id);
    }

    if (account._fields & FIELD_FNAME && account._fname != _accounts[id]._fname) {
        _indByFName[_accounts[id]._fname].erase(id);

        if (account._fname >= _indByFName.size())
            _indByFName.resize(account._fname + 1);
        _indByFName[account._fname].insert(id);

        if (!_accounts[id]._fname && account._fname)
            _indByNotNullFName.insert(id);
        else if (_accounts[id]._fname && !account._fname)
            _indByNotNullFName.erase(id);

        _accounts[id]._fname = account._fname;
    }

    if (account._fields & FIELD_SNAME && account._sname != _accounts[id]._sname) {
        _indBySName[_accounts[id]._sname].erase(id);

        if (account._sname >= _indBySName.size())
            _indBySName.resize(account._sname + 1);
        _indBySName[account._sname].insert(id);

        if (!_accounts[id]._sname && account._sname)
            _indByNotNullSName.insert(id);
        else if (_accounts[id]._sname && !account._sname)
            _indByNotNullSName.erase(id);

        _accounts[id]._sname = account._sname;
    }

    if (account._fields & FIELD_PHONE && strcmp(account._phone, _accounts[id]._phone) != 0) {
        auto oldCodeStr = strchr(_accounts[id]._phone, '(');
        uint16_t old_phone = 0;
        if (oldCodeStr)
            old_phone = static_cast<uint16_t>(strtol(oldCodeStr + 1, nullptr, 10));

        auto newCodeStr = strchr(account._phone, '(');
        uint16_t new_phone = 0;
        if (newCodeStr)
            new_phone = (uint16_t) strtol(newCodeStr + 1, nullptr, 10);

        if (old_phone != new_phone) {
            _indByPhone[old_phone].erase(id);
            _indByPhone[new_phone].insert(id);
        }

        if (!_accounts[id]._phone[0] && account._phone[0])
            _indByNotNullPhone.insert(id);
        else if (_accounts[id]._phone[0] && !account._phone[0])
            _indByNotNullPhone.erase(id);

        _accounts[id]._phone = account._phone;
    }

    if (account._fields & FIELD_BIRTH && account._birth != _accounts[id]._birth) {
        const time_t oldBirth = _accounts[id]._birth;
        auto old_birth_year = gmtime(&oldBirth)->tm_year;

        const time_t newBirth = account._birth;
        auto new_birth_year = gmtime(&newBirth)->tm_year;
        if (old_birth_year != new_birth_year) {
            _indByBirthYear[old_birth_year + 1900].erase(id);
            _indByBirthYear[new_birth_year + 1900].insert(id);
        }

        _accounts[id]._birth = account._birth;
    }

    if (account._fields & FIELD_COUNTRY && account._country != _accounts[id]._country) {
        _indByCountry[_accounts[id]._country].erase(id);

        if (account._country >= _indByCountry.size())
            _indByCountry.resize(account._country + 1);
        _indByCountry[account._country].insert(id);

        if (!_accounts[id]._country && account._country)
            _indByNotNullCountry.insert(id);
        else if (_accounts[id]._country && !account._country)
            _indByNotNullCountry.erase(id);

        _accounts[id]._country = account._country;
    }

    if (account._fields & FIELD_CITY && account._city != _accounts[id]._city) {
        _indByCity[_accounts[id]._city].erase(id);

        if (account._city >= _indByCity.size())
            _indByCity.resize(account._city + 1);
        _indByCity[account._city].insert(id);

        if (!_accounts[id]._city && account._city)
            _indByNotNullCity.insert(id);
        else if (_accounts[id]._city && !account._city)
            _indByNotNullCity.erase(id);

        _accounts[id]._city = account._city;
    }

    if (account._fields & FIELD_SEX && account._man != _accounts[id]._man) {
        _indBySex[_accounts[id]._man].erase(id);
        _indBySex[account._man].insert(id);
        _accounts[id]._man = account._man;
    }

    if (account._fields & FIELD_STATUS && account._status != _accounts[id]._status) {
        _indByStatus[_accounts[id]._status].erase(id);
        _indByStatus[account._status].insert(id);
        _accounts[id]._status = account._status;
    }

    if (account._fields & FIELD_INTERESTS) {
        for (const auto &interest: _accounts[id]._interests)
            _indByInterests[interest].erase(id);

        for (const auto &interest: account._interests) {
            if (interest >= _indByInterests.size())
                _indByInterests.resize(static_cast<size_t>(interest + 1));
            _indByInterests[interest].insert(id);
        }

        _accounts[id]._interests = account._interests;
        _accounts[id]._interestsB[0] = account._interestsB[0];
        _accounts[id]._interestsB[1] = account._interestsB[1];
    }

    if (account._fields & FIELD_PREMIUM) {
        auto old_now_premium =
                _accounts[id]._premium.start <= _ts && _ts <= _accounts[id]._premium.finish;

        auto new_now_premium = account._premium.start <= _ts && _ts <= account._premium.finish;

        if (old_now_premium != new_now_premium) {
            if (old_now_premium)
                _indByNowPremium.erase(id);
            if (new_now_premium)
                _indByNowPremium.insert(id);
        }

        if ((_accounts[id]._premium.start != 0) != (account._premium.start != 0)) {
            _indByHadPremium[_accounts[id]._premium.start != 0].erase(id);
            _indByHadPremium[account._premium.start != 0].insert(id);
        }

        _accounts[id]._premium = account._premium;
    }

    if (account._fields & FIELD_LIKES) {
        for (const auto &like: _accounts[id]._likes)
            _accounts[like.first]._likedMe.erase(id);
        for (const auto &like: account._likes)
            _accounts[like.first]._likedMe.insert(id);

        _accounts[id]._likes = account._likes;
    }

    if (account._fields & (FIELD_STATUS | FIELD_INTERESTS | FIELD_PREMIUM)) {
        auto nowPremium = _accounts[id]._premium.start <= _ts && _ts <= _accounts[id]._premium.finish;
        for (const auto &interest: _accounts[id]._interests)
            _indInterestPremiumStatus[interest][nowPremium][_accounts[id]._status].insert(account._id);
    }

    _groups.Add(_accounts[id], false);
}
