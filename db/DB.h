#ifndef HLCUP2018_DB_H
#define HLCUP2018_DB_H

#include <string>
#include <set>
#include <unordered_map>
#include <map>
#include <vector>
#include <algorithm>
#include "Account.h"
#include "Iterators.h"
#include "dictionary.h"
#include "groups.h"

#define ACCOUNTS_CNT 1400000

namespace hlcup {
    class DB {
    public:
        static dictionary _dictStatus;
        static dictionary _dictInterests;
        static dictionary _dictCountry;
        static dictionary _dictCity;
        static dictionary _dictFName;
        static dictionary _dictSName;
        static groups _groups;

        DB();

        void Load(const char *filename);

        void InsertIndex(const Account &account);

        void UpdateIndex(const Account &account);

        Account *_accounts;
        uint32_t _maxId = 0;
        int32_t _ts;
        ids_set _indBySex[2];
        std::unordered_map<std::string, ids_set> _indByDomain;
        std::unordered_set<std::string> _indByEmail;
        std::vector<ids_set> _indByStatus;
        std::vector<ids_set> _indByFName;
        ids_set _indByNotNullFName;
        std::vector<ids_set> _indBySName;
        ids_set _indByNotNullSName;
        std::unordered_map<uint16_t, ids_set> _indByPhone;
        ids_set _indByNotNullPhone;
        std::vector<ids_set> _indByCountry;
        ids_set _indByNotNullCountry;
        std::vector<ids_set> _indByCity;
        ids_set _indByNotNullCity;
        std::unordered_map<uint16_t, ids_set> _indByBirthYear;
        std::unordered_map<uint16_t, ids_set> _indByJoinedYear;
        std::vector<ids_set> _indByInterests;
//        std::unordered_map<uint32_t, ids_set> _indByLikes;
        ids_set _indByNowPremium;
        ids_set _indByHadPremium[2];
        ids_set _indInterestPremiumStatus[128][2][3];

    private:
        void loadAccounts(std::string data, int offset);
    };
}

#endif //HLCUP2018_DB_H
