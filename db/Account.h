#ifndef HLCUP2018_ACCOUNT_H
#define HLCUP2018_ACCOUNT_H

#include <cstdint>
#include <string>
#include <unordered_set>
#include <ostream>
#include <vector>
#include <unordered_map>
#include <boost/container/flat_set.hpp>
#include "dictionary.h"
#include "ids_set.h"

namespace hlcup {
#define FIELD_FNAME  1
#define FIELD_SNAME  2
#define FIELD_PHONE  4
#define FIELD_BIRTH  8
#define FIELD_COUNTRY  16
#define FIELD_CITY  32
#define FIELD_SEX  64
#define FIELD_STATUS  128
#define FIELD_INTERESTS  256
#define FIELD_PREMIUM  512
#define FIELD_LIKES  1024
#define FIELD_EMAIL  2048
#define FIELD_JOINED  4096

#define LIKE_EPOCH 1451606400

    class /*__attribute__((packed))*/Account {
    public:
        Account();

        ~Account();

        bool UmnarshalJSON(const char *data, size_t len);

        std::vector<uint16_t> _interests;
        std::vector<std::pair<uint32_t, int32_t>> _likes;
        ids_set _likedMe;

        char *_email = nullptr;
        char *_phone = nullptr;
        uint64_t _interestsB[2] = {0, 0};

        uint32_t _id = 0;
        int32_t _birth = 0;
        int32_t _joined = 0;
        struct {
            int32_t start = 0, finish = 0;
        } _premium;

        uint16_t _fname = 0;
        uint16_t _sname = 0;
        uint16_t _country = 0;
        uint16_t _city = 0;
        uint16_t _fields = 0;

        bool _notNull = false;
        bool _man = false;
        uint8_t _status = 0;

    };
}


#endif //HLCUP2018_ACCOUNT_H
