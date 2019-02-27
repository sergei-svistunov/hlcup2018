#include "Account.h"
#include "json.h"
#include "utils.h"
#include "DB.h"

hlcup::Account::Account() = default;

hlcup::Account::~Account() = default;

bool hlcup::Account::UmnarshalJSON(const char *data, size_t len) {
    JSON_SKIP_SPACES()
    JSON_START_OBJECT()

    while (true) {
        JSON_SKIP_SPACES()
        JSON_DATA_DEBUG()

        if (data[0] == '}') {
            _notNull = true;
            if (!_phone) _phone = const_cast<char *>("");

            return true;

        } else if (data[0] == ',') {
            data++;
            JSON_DATA_DEBUG()
            continue;

        } else if (strncmp(data, "\"id\"", 4) == 0) {
            data += 4;
            JSON_DATA_DEBUG()

            JSON_SKIP_SPACES()
            JSON_FIELDS_SEPARATOR()

            JSON_SKIP_SPACES()
            JSON_UINT(_id)

        } else if (strncmp(data, "\"email\"", 7) == 0) {
            _fields |= FIELD_EMAIL;
            data += 7;
            JSON_DATA_DEBUG()

            JSON_SKIP_SPACES()
            JSON_FIELDS_SEPARATOR()

            JSON_SKIP_SPACES()
            JSON_STRING(_email)

        } else if (strncmp(data, "\"fname\"", 7) == 0) {
            _fields |= FIELD_FNAME;
            data += 7;
            JSON_DATA_DEBUG()

            JSON_SKIP_SPACES()
            JSON_FIELDS_SEPARATOR()

            JSON_SKIP_SPACES()
            char *str;
            JSON_STRING(str);
            utf_decode(str);
            _fname = DB::_dictFName.Add(str);
            free(str);

        } else if (strncmp(data, "\"sname\"", 7) == 0) {
            _fields |= FIELD_SNAME;
            data += 7;
            JSON_DATA_DEBUG()

            JSON_SKIP_SPACES()
            JSON_FIELDS_SEPARATOR()

            JSON_SKIP_SPACES()
            char *str;
            JSON_STRING(str);
            utf_decode(str);
            _sname = DB::_dictSName.Add(str);
            free(str);

        } else if (strncmp(data, "\"phone\"", 7) == 0) {
            _fields |= FIELD_PHONE;
            data += 7;
            JSON_DATA_DEBUG()

            JSON_SKIP_SPACES()
            JSON_FIELDS_SEPARATOR()

            JSON_SKIP_SPACES()
            JSON_STRING(_phone)

        } else if (strncmp(data, "\"birth\"", 7) == 0) {
            _fields |= FIELD_BIRTH;
            data += 7;
            JSON_DATA_DEBUG()

            JSON_SKIP_SPACES()
            JSON_FIELDS_SEPARATOR()

            JSON_SKIP_SPACES()
            JSON_LONG(_birth)

        } else if (strncmp(data, "\"country\"", 9) == 0) {
            _fields |= FIELD_COUNTRY;
            data += 9;
            JSON_DATA_DEBUG()

            JSON_SKIP_SPACES()
            JSON_FIELDS_SEPARATOR()

            JSON_SKIP_SPACES()
            char *str;
            JSON_STRING(str);
            utf_decode(str);
            _country = DB::_dictCountry.Add(str);
            free(str);

        } else if (strncmp(data, "\"city\"", 6) == 0) {
            _fields |= FIELD_CITY;
            data += 6;
            JSON_DATA_DEBUG()

            JSON_SKIP_SPACES()
            JSON_FIELDS_SEPARATOR()

            JSON_SKIP_SPACES()
            char *str;
            JSON_STRING(str);
            utf_decode(str);
            _city = DB::_dictCity.Add(str);
            free(str);

        } else if (strncmp(data, "\"sex\"", 5) == 0) {
            _fields |= FIELD_SEX;
            data += 5;
            JSON_DATA_DEBUG()

            JSON_SKIP_SPACES()
            JSON_FIELDS_SEPARATOR()

            JSON_SKIP_SPACES()
            char *sex;
            JSON_STRING(sex)
            if (!sex[0] || (sex[0] != 'm' && sex[0] != 'f') || sex[1] != 0) {
                free(sex);
                JSON_ERROR("Invalid field sex")
            }
            _man = sex[0] == 'm';
            free(sex);

        } else if (strncmp(data, "\"status\"", 8) == 0) {
            _fields |= FIELD_STATUS;
            data += 8;
            JSON_DATA_DEBUG()

            JSON_SKIP_SPACES()
            JSON_FIELDS_SEPARATOR()

            JSON_SKIP_SPACES()
            char *strStatus;
            JSON_STRING(strStatus)
            utf_decode(strStatus);
            auto v = DB::_dictStatus.GetByValue(strStatus);
            if (v == DICT_NONE) {
                free(strStatus);
                JSON_ERROR("Invalid status");
            }
            _status = static_cast<uint8_t>(v);
            free(strStatus);

        } else if (strncmp(data, "\"joined\"", 8) == 0) {
            _fields |= FIELD_JOINED;
            data += 8;
            JSON_DATA_DEBUG()

            JSON_SKIP_SPACES()
            JSON_FIELDS_SEPARATOR()

            JSON_SKIP_SPACES()
            JSON_LONG(_joined)

        } else if (strncmp(data, "\"premium\"", 9) == 0) {
            _fields |= FIELD_PREMIUM;
            data += 9;
            JSON_DATA_DEBUG()

            JSON_SKIP_SPACES()
            JSON_FIELDS_SEPARATOR()

            JSON_SKIP_SPACES()
            JSON_START_OBJECT();

            while (true) {
                JSON_SKIP_SPACES()
                JSON_DATA_DEBUG()

                if (data[0] == '}') {
                    data++;
                    break;

                } else if (data[0] == ',') {
                    data++;
                    JSON_DATA_DEBUG()
                    continue;

                } else if (strncmp(data, "\"start\"", 7) == 0) {
                    data += 7;
                    JSON_DATA_DEBUG()

                    JSON_SKIP_SPACES()
                    JSON_FIELDS_SEPARATOR()

                    JSON_SKIP_SPACES()
                    JSON_LONG(_premium.start)

                } else if (strncmp(data, "\"finish\"", 8) == 0) {
                    data += 8;
                    JSON_DATA_DEBUG()

                    JSON_SKIP_SPACES()
                    JSON_FIELDS_SEPARATOR()

                    JSON_SKIP_SPACES()
                    JSON_LONG(_premium.finish)

                } else {
                    JSON_ERROR("Unknown field")
                }
            }

        } else if (strncmp(data, "\"interests\"", 11) == 0) {
            _fields |= FIELD_INTERESTS;
            data += 11;
            JSON_DATA_DEBUG()

            JSON_SKIP_SPACES()
            JSON_FIELDS_SEPARATOR()

            JSON_SKIP_SPACES()
            JSON_START_ARRAY();

            while (true) {
                JSON_SKIP_SPACES()
                JSON_DATA_DEBUG()

                if (data[0] == ']') {
                    data++;
                    break;

                } else if (data[0] == ',') {
                    data++;
                    JSON_DATA_DEBUG()
                    continue;

                } else if (data[0] == '"') {
                    JSON_DATA_DEBUG()
                    char *str;
                    JSON_STRING(str);
                    utf_decode(str);
                    auto interestId = DB::_dictInterests.Add(str);
                    _interests.emplace_back(interestId);
                    auto ind = 0;
                    if (interestId > 63) {
                        ind = 1;
                        interestId -= 64;
                    }
                    _interestsB[ind] |= (uint64_t(1) << interestId);
                    free(str);

                } else {
                    JSON_ERROR("Unknown")
                }
            }

        } else if (strncmp(data, "\"likes\"", 7) == 0) {
            _fields |= FIELD_LIKES;
            data += 7;
            JSON_DATA_DEBUG()

            JSON_SKIP_SPACES()
            JSON_FIELDS_SEPARATOR()

            JSON_SKIP_SPACES()
            JSON_START_ARRAY()

            while (true) {
                JSON_SKIP_SPACES()
                JSON_DATA_DEBUG()

                if (data[0] == ']') {
                    data++;
                    break;

                } else if (data[0] == ',') {
                    data++;
                    JSON_DATA_DEBUG()
                    continue;

                } else if (data[0] == '{') {
                    data++;
                    JSON_DATA_DEBUG()

                    uint32_t id = 0;
                    int64_t ts = 0;
                    while (true) {
                        JSON_SKIP_SPACES()
                        JSON_DATA_DEBUG()

                        if (data[0] == '}') {
                            data++;
                            break;

                        } else if (data[0] == ',') {
                            data++;
                            JSON_DATA_DEBUG()
                            continue;

                        } else if (strncmp(data, "\"id\"", 4) == 0) {
                            data += 4;
                            JSON_DATA_DEBUG()

                            JSON_SKIP_SPACES()
                            JSON_FIELDS_SEPARATOR()

                            JSON_SKIP_SPACES()
                            JSON_UINT(id)

                        } else if (strncmp(data, "\"ts\"", 4) == 0) {
                            data += 4;
                            JSON_DATA_DEBUG()

                            JSON_SKIP_SPACES()
                            JSON_FIELDS_SEPARATOR()

                            JSON_SKIP_SPACES()
                            JSON_LONG(ts);

                        } else {
                            JSON_ERROR("Unknown field")
                        }
                    }

                    _likes.emplace_back(id, ts - LIKE_EPOCH);

                } else {
                    JSON_ERROR("Unknown")
                }
            }

        } else {
            JSON_ERROR("Unknown field")
        }
    }
}
