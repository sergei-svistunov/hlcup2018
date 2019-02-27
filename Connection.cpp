#include <bits/types/struct_iovec.h>
#include <sys/uio.h>
#include <unistd.h>
#include <chrono>
#include <tuple>
#include <cmath>
#include <x86intrin.h>
#include "Connection.h"
#include "db/utils.h"
#include "db/json.h"

#ifdef PPROF

#include <gperftools/profiler.h>

#endif

static hlcup::ids_set emptySet;

hlcup::Connection::Connection(int fd, hlcup::DB *db, UpdateQueue *updateQueue) : _fd(fd), _db(db), _content_length(0),
                                                                                 _method(UNKNOWN),
                                                                                 _requst_body(nullptr),
                                                                                 _updateQueue(updateQueue) {
    _inBuf.Reserve(10 * 1024);
    _outBuf.Reserve(8 * 1024);

    memset((void *) _inBuf.Data, 0, _inBuf.Capacity);
    memset((void *) _outBuf.Data, 0, _outBuf.Capacity);
}

hlcup::Connection::~Connection() = default;

void hlcup::Connection::Reset(int fd) {
    _fd = fd;
    _method = UNKNOWN;
    _inBuf.Reset();
    _outBuf.Reset();
    _path = nullptr;
    _requst_body = nullptr;
    _content_length = 0;
}

bool hlcup::Connection::ProcessEvent() {
    if (_requst_body != nullptr) {
        if (_inBuf.End - _requst_body >= _content_length) {
            doRequest();
            return true;
        }

        return false;
    }

    auto body = _inBuf.Data;
    if (strncmp(body, "GET ", 4) == 0) {
        _method = GET;
        body += 4;
    } else if (strncmp(body, "POST ", 5) == 0) {
        body += 5;
        _method = POST;
    } else {
        WriteBadRequest();
        return true;
    }

    _path = body;
    body = strchr(body, ' ');
    body[0] = 0;
    body++;

    body = strchr(body, '\n') + 1;

    // Read headers
    while (true) {
        auto start = body;
        // ToDo: optimize
        if (strncmp(body, "Content-Length:", 15) == 0) {
            _content_length = static_cast<size_t>(strtol(body + 16, nullptr, 10));
//            std::cerr << "Content Length: " << contentLength << std::endl;
        }
        body = strchr(body, '\n') + 1;
        if (body - start <= 2) break;
    }

    _requst_body = body;

    if (_method == GET || (_method == POST && _inBuf.End - body >= _content_length)) {
        doRequest();
        return true;
    }

    return false;
}

void hlcup::Connection::WriteResponse() {
    static const char *headerStart = "HTTP/1.0 200 OK\r\n"
                                     "Server: F\r\n"
                                     "Content-Type: application/json; encoding=utf-8\r\n"
                                     "Connection: keep-alive\r\n"
                                     "Content-Length: ",
            *headerEnd = "\r\n\r\n";
//	header.AddLen(65);

    char clBuf[24];
    size_t clBufLen = hl_write_string(_outBuf.Len(), clBuf);

    const struct iovec iov[4] = {
            {(void *) headerStart,  strlen(headerStart)},
            {(void *) clBuf,        clBufLen},
            {(void *) headerEnd,    strlen(headerEnd)},
            {(void *) _outBuf.Data, _outBuf.Len()}
    };

    auto n = writev(_fd, iov, 4);
    if (n == -1) {
        perror("writev");
    }
}

void hlcup::Connection::WriteBadRequest() {
    auto &resp = _method == POST ? BAD_REQUEST_CLOSE : BAD_REQUEST;
    auto cnt = write(_fd, resp.c_str(), resp.length());
    if (cnt != resp.length()) {
        perror("Not full write");
    }
}

void hlcup::Connection::WriteNotFound() {
    auto &resp = _method == POST ? NOT_FOUND_CLOSE : NOT_FOUND;
    auto cnt = write(_fd, resp.c_str(), resp.length());
    if (cnt != resp.length()) {
        perror("Not full write");
    }
}

void hlcup::Connection::WriteEmptyObject() {
    auto &resp = _method == POST ? EMPTY_OBJECT_CLOSE : EMPTY_OBJECT;
    auto cnt = write(_fd, resp.c_str(), resp.length());
    if (cnt != resp.length()) {
        perror("Not full write");
    }
}

void hlcup::Connection::WriteCreated() {
    auto &resp = _method == POST ? CREATED_CLOSE : CREATED;
    auto cnt = write(_fd, resp.c_str(), resp.length());
    if (cnt != resp.length()) {
        perror("Not full write");
    }
}

void hlcup::Connection::WriteUpdated() {
    auto &resp = _method == POST ? UPDATED_CLOSE : UPDATED;
    auto cnt = write(_fd, resp.c_str(), resp.length());
    if (cnt != resp.length()) {
        perror("Not full write");
    }
}

void hlcup::Connection::doRequest() {
#ifdef MEASURE_TIME
    auto start = std::chrono::high_resolution_clock::now();
    auto path = strdup(_path);
#endif

    if (_inBuf.Capacity < _content_length)
        std::cerr << "Content length " << _content_length << " > " << _inBuf.Capacity << std::endl;

    switch (_method) {
        case GET:
            if (strncmp(_path, "/accounts/filter/?", 18) == 0) {
                handlerGetAccountsFilter();
            } else if (strncmp(_path, "/accounts/group/?", 17) == 0) {
                handlerGetAccountsGroup();
            } else if (strncmp(_path, "/accounts/", 10) == 0) {
                char *end;
                auto id = static_cast<uint32_t>(strtol(_path + 10, &end, 10));
                if (end[0] != '/' || id > _db->_maxId || !_db->_accounts[id]._notNull) {
                    WriteNotFound();
                    return;
                }
                if (strncmp(end, "/recommend/?", 12) == 0) {
                    handlerGetAccountRecommend(id, end + 12);
                } else if (strncmp(end, "/suggest/?", 10) == 0) {
                    handlerGetAccountSuggest(id, end + 10);
                } else if (strncmp(end, "/dump", 5) == 0) {
                    handlerGetAccountDump(id);
                } else {
                    WriteNotFound();
                    return;
                }

#ifdef PPROF
            } else if (strncmp(_path, "/done", 5) == 0) {
                printf("Stop profiling");
                ProfilerStop();
#endif
            } else {
                WriteNotFound();
            }
            break;
        case POST:
            if (strncmp(_path, "/accounts/new/?", 15) == 0) {
                handlerPostAccountsNew();
            } else if (strncmp(_path, "/accounts/likes/?", 17) == 0) {
                handlerPostAccountsLikes();
            } else if (strncmp(_path, "/accounts/", 10) == 0) {
                char *end;
                auto id = static_cast<uint32_t>(strtol(_path + 10, &end, 10));
                if (end[0] != '/' || id > _db->_maxId || !_db->_accounts[id]._notNull) {
                    WriteNotFound();
                    return;
                }
                handlerPostAccounts(id);
            } else {
                WriteNotFound();
            }
            break;
        default:
            WriteBadRequest();
    }

#ifdef MEASURE_TIME
    auto finish = std::chrono::high_resolution_clock::now();
    double dur = std::chrono::duration_cast<std::chrono::microseconds>(finish - start).count();
//    if (dur > 1500000) {
    const char *methodName = _method == GET ? "GET" : "POST";
    printf("%f\t%s %s\n", dur / 1000000, methodName, path);
//    }
    free(path);
#endif
}

void hlcup::Connection::handlerGetAccountsFilter() {
    uint16_t limit = 0;
    uint16_t fields = 0;

    char *emailGt = nullptr, *emailLt = nullptr, *snameStarts = nullptr;
    size_t snameStartsLen = 0;
    int64_t birthGt = INT64_MIN, birthLt = INT64_MAX;

    char *query = _path + 18;

    intersectIteratorBuilder itBuilder;

    while (query[0]) {
        if (strncmp(query, "query_id=", 9) == 0) {
            query = strchrnul(query, '&');
            if (query[0]) query++;

        } else IFNAME(limit)
            char *end;
            limit = static_cast<uint16_t>(strtol(value, &end, 10));
            if (!limit || *end || value[0] == '-') {
                WriteBadRequest();
                return;
            }

        } else IFNAME(sex_eq)
            fields |= FIELD_SEX;
            itBuilder.add(
                    new idsSetIterator(_db->_indBySex[value[0] == 'm'].begin(), _db->_indBySex[value[0] == 'm'].end()));

        } else IFNAME(email_domain)
            auto it = _db->_indByDomain.find(value);
            if (it != _db->_indByDomain.end())
                itBuilder.add(new idsSetIterator(it->second.begin(), it->second.end()));
            else
                itBuilder.add(new idsSetIterator(emptySet.begin(), emptySet.end()));

        } else IFNAME(email_lt)
            emailLt = value;

        } else IFNAME(email_gt)
            emailGt = value;

        } else IFNAME(status_eq)
            fields |= FIELD_STATUS;
            auto v = DB::_dictStatus.GetByValue(value);
            if (v != DICT_NONE)
                itBuilder.add(new idsSetIterator(_db->_indByStatus[v].begin(), _db->_indByStatus[v].end()));
            else
                itBuilder.add(new idsSetIterator(emptySet.begin(), emptySet.end()));

        } else IFNAME(status_neq)
            fields |= FIELD_STATUS;
            unionIteratorBuilder vItBuilder;
            auto v = DB::_dictStatus.GetByValue(value);
            for (auto i = 0; i < DB::_dictStatus.Size(); ++i) {
                if (i == v)
                    continue;
                vItBuilder.add(new idsSetIterator(_db->_indByStatus[i].begin(), _db->_indByStatus[i].end()));
            }
            itBuilder.add(vItBuilder.build());

        } else IFNAME(fname_eq)
            fields |= FIELD_FNAME;
            auto v = DB::_dictFName.GetByValue(value);
            if (v != DICT_NONE)
                itBuilder.add(new idsSetIterator(_db->_indByFName[v].begin(), _db->_indByFName[v].end()));
            else
                itBuilder.add(new idsSetIterator(emptySet.begin(), emptySet.end()));

        } else IFNAME(fname_any)
            fields |= FIELD_FNAME;
            unionIteratorBuilder vItBuilder;
            while (value[0]) {
                auto next = strchrnul(value, ',');
                if (next[0] == ',') {
                    next[0] = 0;
                    next++;
                }
//                std::cout << "\t" << value << std::endl;

                auto v = DB::_dictFName.GetByValue(value);
                if (v != DICT_NONE)
                    vItBuilder.add(new idsSetIterator(_db->_indByFName[v].begin(), _db->_indByFName[v].end()));
                value = next;
            }
            itBuilder.add(vItBuilder.build());

        } else IFNAME(fname_null)
            if (value[0] == '1') {
                auto &ids = _db->_indByFName[0];
                itBuilder.add(new idsSetIterator(ids.begin(), ids.end()));
            } else {
                fields |= FIELD_FNAME;
                itBuilder.add(new idsSetIterator(_db->_indByNotNullFName.begin(), _db->_indByNotNullFName.end()));
            }

        } else IFNAME(sname_eq)
            fields |= FIELD_SNAME;
            auto v = DB::_dictSName.GetByValue(value);
            if (v != DICT_NONE)
                itBuilder.add(new idsSetIterator(_db->_indBySName[v].begin(), _db->_indBySName[v].end()));
            else
                itBuilder.add(new idsSetIterator(emptySet.begin(), emptySet.end()));

        } else IFNAME(sname_starts)
            fields |= FIELD_SNAME;
            snameStarts = value;
            snameStartsLen = strlen(value);

        } else IFNAME(sname_null)
            if (value[0] == '1') {
                auto &ids = _db->_indBySName[0];
                itBuilder.add(new idsSetIterator(ids.begin(), ids.end()));
            } else {
                fields |= FIELD_SNAME;
                itBuilder.add(new idsSetIterator(_db->_indByNotNullSName.begin(), _db->_indByNotNullSName.end()));
            }

        } else IFNAME(phone_code)
            fields |= FIELD_PHONE;
            auto it = _db->_indByPhone.find(static_cast<const uint16_t>(strtol(value, nullptr, 10)));
            if (it != _db->_indByPhone.end())
                itBuilder.add(new idsSetIterator(it->second.begin(), it->second.end()));
            else
                itBuilder.add(new idsSetIterator(emptySet.begin(), emptySet.end()));

        } else IFNAME(phone_null)
            if (value[0] == '1') {
                auto &ids = _db->_indByPhone[0];
                itBuilder.add(new idsSetIterator(ids.begin(), ids.end()));
            } else {
                fields |= FIELD_PHONE;
                itBuilder.add(new idsSetIterator(_db->_indByNotNullPhone.begin(), _db->_indByNotNullPhone.end()));
            }

        } else IFNAME(country_eq)
            fields |= FIELD_COUNTRY;
            auto v = DB::_dictCountry.GetByValue(value);
            if (v != DICT_NONE)
                itBuilder.add(new idsSetIterator(_db->_indByCountry[v].begin(), _db->_indByCountry[v].end()));
            else
                itBuilder.add(new idsSetIterator(emptySet.begin(), emptySet.end()));

        } else IFNAME(country_null)
            if (value[0] == '1') {
                auto &ids = _db->_indByCountry[0];
                itBuilder.add(new idsSetIterator(ids.begin(), ids.end()));
            } else {
                fields |= FIELD_COUNTRY;
                itBuilder.add(new idsSetIterator(_db->_indByNotNullCountry.begin(), _db->_indByNotNullCountry.end()));
            }

        } else IFNAME(city_eq)
            fields |= FIELD_CITY;
            auto v = DB::_dictCity.GetByValue(value);
            if (v != DICT_NONE)
                itBuilder.add(new idsSetIterator(_db->_indByCity[v].begin(), _db->_indByCity[v].end()));
            else
                itBuilder.add(new idsSetIterator(emptySet.begin(), emptySet.end()));

        } else IFNAME(city_any)
            fields |= FIELD_CITY;
            unionIteratorBuilder vItBuilder;
            while (value[0]) {
                auto next = strchrnul(value, ',');
                if (next[0] == ',') {
                    next[0] = 0;
                    next++;
                }
//                std::cout << "\t" << value << std::endl;
                auto v = DB::_dictCity.GetByValue(value);
                if (v != DICT_NONE)
                    vItBuilder.add(new idsSetIterator(_db->_indByCity[v].begin(), _db->_indByCity[v].end()));
                value = next;
            }
            itBuilder.add(vItBuilder.build());

        } else IFNAME(city_null)
            if (value[0] == '1') {
                auto &ids = _db->_indByCity[0];
                itBuilder.add(new idsSetIterator(ids.begin(), ids.end()));
            } else {
                fields |= FIELD_CITY;
                itBuilder.add(new idsSetIterator(_db->_indByNotNullCity.begin(), _db->_indByNotNullCity.end()));
            }

        } else IFNAME(birth_lt)
            fields |= FIELD_BIRTH;
            birthLt = strtol(value, nullptr, 10);

        } else IFNAME(birth_gt)
            fields |= FIELD_BIRTH;
            birthGt = strtol(value, nullptr, 10);

        } else IFNAME(birth_year)
            fields |= FIELD_BIRTH;
            auto it = _db->_indByBirthYear.find(static_cast<const uint16_t>(strtol(value, nullptr, 10)));
            if (it != _db->_indByBirthYear.end())
                itBuilder.add(new idsSetIterator(it->second.begin(), it->second.end()));
            else
                itBuilder.add(new idsSetIterator(emptySet.begin(), emptySet.end()));

        } else IFNAME(interests_contains)
            fields |= FIELD_INTERESTS;
            intersectIteratorBuilder vItBuilder;
            while (value[0]) {
                auto next = strchrnul(value, ',');
                if (next[0] == ',') {
                    next[0] = 0;
                    next++;
                }
//                std::cout << "\t" << value << std::endl;
                auto v = DB::_dictInterests.GetByValue(value);
                if (v != DICT_NONE)
                    vItBuilder.add(new idsSetIterator(_db->_indByInterests[v].begin(), _db->_indByInterests[v].end()));
                else
                    vItBuilder.add(new idsSetIterator(emptySet.begin(), emptySet.end()));
                value = next;
            }
            itBuilder.add(vItBuilder.build());

        } else IFNAME(interests_any)
            fields |= FIELD_INTERESTS;
            unionIteratorBuilder vItBuilder;
            while (value[0]) {
                auto next = strchrnul(value, ',');
                if (next[0] == ',') {
                    next[0] = 0;
                    next++;
                }
//                std::coutcout << "\t" << value << std::endl;
                auto v = DB::_dictInterests.GetByValue(value);
                if (v != DICT_NONE)
                    vItBuilder.add(new idsSetIterator(_db->_indByInterests[v].begin(), _db->_indByInterests[v].end()));
                value = next;
            }
            itBuilder.add(vItBuilder.build());

        } else IFNAME(likes_contains)
            fields |= FIELD_LIKES;
            intersectIteratorBuilder vItBuilder;
            while (value[0]) {
                auto next = strchrnul(value, ',');
                if (next[0] == ',') {
                    next[0] = 0;
                    next++;
                }
//                std::cout << "\t" << value << std::endl;
                auto id = strtol(value, nullptr, 10);
                if (id < ACCOUNTS_CNT || _db->_accounts[id]._notNull)
                    vItBuilder.add(
                            new idsSetIterator(_db->_accounts[id]._likedMe.begin(), _db->_accounts[id]._likedMe.end()));
                else
                    vItBuilder.add(new idsSetIterator(emptySet.begin(), emptySet.end()));
                value = next;
            }
            itBuilder.add(vItBuilder.build());

        } else IFNAME(premium_now)
            fields |= FIELD_PREMIUM;
            itBuilder.add(new idsSetIterator(_db->_indByNowPremium.begin(), _db->_indByNowPremium.end()));

        } else IFNAME(premium_null)
            if (value[0] == '1') {
                itBuilder.add(
                        new idsSetIterator(_db->_indByHadPremium[false].begin(), _db->_indByHadPremium[false].end()));
            } else {
                fields |= FIELD_PREMIUM;
                itBuilder.add(
                        new idsSetIterator(_db->_indByHadPremium[true].begin(), _db->_indByHadPremium[true].end()));
            }

        } else {
            WriteBadRequest();
            return;
        }
    }

    auto it = itBuilder.build();
    if (!it)
        it = new sequenceIterator(_db->_maxId);

//    it->dump("");

    _outBuf.Append("{\"accounts\":[");
    auto first = true;
    for (auto id = it->cur(); id != dbIterator::end; id = it->next()) {
        if (!(_db->_accounts[id]._notNull && _db->_accounts[id]._birth < birthLt &&
              _db->_accounts[id]._birth > birthGt))
            continue;
        if (emailGt != nullptr && strcmp(_db->_accounts[id]._email, emailGt) <= 0)
            continue;
        if (emailLt != nullptr && strcmp(_db->_accounts[id]._email, emailLt) >= 0)
            continue;
        if (snameStarts != nullptr) {
            if (_db->_accounts[id]._sname == 0)
                continue;
            if (strncmp(DB::_dictSName.Get(_db->_accounts[id]._sname), snameStarts, snameStartsLen) != 0)
                continue;
        }

        if (first) {
            _outBuf.Append("{");
            first = false;
        } else {
            _outBuf.Append(",{");
        }

        _outBuf.Append("\"id\":");
        _outBuf.AddLen(hl_write_string(uint64_t(id), _outBuf.End));

        _outBuf.Append(R"(,"email":")");
        _outBuf.Append(_db->_accounts[id]._email);
        _outBuf.Append("\"");

        if (fields & FIELD_FNAME && _db->_accounts[id]._fname) {
            _outBuf.Append(R"(,"fname":")");
            _outBuf.Append(DB::_dictFName.Get(_db->_accounts[id]._fname));
            _outBuf.Append("\"");
        }

        if (fields & FIELD_SNAME && _db->_accounts[id]._sname) {
            _outBuf.Append(R"(,"sname":")");
            _outBuf.Append(DB::_dictSName.Get(_db->_accounts[id]._sname));
            _outBuf.Append("\"");
        }

        if (fields & FIELD_PHONE && _db->_accounts[id]._phone) {
            _outBuf.Append(R"(,"phone":")");
            _outBuf.Append(_db->_accounts[id]._phone);
            _outBuf.Append("\"");
        }

        if (fields & FIELD_BIRTH) {
            _outBuf.Append(",\"birth\":");
            _outBuf.AddLen(hl_write_string(static_cast<int64_t>(_db->_accounts[id]._birth), _outBuf.End));
            _outBuf.Append("");
        }

        if (fields & FIELD_COUNTRY && _db->_accounts[id]._country) {
            _outBuf.Append(R"(,"country":")");
            _outBuf.Append(DB::_dictCountry.Get(_db->_accounts[id]._country));
            _outBuf.Append("\"");
        }

        if (fields & FIELD_CITY && _db->_accounts[id]._city) {
            _outBuf.Append(R"(,"city":")");
            _outBuf.Append(DB::_dictCity.Get(_db->_accounts[id]._city));
            _outBuf.Append("\"");
        }

        if (fields & FIELD_SEX) {
            _outBuf.Append(R"(,"sex":")");
            _outBuf.Append(_db->_accounts[id]._man ? "m" : "f");
            _outBuf.Append("\"");
        }

        if (fields & FIELD_STATUS) {
            _outBuf.Append(R"(,"status":")");
            _outBuf.Append(DB::_dictStatus.Get(_db->_accounts[id]._status));
            _outBuf.Append("\"");
        }

//        if (fields & FIELD_INTERESTS) {
//            _outBuf.Append(R"(,"interests":[)");
//            bool firstInterest = true;
//            for (auto &interest : _db->_accounts[id]._interests) {
//                if (firstInterest) {
//                    firstInterest = false;
//                } else {
//                    _outBuf.Append(",");
//                }
//                _outBuf.Append("\"");
//                _outBuf.Append(_db->_dictInterests.Get(interest));
//                _outBuf.Append("\"");
//            }
//            _outBuf.Append("]");
//        }

        if (fields & FIELD_PREMIUM && _db->_accounts[id]._premium.start) {
            _outBuf.Append(R"(,"premium":{"start":)");
            _outBuf.AddLen(hl_write_string(static_cast<int64_t>(_db->_accounts[id]._premium.start), _outBuf.End));
            _outBuf.Append(R"(,"finish":)");
            _outBuf.AddLen(hl_write_string(static_cast<int64_t>(_db->_accounts[id]._premium.finish), _outBuf.End));
            _outBuf.Append("}");
        }

//        if (fields & FIELD_LIKES) {
//            _outBuf.Append(R"(,"likes":[)");
//            bool firstInterest = true;
//            for (auto &like : _db->_accounts[id]._likes) {
//                if (firstInterest) {
//                    firstInterest = false;
//                } else {
//                    _outBuf.Append(",");
//                }
//                _outBuf.Append("{\"id\":");
//                _outBuf.AddLen(hl_write_string((uint64_t) like.first, _outBuf.End));
//                _outBuf.Append(",\"ts\":");
//                _outBuf.AddLen(hl_write_string(like.second, _outBuf.End));
//                _outBuf.Append("}");
//            }
//            _outBuf.Append("]");
//        }

        _outBuf.Append("}");

        if (--limit <= 0) {
            break;
        }
    }
    _outBuf.Append("]}");

    delete it;

    WriteResponse();
}

void hlcup::Connection::handlerGetAccountsGroup() {
    auto query = _path + 17;

    uint16_t limit = 5;
    std::vector<uint16_t> fields;
    bool desc = false;

    intersectIteratorBuilder itBuilder;
    std::vector<std::pair<uint16_t, uint16_t>> filter;
    uint16_t joined = 0;
    bool unknownFilter = false;

    while (query[0]) {
        if (strncmp(query, "query_id=", 9) == 0) {
            query = strchrnul(query, '&');
            if (query[0]) query++;

        } else IFNAME(limit)
            char *end;
            limit = static_cast<uint16_t>(strtol(value, &end, 10));
            if (!limit || *end || value[0] == '-') {
                WriteBadRequest();
                return;
            }

        } else IFNAME(order)
            desc = value[0] == '-';

        } else IFNAME(keys)
            while (value[0]) {
                auto next = strchrnul(value, ',');
                if (next[0] == ',') {
                    next[0] = 0;
                    next++;
                }
//                std::cout << "\t" << value << std::endl;
                if (strcmp(value, "sex") == 0)
                    fields.emplace_back(FIELD_SEX);
                else if (strcmp(value, "status") == 0)
                    fields.push_back(FIELD_STATUS);
                else if (strcmp(value, "interests") == 0)
                    fields.push_back(FIELD_INTERESTS);
                else if (strcmp(value, "country") == 0)
                    fields.push_back(FIELD_COUNTRY);
                else if (strcmp(value, "city") == 0)
                    fields.push_back(FIELD_CITY);
                else {
                    WriteBadRequest();
                    return;
                }

                value = next;
            }

        } else IFNAME(sex)
            if (!value[0]) {
                WriteBadRequest();
                return;
            }
            filter.emplace_back(FIELD_SEX, value[0] == 'm');
            if (value[0] == 'm')
                itBuilder.add(new idsSetIterator(_db->_indBySex[true].begin(), _db->_indBySex[true].end()));
            else if (value[0] == 'f')
                itBuilder.add(new idsSetIterator(_db->_indBySex[false].begin(), _db->_indBySex[false].end()));
            else {
                itBuilder.add(new idsSetIterator(emptySet.begin(), emptySet.end()));
            }

//        } else IFNAME(fname)
//            auto v = DB::_dictFName.GetByValue(value);
//            if (v != DICT_NONE)
//                itBuilder.add(new idsSetIterator(_db->_indByFName[v].begin(), _db->_indByFName[v].end()));
//            else {
//                itBuilder.add(new idsSetIterator(emptySet.begin(), emptySet.end()));
//            }
//
//        } else IFNAME(sname)
//            auto v = DB::_dictSName.GetByValue(value);
//            if (v != DICT_NONE)
//                itBuilder.add(new idsSetIterator(_db->_indBySName[v].begin(), _db->_indBySName[v].end()));
//            else {
//                itBuilder.add(new idsSetIterator(emptySet.begin(), emptySet.end()));
//            }

        } else IFNAME(status)
            if (!value[0]) {
                WriteBadRequest();
                return;
            }
            filter.emplace_back(FIELD_STATUS, DB::_dictStatus.GetByValue(value));
            auto v = DB::_dictStatus.GetByValue(value);
            if (v != DICT_NONE)
                itBuilder.add(new idsSetIterator(_db->_indByStatus[v].begin(), _db->_indByStatus[v].end()));
            else {
                itBuilder.add(new idsSetIterator(emptySet.begin(), emptySet.end()));
            }

        } else IFNAME(birth)
            if (!value[0]) {
                WriteBadRequest();
                return;
            }
//            unknownFilter = true;
            char *end;
            auto birth = static_cast<uint16_t>(strtol(value, &end, 10));
            filter.emplace_back(FIELD_BIRTH, birth);
            auto it = _db->_indByBirthYear.find(birth);
            if (*end == 0 && it != _db->_indByBirthYear.end())
                itBuilder.add(new idsSetIterator(it->second.begin(), it->second.end()));
            else {
                itBuilder.add(new idsSetIterator(emptySet.begin(), emptySet.end()));
            }

        } else IFNAME(joined)
            if (!value[0]) {
                WriteBadRequest();
                return;
            }
//            unknownFilter = true;
            char *end;
            joined = static_cast<uint16_t>(strtol(value, &end, 10));
            filter.emplace_back(FIELD_JOINED, joined);
            auto it = _db->_indByJoinedYear.find(joined);
            if (*end == 0 && it != _db->_indByJoinedYear.end())
                itBuilder.add(new idsSetIterator(it->second.begin(), it->second.end()));
            else {
                itBuilder.add(new idsSetIterator(emptySet.begin(), emptySet.end()));
            }

        } else IFNAME(country)
            if (!value[0]) {
                WriteBadRequest();
                return;
            }
            filter.emplace_back(FIELD_COUNTRY, DB::_dictCountry.GetByValue(value));
            auto v = DB::_dictCountry.GetByValue(value);
            if (v != DICT_NONE)
                itBuilder.add(new idsSetIterator(_db->_indByCountry[v].begin(), _db->_indByCountry[v].end()));
            else {
                itBuilder.add(new idsSetIterator(emptySet.begin(), emptySet.end()));
            }

        } else IFNAME(city)
            if (!value[0]) {
                WriteBadRequest();
                return;
            }
            filter.emplace_back(FIELD_CITY, DB::_dictCity.GetByValue(value));
            auto v = DB::_dictCity.GetByValue(value);
            if (v != DICT_NONE)
                itBuilder.add(new idsSetIterator(_db->_indByCity[v].begin(), _db->_indByCity[v].end()));
            else {
                itBuilder.add(new idsSetIterator(emptySet.begin(), emptySet.end()));
            }

        } else IFNAME(interests)
            if (!value[0]) {
                WriteBadRequest();
                return;
            }
            filter.emplace_back(FIELD_INTERESTS, DB::_dictInterests.GetByValue(value));
            auto v = DB::_dictInterests.GetByValue(value);
            if (v != DICT_NONE)
                itBuilder.add(new idsSetIterator(_db->_indByInterests[v].begin(), _db->_indByInterests[v].end()));
            else {
                itBuilder.add(new idsSetIterator(emptySet.begin(), emptySet.end()));
            }

        } else IFNAME(likes)
            if (!value[0]) {
                WriteBadRequest();
                return;
            }
            unknownFilter = true;
            char *end;
            auto id = strtol(value, &end, 10);
            if (*end == 0 && id < ACCOUNTS_CNT && _db->_accounts[id]._notNull)
                itBuilder.add(
                        new idsSetIterator(_db->_accounts[id]._likedMe.begin(), _db->_accounts[id]._likedMe.end()));
            else {
                itBuilder.add(new idsSetIterator(emptySet.begin(), emptySet.end()));
            }

        } else {
            WriteBadRequest();
            return;
        }
    }

    std::vector<std::pair<uint32_t, std::string>> res;
    res.reserve(1024);

    bool fcombExists = false;
    if (filter.size() == 2) {
        uint16_t fcomb = 0;
        for (const auto &f: filter)
            fcomb |= f.first;
        switch (fcomb) {
            case FIELD_STATUS | FIELD_JOINED:
            case FIELD_STATUS | FIELD_BIRTH:
            case FIELD_STATUS | FIELD_SEX:
            case FIELD_SEX | FIELD_JOINED:
            case FIELD_SEX | FIELD_BIRTH:
                fcombExists = true;
        }
    }

    if (!unknownFilter && (filter.size() <= 1 || fcombExists)) {
        DB::_groups.FillRes(res, fields, filter);
    } else {
        auto it = itBuilder.build();
        if (!it) {
            it = new sequenceIterator(_db->_maxId);
        }

        std::unordered_map<std::string, uint32_t> groups;
        for (auto id = it->cur(); id != dbIterator::end; id = it->next()) {
//            std::cerr << id << std::endl;

            std::vector<std::string> keys;
            keys.emplace_back("");

            bool first = true;
            for (const auto &field: fields) {
                if (first) {
                    first = false;
                } else {
                    for (auto &key: keys)
                        key.append("|");
                }

                switch (field) {
                    case FIELD_SEX:
                        for (auto &key: keys)
                            key.append(_db->_accounts[id]._man ? "m" : "f");
                        break;

                    case FIELD_STATUS:
                        for (auto &key: keys)
                            key.append(DB::_dictStatus.Get(_db->_accounts[id]._status));
                        break;

                    case FIELD_COUNTRY:
                        if (_db->_accounts[id]._country)
                            for (auto &key: keys)
                                key.append(DB::_dictCountry.Get(_db->_accounts[id]._country));
                        break;

                    case FIELD_CITY:
                        if (_db->_accounts[id]._city)
                            for (auto &key: keys)
                                key.append(DB::_dictCity.Get(_db->_accounts[id]._city));
                        break;

                    case FIELD_INTERESTS:
                        std::vector<std::string> newKeys;
                        for (const auto &interest: _db->_accounts[id]._interests)
                            for (auto &key: keys)
                                newKeys.push_back(key + DB::_dictInterests.Get(interest));
                        keys = newKeys;
                        break;
                }
            }

            for (const auto &key: keys)
                groups[key]++;
        }

        delete it;

        res.reserve(groups.size());
        for (const auto &p: groups) {
            res.emplace_back(p.second, p.first);
        }
    }

    if (desc)
        std::sort(res.begin(), res.end(), std::greater<>());
    else
        std::sort(res.begin(), res.end());

    _outBuf.Append("{\"groups\":[");
    auto first = true;
    for (const auto &p : res) {
        if (first) {
            _outBuf.Append("{");
            first = false;
        } else {
            _outBuf.Append(",{");
        }

        size_t pos = 0, end = 0;
        bool firstEl = true;
        for (const auto &field: fields) {
            end = p.second.find('|', pos);
            if (end == std::string::npos) {
                end = p.second.size();
            }
            if (end - pos == 0) {
                ++pos;
                continue;
            }

            if (firstEl) {
                firstEl = false;
            } else {
                _outBuf.Append(",");
            }

            switch (field) {
                case FIELD_SEX:
                    _outBuf.Append(R"("sex":)");
                    break;

                case FIELD_STATUS:
                    _outBuf.Append(R"("status":)");
                    break;

                case FIELD_COUNTRY:
                    _outBuf.Append(R"("country":)");
                    break;

                case FIELD_CITY:
                    _outBuf.Append(R"("city":)");
                    break;

                case FIELD_INTERESTS:
                    _outBuf.Append(R"("interests":)");
                    break;
            }

            if (end - pos == 0) {
                _outBuf.Append("null");
            } else {
                _outBuf.Append("\"");
                strncat(_outBuf.End, p.second.c_str() + pos, end - pos);
                _outBuf.AddLen(end - pos);
                _outBuf.Append("\"");
            }
            pos = end + 1;
        }

        if (!firstEl)
            _outBuf.Append(",");
        _outBuf.Append("\"count\":");
        _outBuf.AddLen(hl_write_string((uint64_t) p.first, _outBuf.End));

        _outBuf.Append("}");

        if (--limit == 0)
            break;
    }

    _outBuf.Append("]}");

    WriteResponse();
}

void hlcup::Connection::handlerGetAccountRecommend(uint32_t accountId, char *query) {
    uint16_t limit = 0;

    intersectIteratorBuilder itBuilder;

    while (query[0]) {
        if (strncmp(query, "query_id=", 9) == 0) {
            query = strchrnul(query, '&');
            if (query[0]) query++;

        } else IFNAME(limit)
            char *end;
            limit = static_cast<uint16_t>(strtol(value, &end, 10));
            if (!limit || *end || value[0] == '-') {
                WriteBadRequest();
                return;
            }

        } else IFNAME(country)
            if (!*value) {
                WriteBadRequest();
                return;
            }
            auto v = DB::_dictCountry.GetByValue(value);
            if (v != DICT_NONE)
                itBuilder.add(new idsSetIterator(_db->_indByCountry[v].begin(), _db->_indByCountry[v].end()));
            else
                itBuilder.add(new idsSetIterator(emptySet.begin(), emptySet.end()));

        } else IFNAME(city)
            if (!*value) {
                WriteBadRequest();
                return;
            }
            auto v = DB::_dictCity.GetByValue(value);
            if (v != DICT_NONE)
                itBuilder.add(new idsSetIterator(_db->_indByCity[v].begin(), _db->_indByCity[v].end()));
            else
                itBuilder.add(new idsSetIterator(emptySet.begin(), emptySet.end()));

        } else {
            WriteBadRequest();
            return;
        }
    }

    struct {
        uint64_t score;
        uint32_t id;
    } scores[20];
    int8_t scoresLen = 0;

    auto filterIt = itBuilder.build();

    for (int premium = 1; premium >= 0; --premium) {
        for (int status = 0; status < 3; ++status) {
            if (scoresLen >= limit)
                break;

            unionIteratorBuilder interestsItBuilder;
            for (const auto &interest: _db->_accounts[accountId]._interests) {
                const auto &ids = _db->_indInterestPremiumStatus[interest][premium][status];
                if (!ids.empty())
                    interestsItBuilder.add(new idsSetIterator(ids.begin(), ids.end()));
            }

            intersectIteratorBuilder itBuilderLocal;
            if (auto tmpIt = interestsItBuilder.build())
                itBuilderLocal.add(tmpIt);
            else {
                _outBuf.Append("{\"accounts\":[]}");
                WriteResponse();
                delete filterIt;
                return;
            }
            if (filterIt != nullptr) {
                itBuilderLocal.add(filterIt->clone());
            }

            auto it = itBuilderLocal.build();

            for (auto id = it->cur(); id != dbIterator::end; id = it->next()) {
                if (_db->_accounts[accountId]._man == _db->_accounts[id]._man)
                    continue;

                uint64_t score = 0;

                if (_db->_accounts[id]._premium.start <= _db->_ts && _db->_ts <= _db->_accounts[id]._premium.finish) {
                    score |= (uint64_t(1) << 51);
                }

                switch (_db->_accounts[id]._status) {
                    case 0:
                        score |= (uint64_t(2) << 48);
                        break;
                    case 1:
                        score |= (uint64_t(1) << 48);
                        break;
                    default:
                        break;
                }

                auto commonInterests = static_cast<uint64_t>(
                        _mm_popcnt_u64(_db->_accounts[accountId]._interestsB[0] & _db->_accounts[id]._interestsB[0]) +
                        _mm_popcnt_u64(_db->_accounts[accountId]._interestsB[1] & _db->_accounts[id]._interestsB[1])
                );

                score |= (commonInterests << 32);

                score |= _db->_accounts[accountId]._birth > _db->_accounts[id]._birth
                         ? static_cast<uint32_t >(_db->_accounts[id]._birth - _db->_accounts[accountId]._birth)
                         : static_cast<uint32_t >(_db->_accounts[accountId]._birth - _db->_accounts[id]._birth);

                if (scoresLen == 0) {
                    scores[0] = {score, id};
                    scoresLen = 1;
                } else {
                    if (scoresLen < limit) {
                        scores[scoresLen++] = {score, id};
                    } else {
                        if (scores[limit - 1].score == score ? scores[limit - 1].id > id : scores[limit - 1].score <
                                                                                           score)
                            scores[limit - 1] = {score, id};
                    }

                    for (int i = scoresLen - 2; i >= 0; --i) {
                        if (scores[i].score == score ? scores[i].id > id : scores[i].score < score) {
                            scores[i + 1] = scores[i];
                            scores[i] = {score, id};
                        } else {
                            break;
                        }
                    }
                }
            }

            delete it;
        }
    }

    delete filterIt;

    _outBuf.Append("{\"accounts\":[");
    auto first = true;

    if (limit > scoresLen)
        limit = static_cast<uint16_t>(scoresLen);

    for (auto i = 0; i < limit; ++i) {
        if (first) {
            _outBuf.Append("{");
            first = false;
        } else {
            _outBuf.Append(",{");
        }

        _outBuf.Append("\"id\":");
        _outBuf.AddLen(hl_write_string(uint64_t(scores[i].id), _outBuf.End));

//        _outBuf.Append(",\"score\":");
//        _outBuf.AddLen(hl_write_string(uint64_t(p.score), _outBuf.End));

        _outBuf.Append(R"(,"email":")");
        _outBuf.Append(_db->_accounts[scores[i].id]._email);
        _outBuf.Append("\"");

        if (_db->_accounts[scores[i].id]._fname) {
            _outBuf.Append(R"(,"fname":")");
            _outBuf.Append(DB::_dictFName.Get(_db->_accounts[scores[i].id]._fname));
            _outBuf.Append("\"");
        }

        if (_db->_accounts[scores[i].id]._sname) {
            _outBuf.Append(R"(,"sname":")");
            _outBuf.Append(DB::_dictSName.Get(_db->_accounts[scores[i].id]._sname));
            _outBuf.Append("\"");
        }

        _outBuf.Append(",\"birth\":");
        _outBuf.AddLen(hl_write_string(static_cast<int64_t>(_db->_accounts[scores[i].id]._birth), _outBuf.End));
        _outBuf.Append("");

        _outBuf.Append(R"(,"status":")");
        _outBuf.Append(DB::_dictStatus.Get(_db->_accounts[scores[i].id]._status));
        _outBuf.Append("\"");

        if (_db->_accounts[scores[i].id]._premium.start) {
            _outBuf.Append(R"(,"premium":{"start":)");
            _outBuf.AddLen(
                    hl_write_string(static_cast<int64_t>(_db->_accounts[scores[i].id]._premium.start), _outBuf.End));
            _outBuf.Append(R"(,"finish":)");
            _outBuf.AddLen(
                    hl_write_string(static_cast<int64_t>(_db->_accounts[scores[i].id]._premium.finish), _outBuf.End));
            _outBuf.Append("}");
        }

        _outBuf.Append("}");
    }
    _outBuf.Append("]}");

    WriteResponse();
}

void hlcup::Connection::handlerGetAccountSuggest(uint32_t accountId, char *query) {
    uint16_t limit = 0;

    intersectIteratorBuilder itBuilder;

//    std::cout << _db->_accounts[accountId] << std::endl;

    while (query[0]) {
        if (strncmp(query, "query_id=", 9) == 0) {
            query = strchrnul(query, '&');
            if (query[0]) query++;

        } else IFNAME(limit)
            char *end;
            limit = static_cast<uint16_t>(strtol(value, &end, 10));
            if (!limit || *end || value[0] == '-') {
                WriteBadRequest();
                return;
            }

        } else IFNAME(country)
            if (!*value) {
                WriteBadRequest();
                return;
            }
            auto v = DB::_dictCountry.GetByValue(value);
            if (v != DICT_NONE)
                itBuilder.add(new idsSetIterator(_db->_indByCountry[v].begin(), _db->_indByCountry[v].end()));
            else {
                itBuilder.add(new idsSetIterator(emptySet.begin(), emptySet.end()));
            }

        } else IFNAME(city)
            if (!*value) {
                WriteBadRequest();
                return;
            }
            auto v = DB::_dictCity.GetByValue(value);
            if (v != DICT_NONE)
                itBuilder.add(new idsSetIterator(_db->_indByCity[v].begin(), _db->_indByCity[v].end()));
            else {
                itBuilder.add(new idsSetIterator(emptySet.begin(), emptySet.end()));
            }

        } else {
            WriteBadRequest();
            return;
        }
    }

    unionIteratorBuilder likesItBuilder;
    std::unordered_map<uint32_t, std::pair<uint32_t, uint16_t>> myLikesTS;
    for (const auto &like : _db->_accounts[accountId]._likes) {
        auto &ids = _db->_accounts[like.first]._likedMe;
        likesItBuilder.add(new idsSetIterator(ids.begin(), ids.end()));
        auto &likeTS = myLikesTS[like.first];
        likeTS.first += like.second;
        ++likeTS.second;
    }
    if (auto tmpIt = likesItBuilder.build())
        itBuilder.add(tmpIt);
    else {
        _outBuf.Append("{\"accounts\":[]}");
        WriteResponse();
        return;
    }

//    itBuilder.add(
//            new idsSetIterator(
//                    _db->_indBySex[_db->_accounts[accountId]._man].begin(),
//                    _db->_indBySex[_db->_accounts[accountId]._man].end()
//            )
//    );

    auto it = itBuilder.build();
    if (!it) {
        _outBuf.Append("{\"accounts\":[]}");
        WriteResponse();
        return;
    }

    auto bufSz = limit + 5;

    struct {
        double similarity;
        uint32_t id;
    } suggests[bufSz];
    int suggestsLen = 0;

    for (auto id = it->cur(); id != dbIterator::end; id = it->next()) {
//        std::cerr << id << std::endl;
        if (_db->_accounts[id]._man != _db->_accounts[accountId]._man)
            continue;

        std::unordered_map<uint32_t, std::pair<uint32_t, uint16_t>> userLikesTS;
        for (const auto &like : _db->_accounts[id]._likes) {
            auto &likeTS = userLikesTS[like.first];
            likeTS.first += like.second;
            ++likeTS.second;
        }

        double similarity = 0.0;
        for (const auto &like : userLikesTS) {
            const auto myLike = myLikesTS.find(like.first);
            if (myLike != myLikesTS.end()) {
                auto myLikeTsNum = double(myLike->second.first) / myLike->second.second;
                auto likeTsNum = double(like.second.first) / like.second.second;

                similarity += myLikeTsNum == likeTsNum
                              ? double(1.0)
                              : double(1.0) /
                                (myLikeTsNum - likeTsNum > 0 ? myLikeTsNum - likeTsNum : likeTsNum - myLikeTsNum);
            }
        }

        if (suggestsLen == 0) {
            suggests[0] = {similarity, id};
            suggestsLen = 1;
        } else {
            if (suggestsLen < bufSz) {
                suggests[suggestsLen++] = {similarity, id};
            } else {
                if (suggests[bufSz - 1].similarity == similarity
                    ? suggests[bufSz - 1].id > id
                    : suggests[bufSz - 1].similarity < similarity)
                    suggests[bufSz - 1] = {similarity, id};
            }

            for (int i = suggestsLen - 2; i >= 0; --i) {
                if (suggests[i].similarity == similarity ? suggests[i].id > id : suggests[i].similarity < similarity) {
                    suggests[i + 1] = suggests[i];
                    suggests[i] = {similarity, id};
                } else {
                    break;
                }
            }
        }
    }

    std::vector<ids_set> res;
    res.reserve(limit);
    size_t resCount = 0;
    for (int j = 0; j < suggestsLen; ++j) {
//        std::cerr << p.second << ": " << p.first <<
//                  " " << (_db->_accounts[p.second]._city ? _db->_accounts[p.second]._city : "null") <<
//                  " " << _db->_accounts[p.second]._status
//                  << std::endl;
        ids_set ids;
        for (const auto &like : _db->_accounts[suggests[j].id]._likes) {
            if (myLikesTS.find(like.first) != myLikesTS.end())
                continue;
            ids.insert(like.first);
//            std::cerr << "\t" << like.first << ": " << like.second << std::endl;
        }
        if (ids.empty())
            continue;
        res.emplace_back(ids);
        resCount += ids.size();
        if (resCount >= limit)
            break;
    }

    _outBuf.Append("{\"accounts\":[");
    auto first = true;
    for (auto &ids: res) {
        if (limit == 0) {
            break;
        }
        for (const auto &id: ids) {
            if (first) {
                _outBuf.Append("{");
                first = false;
            } else {
                _outBuf.Append(",{");
            }

            _outBuf.Append("\"id\":");
            _outBuf.AddLen(hl_write_string(uint64_t(id), _outBuf.End));

            _outBuf.Append(R"(,"email":")");
            _outBuf.Append(_db->_accounts[id]._email);
            _outBuf.Append("\"");

            if (_db->_accounts[id]._fname) {
                _outBuf.Append(R"(,"fname":")");
                _outBuf.Append(DB::_dictFName.Get(_db->_accounts[id]._fname));
                _outBuf.Append("\"");
            }

            if (_db->_accounts[id]._sname) {
                _outBuf.Append(R"(,"sname":")");
                _outBuf.Append(DB::_dictSName.Get(_db->_accounts[id]._sname));
                _outBuf.Append("\"");
            }

            _outBuf.Append(R"(,"status":")");
            _outBuf.Append(DB::_dictStatus.Get(_db->_accounts[id]._status));
            _outBuf.Append("\"");

            _outBuf.Append("}");

            if (--limit == 0) {
                break;
            }
        }
    }
    _outBuf.Append("]}");

    delete it;

    WriteResponse();

}

void hlcup::Connection::handlerPostAccountsNew() {
    Account account;
    if (!account.UmnarshalJSON(_requst_body, _inBuf.End - _requst_body)) {
        WriteBadRequest();
        return;
    }

//    std::cerr << account << std::endl;

    if (account._id >= ACCOUNTS_CNT || _db->_accounts[account._id]._notNull) {
        WriteBadRequest();
        return;
    }

    if (account._id == 0) {
        WriteBadRequest();
        return;
    }

    if (account._fields & FIELD_EMAIL && account._email &&
        (!strchr(account._email, '@')
         || _db->_indByEmail.find(account._email) != _db->_indByEmail.end()
        )) {
        WriteBadRequest();
        return;
    }

//    if (account._fields & FIELD_LIKES) {
//        for (const auto &like : account._likes) {
//            if (!_db->_accounts[like.first]._notNull) {
//                WriteBadRequest();
//                return;
//            }
//        }
//    }

    _db->_accounts[account._id] = account;
    if (account._id > _db->_maxId)
        _db->_maxId = account._id;

    _db->_indByEmail.insert(account._email);
    _updateQueue->Add(UpdateQueue::ACTION_NEW, account);

    WriteCreated();
}

void hlcup::Connection::handlerPostAccounts(uint32_t id) {
    Account account;
    if (!account.UmnarshalJSON(_requst_body, _inBuf.End - _requst_body)) {
        WriteBadRequest();
        return;
    }

    if (account._fields & FIELD_EMAIL && strcmp(account._email, _db->_accounts[id]._email) != 0) {
        if (!strchr(account._email, '@') || _db->_indByEmail.find(account._email) != _db->_indByEmail.end()) {
            WriteBadRequest();
            return;
        }

        _db->_indByEmail.erase(_db->_accounts[id]._email);
        _db->_indByEmail.insert(account._email);
    }

    account._id = id;

    _updateQueue->Add(UpdateQueue::ACTION_UPDATE, account);

    WriteUpdated();
}


void hlcup::Connection::handlerPostAccountsLikes() {
    std::vector<UpdateQueue::batch_like> likes;

    auto parsed = [this, &likes](const char *data) -> bool {
        JSON_SKIP_SPACES()
        JSON_START_OBJECT()

        while (true) {
            JSON_SKIP_SPACES()
            JSON_DATA_DEBUG()

            if (data[0] == '}') {
                return true;

            } else if (data[0] == ',') {
                data++;
                JSON_DATA_DEBUG()
                continue;

            } else if (strncmp(data, "\"likes\"", 7) == 0) {
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

                        uint32_t likee = 0, liker = 0;
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

                            } else if (strncmp(data, "\"likee\"", 7) == 0) {
                                data += 7;
                                JSON_DATA_DEBUG()

                                JSON_SKIP_SPACES()
                                JSON_FIELDS_SEPARATOR()

                                JSON_SKIP_SPACES()
                                JSON_UINT(likee)

                            } else if (strncmp(data, "\"liker\"", 7) == 0) {
                                data += 7;
                                JSON_DATA_DEBUG()

                                JSON_SKIP_SPACES()
                                JSON_FIELDS_SEPARATOR()

                                JSON_SKIP_SPACES()
                                JSON_UINT(liker)

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

                        if (likee >= ACCOUNTS_CNT || !_db->_accounts[likee]._notNull) {
                            JSON_ERROR("Invalid likee")
                        }

                        if (liker >= ACCOUNTS_CNT || !_db->_accounts[liker]._notNull) {
                            JSON_ERROR("Invalid likee")
                        }

                        likes.emplace_back(
                                UpdateQueue::batch_like{likee, liker, static_cast<int32_t>(ts - LIKE_EPOCH)});

                    } else {
                        JSON_ERROR("Unknown")
                    }
                }

            } else {
                JSON_ERROR("Unknown field")
            }
        }
        return true;
    }(_requst_body);

    if (!parsed) {
        WriteBadRequest();
        return;
    }

    _updateQueue->AddLikes(likes);

    WriteUpdated();
}

void hlcup::Connection::handlerGetAccountDump(uint32_t id) {

    _outBuf.Append("{");

    _outBuf.Append("\"id\":");
    _outBuf.AddLen(hl_write_string(uint64_t(id), _outBuf.End));

    _outBuf.Append(R"(,"email":")");
    _outBuf.Append(_db->_accounts[id]._email);
    _outBuf.Append("\"");

    _outBuf.Append(R"(,"fname":")");
    _outBuf.Append(DB::_dictFName.Get(_db->_accounts[id]._fname));
    _outBuf.Append("\"");

    _outBuf.Append(R"(,"sname":")");
    _outBuf.Append(DB::_dictSName.Get(_db->_accounts[id]._sname));
    _outBuf.Append("\"");

    _outBuf.Append(R"(,"phone":")");
    _outBuf.Append(_db->_accounts[id]._phone);
    _outBuf.Append("\"");

    _outBuf.Append(",\"birth\":");
    _outBuf.AddLen(hl_write_string(static_cast<int64_t>(_db->_accounts[id]._birth), _outBuf.End));
    _outBuf.Append("");

    _outBuf.Append(R"(,"country":")");
    _outBuf.Append(DB::_dictCountry.Get(_db->_accounts[id]._country));
    _outBuf.Append("\"");

    _outBuf.Append(R"(,"city":")");
    _outBuf.Append(DB::_dictCity.Get(_db->_accounts[id]._city));
    _outBuf.Append("\"");

    _outBuf.Append(R"(,"sex":")");
    _outBuf.Append(_db->_accounts[id]._man ? "m" : "f");
    _outBuf.Append("\"");

    _outBuf.Append(R"(,"status":")");
    _outBuf.Append(DB::_dictStatus.Get(_db->_accounts[id]._status));
    _outBuf.Append("\"");

    _outBuf.Append(R"(,"interests":[)");
    bool firstInterest = true;
    for (auto &interest : _db->_accounts[id]._interests) {
        if (firstInterest) {
            firstInterest = false;
        } else {
            _outBuf.Append(",");
        }
        _outBuf.Append("\"");
        _outBuf.Append(DB::_dictInterests.Get(interest));
        _outBuf.Append("\"");
    }
    _outBuf.Append("]");

    _outBuf.Append(R"(,"premium":{"start":)");
    _outBuf.AddLen(hl_write_string(static_cast<int64_t>(_db->_accounts[id]._premium.start), _outBuf.End));
    _outBuf.Append(R"(,"finish":)");
    _outBuf.AddLen(hl_write_string(static_cast<int64_t>(_db->_accounts[id]._premium.finish), _outBuf.End));
    _outBuf.Append("}");

    _outBuf.Append("}");

    WriteResponse();
}
