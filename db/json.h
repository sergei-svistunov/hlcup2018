#ifndef DB_JSON_H_
#define DB_JSON_H_

#define DEBUG_JSON 0

#include <cstdlib>
#include <cstring>

#if DEBUG_JSON > 1
#define JSON_DATA_DEBUG() fprintf(stderr, "--- %s\n", strndup(data, len));
#else
#define JSON_DATA_DEBUG() /**/
#endif

#if DEBUG_JSON > 0
#define JSON_ERROR(t) fprintf(stderr, "%s (%s:%d)\n", #t, __FILE__, __LINE__); return false;
#else
#define JSON_ERROR(t) return false;
#endif

#define JSON_SKIP_SPACES() data += strspn(data, " \t\r\n"); JSON_DATA_DEBUG()

#define JSON_START_OBJECT() if (data[0] != '{') { \
        JSON_ERROR(Need {}) \
    } \
    data++; \
    JSON_DATA_DEBUG();

#define JSON_START_ARRAY() if (data[0] != '[') { \
        JSON_ERROR(Need []) \
    } \
    data++; \
    JSON_DATA_DEBUG();


#define JSON_FIELDS_SEPARATOR() if (data[0] != ':') { \
        JSON_ERROR(Need :) \
    } \
    data++; \
    JSON_DATA_DEBUG();

#define JSON_LONG(field) char *endptr; \
    field = strtol(data, &endptr, 10); \
    if (data == endptr) { \
        JSON_ERROR(Invalid ## field ## value); \
    } \
    data = endptr; \
    if (data[0] == '.') data +=2; \
    JSON_DATA_DEBUG();

#define JSON_UINT(field) char *endptr; \
    field = (uint32_t)strtol(data, &endptr, 10); \
    if (data == endptr) { \
        JSON_ERROR(Invalid ## field ## value); \
    } \
    data = endptr; \
    JSON_DATA_DEBUG();


#define JSON_STRING(field) if (data[0] != '"') {\
        JSON_ERROR(Need dquote); \
    } \
    auto strend = strchr(data+1, '"'); \
    if (strend == NULL) { \
        JSON_ERROR(Need dquote); \
    } \
    field = strndup(data+1, strend - data - 1); \
    data = strend + 1; \
    JSON_DATA_DEBUG();

#define JSON_CHAR(field) if (data[0] != '"') {\
        JSON_ERROR(Need dquote); \
    } \
    if (data[2] != '"') {\
        JSON_ERROR(Need dquote); \
    } \
    field = data[1]; \
    data += 3; \
    JSON_DATA_DEBUG();

#endif /* DB_JSON_H_ */
