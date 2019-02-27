#ifndef HLCUP2018_UTILS_H
#define HLCUP2018_UTILS_H

#include <cstdint>

static int percent_decode(char *out, char *in) {
    static const char tbl[256] = {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                                  -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                                  -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                                  -1, -1, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -1, -1, -1, -1, -1, -1,
                                  -1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                                  -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 10,
                                  11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                                  -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                                  -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                                  -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                                  -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                                  -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                                  -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                                  -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                                  -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                                  -1, -1, -1, -1, -1};
    char c, v1, v2;
    if (in != nullptr) {
        while ((c = *in++) != '\0') {
            switch (c) {
                case '%':
                    if (!(v1 = *in++) || (v1 = tbl[(unsigned char) v1]) < 0
                        || !(v2 = *in++)
                        || (v2 = tbl[(unsigned char) v2]) < 0) {
                        return -1;
                    }
                    c = (v1 << 4) | v2;
                    break;
                case '+':
                    c = ' ';
                    break;
                default:
                    break;
            }
            *out++ = c;
        }
    }
    *out = '\0';
    return 0;
}

static void utf_decode(char *in) {
    if (!in)
        return;

    static const char tbl[256] = {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                                  -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                                  -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                                  -1, -1, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -1, -1, -1, -1, -1, -1,
                                  -1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                                  -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 10,
                                  11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                                  -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                                  -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                                  -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                                  -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                                  -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                                  -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                                  -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                                  -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                                  -1, -1, -1, -1, -1};

    char *out = in;

    while (in[0] != 0) {
        if (in[0] == '\\' && in[1] == 'u') {
            auto u = static_cast<uint16_t>(tbl[in[2]] << 12 | tbl[in[3]] << 8 | tbl[in[4]] << 4
                                           | tbl[in[5]]);
            if (u < 255) {
                out[0] = static_cast<char>(u);
                out++;
            } else {
                uint16_t w;
                if (u >= 0x0410 && u <= 0x043f) {
                    w = static_cast<uint16_t>(u - 0x0410 + 0xd090);
                } else {
                    w = static_cast<uint16_t>(u - 0x0440 + 0xd180);
                }

                out[0] = static_cast<char>(w >> 8);
                out[1] = static_cast<char>(w);

                out += 2;
            }
            in += 6;
        } else {
            out[0] = in[0];
            in++;
            out++;
        }
    }

    out[0] = 0;
}

static size_t hl_write_string(uint64_t v, char *dest) {
    static char const digit[] = "0123456789";

    size_t cnt = 0;
    do {
        cnt++;
        *dest++ = digit[v % 10];
        v /= 10;
    } while (v);
    *dest = 0;
    dest -= cnt;

    size_t l = 0;
    size_t r = cnt - 1;
    while (l < r) {
        char t = dest[r];
        dest[r--] = dest[l];
        dest[l++] = t;
    }

    return cnt;
}

static size_t hl_write_string(int64_t v, char *dest) {
    static char const digit[] = "0123456789";
    bool minus = v < 0;
    if (minus)
        v *= -1;

    size_t cnt = 0;
    do {
        cnt++;
        *dest++ = digit[v % 10];
        v /= 10;
    } while (v);

    if (minus) {
        cnt++;
        *dest++ = '-';
    }
    *dest = 0;
    dest -= cnt;

    size_t l = 0;
    size_t r = cnt - 1;
    while (l < r) {
        char t = dest[r];
        dest[r--] = dest[l];
        dest[l++] = t;
    }

    return cnt;
}

#define IFNAME(name) if (strncmp(query, #name "=", strlen(#name "=")) == 0) { \
    auto value = query += strlen(#name) + 1; \
    query = strchrnul(query, '&'); \
    if (query[0] == '&') { \
        query[0] = 0; \
        query++; \
    } \
    if (value[0]) \
        percent_decode(value, value); //\
//    std::cout << #name ": " << value << std::endl;

#endif //HLCUP2018_UTILS_H
