#ifndef HLCUP2018_BUFFER_H
#define HLCUP2018_BUFFER_H


#include <cstring>

namespace hlcup {

    class Buffer {
    public:
        Buffer() {
            Data = End = nullptr;
            Capacity = 0;
        }

        ~Buffer() {
            delete[] Data;
        }

        void Reserve(size_t capacity) {
            Capacity = capacity;
            Data = End = new char[capacity];
        }

        inline void Append(const char *str) {
            strcpy(End, str);
            AddLen(strlen(str));
        }

        inline void AddLen(size_t len) {
            End += len;
            *End = 0;
        }

        inline void Reset() {
            End = Data;
            *End = 0;
        }

        inline size_t Len() {
            return End - Data;
        }

        char *Data, *End;
        size_t Capacity;
    };

}


#endif //HLCUP2018_BUFFER_H
