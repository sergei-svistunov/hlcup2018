
#ifndef HLCUP2018_IDS_SET_H
#define HLCUP2018_IDS_SET_H

#include <cstdint>
#include <algorithm>
#include <vector>
#include <pthread.h>
#include <functional>

namespace hlcup {
    class ids_set : public std::vector<uint32_t> {
    public:
//        ids_set() {
//            pthread_mutex_init(&_mtx, nullptr);
//        }

        inline void insert(const uint32_t v) {
            if (empty()) {
                push_back(v);
                return;
            }

            resize(size() + 1);

            auto it = std::lower_bound(begin(), end(), v, std::greater<>());
            if (it != end() && *it == v) {
                resize(size() - 1);
                return;
            }

            if (it == end()) {
                back() = v;
                return;
            }

            std::move_backward(it, end() - 1, end());

            *it = v;
        }

//        void insert(const uint32_t v) {
//            pthread_mutex_lock(&_mtx);
//            _insert(v);
//            pthread_mutex_unlock(&_mtx);
//        }

        inline void erase(const uint32_t v) {
//            pthread_mutex_lock(&_mtx);
            auto it = std::lower_bound(begin(), end(), v, std::greater<>());
            if (it != end() && *it == v) {
                std::move_backward(it+1, end(), end() - 1);
                resize(size() - 1);
            }
//            pthread_mutex_unlock(&_mtx);
        }

    private:
//        pthread_mutex_t _mtx{};
    };
}

#endif //HLCUP2018_IDS_SET_H
