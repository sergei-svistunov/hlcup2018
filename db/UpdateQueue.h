#ifndef HLCUP2018_UPDATEQUEUE_H
#define HLCUP2018_UPDATEQUEUE_H

#include <cstdint>
#include <queue>
#include <vector>
#include <condition_variable>
#include "Account.h"
#include "DB.h"

namespace hlcup {

    class UpdateQueue {
    public:
        enum ActionType {
            ACTION_NEW, ACTION_UPDATE, ACTION_ADD_LIKES
        };

        struct batch_like {
            uint32_t likee, liker;
            int32_t ts;
        };

        struct QueueAction {
            ActionType type;
            const Account account;
            const std::vector<batch_like> likes;
        };

        explicit UpdateQueue(DB *_db);

        void Add(ActionType type, const Account &account);
        void AddLikes(const std::vector<batch_like> &likes);

        void Run(uint8_t threadsCnt);

    private:
        DB *_db;
        std::queue<QueueAction> _queue;
        std::condition_variable _cond;
        std::mutex _mutex;
        bool _done = false;
    };

}

#endif //HLCUP2018_UPDATEQUEUE_H
