#ifndef HLCUP2018_WORKER_H
#define HLCUP2018_WORKER_H

#include <cstdint>
#include <pthread.h>
#include "db/DB.h"
#include "Connection.h"

namespace hlcup {
#define MAXEVENTS 64

    class ConnectionsPool {
    public:
        explicit ConnectionsPool(DB *db, UpdateQueue *updateQueue) : _db(db), _updateQueue(updateQueue) {
            connQueue.reserve(1000);
            for (uint32_t i = 0; i < connQueue.capacity(); ++i) {
                connQueue.push_back(new Connection(0, db, updateQueue));
            }
        }

        Connection *GetConnection(int fd) {
//            printf("Get connection, size: %ld\n", connQueue.size());
            if (connQueue.empty()) {
                return new Connection(fd, _db, _updateQueue);
            }

            auto conn = connQueue.back();
            connQueue.pop_back();

            conn->Reset(fd);

            return conn;
        }

        void PutConnection(Connection *conn) {
//		printf("Put connection, size: %ld\n", connQueue.size());
            if (connQueue.size() == connQueue.capacity()) {
                delete conn;
                return;
            }

            connQueue.push_back(conn);
        }

    private:
        std::vector<Connection *> connQueue;
        DB *_db;
        UpdateQueue *_updateQueue;
    };

    class Worker {
    public:
        Worker(int _sfd, uint8_t _cpuId);

        virtual ~Worker();

        void RunInThread(pthread_t &thr, DB *db, UpdateQueue *updateQueue);

        void Run();

        uint8_t _cpuId;
    private:
        UpdateQueue *_updateQueue;
        int _sfd;
        DB *_db;
        ConnectionsPool *_connPool;
    };
}


#endif //HLCUP2018_WORKER_H
