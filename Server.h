#ifndef HLCUP2018_SERVER_H
#define HLCUP2018_SERVER_H


#include "db/DB.h"

namespace hlcup {
    class Server {
    public:
        explicit Server(DB *db);

        ~Server();

        void Run(const char *port);

    private:
        int _sfd;
        DB *_db;
    };
}


#endif //HLCUP2018_SERVER_H
