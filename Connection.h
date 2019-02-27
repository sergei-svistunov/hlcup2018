#ifndef HLCUP2018_CONNECTION_H
#define HLCUP2018_CONNECTION_H


#include <string>
#include "db/DB.h"
#include "Buffer.h"
#include "db/UpdateQueue.h"

namespace hlcup {

    const std::string BAD_REQUEST("HTTP/1.1 400 BR\n"
                                  "Server: F\n"
                                  "Connection: keep-alive\n"
                                  "Content-Length: 0\n"
                                  "\n");
    const std::string BAD_REQUEST_CLOSE("HTTP/1.1 400 BR\n"
                                  "Server: F\n"
                                  "Connection: close\n"
                                  "Content-Length: 0\n"
                                  "\n");

    const std::string NOT_FOUND("HTTP/1.1 404 NF\n"
                                "Server: F\n"
                                "Connection: keep-alive\n"
                                "Content-Length: 0\n"
                                "\n");
    const std::string NOT_FOUND_CLOSE("HTTP/1.1 404 NF\n"
                                "Server: F\n"
                                "Connection: close\n"
                                "Content-Length: 0\n"
                                "\n");

    const std::string EMPTY_OBJECT("HTTP/1.1 200 OK\n"
                                   "Server: F\n"
                                   "Connection: keep-alive\n"
                                   "Content-Length: 2\n"
                                   "\n"
                                   "{}");
    const std::string EMPTY_OBJECT_CLOSE("HTTP/1.1 200 OK\n"
                                   "Server: F\n"
                                   "Connection: close\n"
                                   "Content-Length: 2\n"
                                   "\n"
                                   "{}");

    const std::string CREATED("HTTP/1.1 201 OK\n"
                              "Server: F\n"
                              "Connection: keep-alive\n"
                              "Content-Length: 2\n"
                              "\n"
                              "{}");
    const std::string CREATED_CLOSE("HTTP/1.1 201 OK\n"
                              "Server: F\n"
                              "Connection: close\n"
                              "Content-Length: 2\n"
                              "\n"
                              "{}");

    const std::string UPDATED("HTTP/1.1 202 OK\n"
                              "Server: F\n"
                              "Connection: keep-alive\n"
                              "Content-Length: 2\n"
                              "\n"
                              "{}");
    const std::string UPDATED_CLOSE("HTTP/1.1 202 OK\n"
                              "Server: F\n"
                              "Connection: close\n"
                              "Content-Length: 2\n"
                              "\n"
                              "{}");

    class Connection {
    public:
        enum Method {
            UNKNOWN, GET, POST
        };

        Connection(int fd, DB *db, UpdateQueue *_updateQueue);

        ~Connection();

        void Reset(int fd);

        bool ProcessEvent();

        void WriteResponse();

        void WriteBadRequest();

        void WriteNotFound();

        void WriteEmptyObject();

        void WriteCreated();

        void WriteUpdated();

        int _fd;
        Buffer _inBuf;
        Method _method;
    private:
        void doRequest();

        void handlerGetAccountsFilter();

        void handlerGetAccountsGroup();

        void handlerGetAccountRecommend(uint32_t accountId, char *query);

        void handlerGetAccountSuggest(uint32_t accountId, char *query);

        void handlerGetAccountDump(uint32_t accountId);

        void handlerPostAccountsNew();

        void handlerPostAccounts(uint32_t id);

        void handlerPostAccountsLikes();

        UpdateQueue *_updateQueue;
        DB *_db;
        Buffer _outBuf;
//        State _state;
        char *_path;
        const char *_requst_body;
        size_t _content_length;
    };

}


#endif //HLCUP2018_CONNECTION_H
