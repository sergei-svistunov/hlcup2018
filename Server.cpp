#include <netdb.h>
#include <cstring>
#include <unistd.h>
#include <fcntl.h>
#include <thread>
#include <fstream>
#include <netinet/tcp.h>
#include "Server.h"
#include "Worker.h"

#ifdef PPROF
#include <gperftools/profiler.h>
#endif

hlcup::Server::Server(hlcup::DB *db) : _sfd(0), _db(db) {}

hlcup::Server::~Server() {
    if (_sfd != 0) {
        close(_sfd);
    }
}

void hlcup::Server::Run(const char *port) {
    struct addrinfo hints{};
    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_UNSPEC; /* Return IPv4 and IPv6 choices */
    hints.ai_socktype = SOCK_STREAM; /* We want a TCP socket */
    hints.ai_flags = AI_PASSIVE; /* All interfaces */

    struct addrinfo *result, *rp;
    auto s = getaddrinfo(nullptr, port, &hints, &result);
    if (s != 0) {
        perror("getaddrinfo: ");
        return;
    }

    for (rp = result; rp != nullptr; rp = rp->ai_next) {
        _sfd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (_sfd == -1) {
            continue;
        }

        int enable = 1;
        if (setsockopt(_sfd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int)) == -1)
            perror("setsockopt(SO_REUSEADDR): ");

        s = bind(_sfd, rp->ai_addr, rp->ai_addrlen);
        if (s == 0) {
            /* We managed to bind successfully! */
            break;
        }

        close(_sfd);
    }

    if (rp == nullptr) {
        perror("Could not bind");
        return;
    }

    freeaddrinfo(result);

//    int sndsize = 2 * 1024 * 1024;
//    if (setsockopt(_sfd, SOL_SOCKET, SO_SNDBUF, &sndsize, (int) sizeof(sndsize)) == -1) {
//        perror("SO_SNDBUF");
//    }
//
//    if (setsockopt(_sfd, SOL_SOCKET, SO_RCVBUF, &sndsize, (int) sizeof(sndsize)) == -1) {
//        perror("SO_RCVBUF");
//    }

    int val = true;
    if (setsockopt(_sfd, IPPROTO_TCP, TCP_NODELAY, &val, sizeof(val)) == -1) {
        perror("TCP_NODELAY");
    }
    if (setsockopt(_sfd, IPPROTO_TCP, TCP_DEFER_ACCEPT, &val, sizeof(val)) == -1) {
        perror("TCP_DEFER_ACCEPT");
    }
    if (setsockopt(_sfd, IPPROTO_TCP, TCP_QUICKACK, &val, sizeof(val)) == -1) {
        perror("TCP_QUICKACK");
    }

    auto flags = fcntl(_sfd, F_GETFL, 0);
    if (flags == -1) {
        perror("fcntl(F_GETFL)");
    }

    flags |= O_NONBLOCK;
    if (fcntl(_sfd, F_SETFL, flags) == -1) {
        perror("fcntl(F_SETFL)");
    }

    if (listen(_sfd, SOMAXCONN) == -1) {
        perror("Cannot listen");
        return;
    }

//    int sockbufsize = 0;
//    socklen_t size = sizeof(int);
//    getsockopt(_sfd, SOL_SOCKET, SO_SNDBUF, &sockbufsize, &size);
//    printf("%d\n", sockbufsize);

#ifdef PPROF
    ProfilerStart("/tmp/iprof.out");
#endif

    UpdateQueue updateQueue(_db);
    updateQueue.Run(1);

    std::vector<pthread_t> threads(3);
    for (uint8_t i = 0; i < threads.size(); ++i) {
        auto worker = new Worker(_sfd, i);
        worker->RunInThread(threads[i], _db, &updateQueue);
    }

    for (auto &thread : threads) {
        pthread_join(thread, nullptr);
    }
}
