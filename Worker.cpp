#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <sys/epoll.h>
#include <netinet/tcp.h>
#include "Worker.h"

hlcup::Worker::Worker(int sfd, uint8_t cpuId) : _cpuId(cpuId), _sfd(sfd), _db(nullptr) {}

hlcup::Worker::~Worker() = default;

void *runInThread(void *arg) {
    auto worker = (hlcup::Worker *) arg;

    /*
     cpu_set_t cpuset;
     CPU_ZERO(&cpuset);
     CPU_SET(worker->cpuId, &cpuset);

     pthread_t current_thread = pthread_self();
     if (pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset)
     != 0) {
     perror("pthread_setaffinity_np");
     }
     */

    worker->Run();

    return nullptr;
}

void hlcup::Worker::RunInThread(pthread_t &thr, DB *db, UpdateQueue *updateQueue) {
    _db = db;
    _updateQueue = updateQueue;
    pthread_attr_t attr;
    pthread_attr_init(&attr);

    pthread_attr_setguardsize(&attr, 0);

//    if (pthread_attr_setstacksize(&attr, 32 * 1024) != 0) {
//        perror("pthread_attr_setstacksize");
//    }

    pthread_create(&thr, &attr, &runInThread, (void *) this);
}

void hlcup::Worker::Run() {
    int efd = epoll_create1(0);
    if (efd == -1) {
        perror("epoll_create1");
    }

    _connPool = new ConnectionsPool(_db, _updateQueue);

    auto srvConn = new Connection(_sfd, nullptr, nullptr);
    struct epoll_event event{};
    event.data.ptr = srvConn;
    event.events = EPOLLIN;
    if (epoll_ctl(efd, EPOLL_CTL_ADD, _sfd, &event) == -1) {
        perror("epoll_ctl");
        abort();
    }

    struct epoll_event *events;
    events = (epoll_event *) calloc(MAXEVENTS, sizeof event);

    while (true) {
        auto n = epoll_wait(efd, events, MAXEVENTS, 0);
        for (auto i = 0; i < n; i++) {
            auto conn = (Connection *) events[i].data.ptr;

            if ((events[i].events & EPOLLERR) || (events[i].events & EPOLLHUP)
                || (!(events[i].events & EPOLLIN))) {
                /* An error has occured on this fd, or the socket is not
                 ready for reading (why were we notified then?) */
//                std::cerr << conn->_fd << ": " << "epoll ERR/HUP" << std::endl;
//                close(conn->_fd);
                if (conn != srvConn) {
//                    std::cerr << conn->_fd << ": " << "Not server, close" << std::endl;
                    close(conn->_fd);
                    delete conn;
//                    _connPool->PutConnection(conn);
                }
                continue;

            } else if (conn == srvConn) {
                /* We have a notification on the listening socket, which
                 means one or more incoming connections. */
                struct sockaddr in_addr{};
                socklen_t in_len;

                in_len = sizeof in_addr;
                int infd = accept4(_sfd, &in_addr, &in_len, SOCK_NONBLOCK);
                if (infd == -1) {
                    if ((errno == EAGAIN) || (errno == EWOULDBLOCK)) {
                        continue;
                    } else {
                        perror("accept");
                        continue;
                    }
                }

//                std::cerr << infd << ": " << "accept" << std::endl;

                int val = true;
                if (setsockopt(infd, IPPROTO_TCP, TCP_NODELAY, &val, sizeof(val)) == -1) {
                    perror("TCP_NODELAY");
                }

                event.data.ptr = _connPool->GetConnection(infd);
                event.events = EPOLLIN | EPOLLET;
                if (epoll_ctl(efd, EPOLL_CTL_ADD, infd, &event) == -1) {
                    perror("epoll_ctl");
                    abort();
                }

            } else {
                while (true) {
                    auto count = read(conn->_fd, conn->_inBuf.End, conn->_inBuf.Capacity);
//                    std::cerr << conn->_fd << ": " << "Read " << count << std::endl;
                    if (count == -1) {
                        if (errno == EAGAIN) {
//                            std::cerr << conn->_fd << ": " << "Process" << std::endl;
                            if (conn->ProcessEvent()) {
                                if (conn->_method == Connection::POST) {
                                    close(conn->_fd);
                                    _connPool->PutConnection(conn);
                                } else {
                                    conn->Reset(conn->_fd);
                                }
                            }
                            break;
                        }
                        perror("read");
                        close(conn->_fd);
                        _connPool->PutConnection(conn);
                        break;
                    } else if (count == 0) {
//                        std::cerr << conn->_fd << ": " << "Close" << std::endl;
                        close(conn->_fd);
                        _connPool->PutConnection(conn);
                        break;
                    } else { // count > 0
                        conn->_inBuf.AddLen(static_cast<size_t>(count));
                        if (conn->_inBuf.Len() > conn->_inBuf.Capacity) {
                            std::cerr << conn->_fd << ": " << "Buffer overflow" << std::endl;
//                            close(conn->_fd);
//                            break;
                        }
                    }
                }
            }
        }
    }
}
