#include <thread>
#include "UpdateQueue.h"

hlcup::UpdateQueue::UpdateQueue(hlcup::DB *_db) : _db(_db) {}

void hlcup::UpdateQueue::Add(hlcup::UpdateQueue::ActionType type, const hlcup::Account &account) {
    std::unique_lock<std::mutex> lock(_mutex);
    _queue.push({type, account});
    lock.unlock();
    _cond.notify_all();
}

void hlcup::UpdateQueue::AddLikes(const std::vector<hlcup::UpdateQueue::batch_like> &likes) {
    std::unique_lock<std::mutex> lock(_mutex);
    _queue.push({ACTION_ADD_LIKES, {}, likes});
    lock.unlock();
    _cond.notify_all();
}

void hlcup::UpdateQueue::Run(uint8_t threadsCnt) {
    std::vector<std::thread> consumers;
    consumers.reserve(threadsCnt);

    for (int i = 0; i < threadsCnt; ++i) {
        consumers.emplace_back([this]() {
            while (!_done) {
                std::unique_lock<std::mutex> lock(_mutex);
                _cond.wait(lock, [this]() { return !_queue.empty(); });
                auto action = _queue.front();
                _queue.pop();
                lock.unlock();
                _cond.notify_all();

                switch (action.type) {
                    case ACTION_NEW:
                        _db->InsertIndex(action.account);
                        break;
                    case ACTION_UPDATE:
                        _db->UpdateIndex(action.account);
                        break;
                    case ACTION_ADD_LIKES:
                        for (const auto &like: action.likes) {
                            _db->_accounts[like.liker]._likes.emplace_back(like.likee, like.ts);
                            _db->_accounts[like.likee]._likedMe.insert(like.liker);
                        }
                        break;
                }
            }

            return 0;
        });
    }

    for (auto &t: consumers)
        t.detach();
}
