#ifndef HLCUP2018_ITERATORS_H
#define HLCUP2018_ITERATORS_H

#include "DB.h"
#include "ids_set.h"
#include <list>
#include <iostream>
#include <wmmintrin.h>

namespace hlcup {

    class dbIterator {
    public:
        static const uint32_t end = uint32_t(0);

        virtual ~dbIterator() = default;

        virtual uint32_t next() = 0;

        virtual uint32_t next(uint32_t n) = 0;

        virtual uint32_t cur() = 0;

        virtual void reset() = 0;

        virtual dbIterator *clone() = 0;

        virtual void dump(const char *offset) = 0;
    };

    class sequenceIterator : public dbIterator {
    public:
        explicit sequenceIterator(uint32_t firstId) : _cur(firstId), _start(firstId) {};

        uint32_t next() override {
            --_cur;
            if (_cur == 0)
                return dbIterator::end;
            return _cur;
        }

        uint32_t next(uint32_t n) override {
            _cur = n - 1;
            return _cur;
        }

        uint32_t cur() override {
            return _cur;
        }

        void dump(const char *offset) override {
            std::cerr << offset << "Sequence from " << _cur << std::endl;
        }

        void reset() override {
            _cur = _start;
        }

        dbIterator *clone() override {
            return new sequenceIterator(_start);
        };

    private:
        uint32_t _start, _cur;
    };

    class idsSetIterator : public dbIterator {
    public:
        idsSetIterator(std::vector<uint32_t>::const_iterator cur, std::vector<uint32_t>::const_iterator end)
                : _start(&*cur), _cur(&*cur), _end(&*end), _sseEnd(&*end - 4) {
        };

        idsSetIterator(const uint32_t *cur, const uint32_t *end)
                : _start(cur), _cur(cur), _end(end), _sseEnd(end - 4) {
        };

        uint32_t next() override {
            if (_cur == _end)
                return dbIterator::end;

            if (++_cur == _end)
                return dbIterator::end;

            return *_cur;
        };

        uint32_t next(uint32_t n) override {
            auto sseN = _mm_set1_epi32(n);

            while (_cur < _sseEnd) {
                auto sseArr = _mm_loadu_si128((__m128i *) _cur);
                switch (_mm_movemask_ps(_mm_cmpgt_epi32(sseArr, sseN))) {
                    case 0:
                        return *_cur;
                    case 1:
                        ++_cur;
                        return *_cur;
                    case 3:
                        _cur += 2;
                        return *_cur;
                    case 7:
                        _cur += 3;
                        return *_cur;
                    case 15:
                        _cur += 4;
                        break;
                    default:
                        break;
                }
            }

            while (_cur < _end) {
                if (*_cur <= n)
                    return *_cur;
                ++_cur;
            }

            return dbIterator::end;
        }

        uint32_t cur() override {
            if (_cur == _end) {
                return dbIterator::end;
            }
            return *_cur;
        }

        void reset() override {
            _cur = _start;
        }

        dbIterator *clone() override {
            return new idsSetIterator(_start, _end);
        };

        void dump(const char *offset) override {
            std::cerr << offset;
            for (auto tmp = _cur; tmp != _end; ++tmp)
                std::cerr << *tmp << " ";
            std::cerr << std::endl;
        }

    private:
        const uint32_t *_start, *_cur, *_end, *_sseEnd;
    };

    class unionIterator : public dbIterator {
    public:
        unionIterator(dbIterator *left, dbIterator *right, uint32_t cur)
                : _left(left), _right(right), _cur(cur), _start(cur) {};

        unionIterator(dbIterator *left, dbIterator *right) : _left(left), _right(right) {
            auto curL = _left->cur();
            auto curR = _right->cur();

            if (curL == dbIterator::end && curR == dbIterator::end) {
                _cur = dbIterator::end;
                _start = _cur;
                return;
            }

            if (curL == curR) {
                _cur = curL;
                _left->next();
                _right->next();
            } else if (curL > curR) {
                _cur = curL;
                _left->next();
            } else {
                _cur = curR;
                _right->next();
            }

            _start = _cur;
        }

        ~unionIterator() override {
            delete _left;
            delete _right;
        }

        uint32_t next() override {
            if (_cur == dbIterator::end)
                return dbIterator::end;

            auto curL = _left->cur();
            auto curR = _right->cur();

            if (curL == dbIterator::end && curR == dbIterator::end) {
                _cur = dbIterator::end;
                return _cur;
            }

            if (curL == curR) {
                _cur = curL;
                _left->next();
                _right->next();
            } else if (curL > curR) {
                _cur = curL;
                _left->next();
            } else {
                _cur = curR;
                _right->next();
            }

            return _cur;
        };

        uint32_t next(uint32_t n) override {
            if (_cur == dbIterator::end)
                return dbIterator::end;

            auto curL = _left->cur();
            if (n < curL)
                curL = _left->next(n);

            auto curR = _right->cur();
            if (n < curR)
                curR = _right->next(n);

            if (curL == dbIterator::end && curR == dbIterator::end) {
                _cur = dbIterator::end;
                return _cur;
            }

            if (curL == curR) {
                _cur = curL;
                _left->next();
                _right->next();
            } else if (curL > curR) {
                _cur = curL;
                _left->next();
            } else {
                _cur = curR;
                _right->next();
            }

            return _cur;
        }

        uint32_t cur() override {
            return _cur;
        };

        void reset() override {
            _left->reset();
            _right->reset();
            _cur = _start;
        }

        dbIterator *clone() override {
            return new unionIterator(_left->clone(), _right->clone(), _start);
        };

        void dump(const char *offset) override {
            std::cerr << offset << "Union [" << _cur << "]" << std::endl;
            std::string s(offset);
            s += "\t";
            _left->dump(s.c_str());
            _right->dump(s.c_str());
        }

    private:
        dbIterator *_left, *_right;
        uint32_t _cur = dbIterator::end, _start;
    };

    class intersectIterator : public dbIterator {
    public:
        intersectIterator(dbIterator *left, dbIterator *right, uint32_t cur)
                : _left(left), _right(right), _cur(cur), _start(cur) {};

        intersectIterator(dbIterator *left, dbIterator *right) : _left(left), _right(right) {
            auto curL = _left->cur();
            if (curL == dbIterator::end) {
                _cur = dbIterator::end;
                return;
            }

            auto curR = _right->cur();
            if (curR == dbIterator::end) {
                _cur = dbIterator::end;
                return;
            }

            if (curL == curR) {
                _cur = curL;
                return;
            }

            while (curL != curR) {
                if (curL < curR) {
                    curR = _right->next(curL);
                    if (curR == dbIterator::end) {
                        _cur = dbIterator::end;
                        return;
                    }
                } else {
                    curL = _left->next(curR);
                    if (curL == dbIterator::end) {
                        _cur = dbIterator::end;
                        return;
                    }
                }
            }

            _cur = curL;
            _start = _cur;
        };

        ~intersectIterator() override {
            delete _left;
            delete _right;
        };

        uint32_t next() override {
            if (_cur == dbIterator::end)
                return dbIterator::end;

            auto curL = _left->next();
            if (curL == dbIterator::end) {
                _cur = dbIterator::end;
                return _cur;
            }

            auto curR = _right->next();
            if (curR == dbIterator::end) {
                _cur = dbIterator::end;
                return _cur;
            }

            while (curL != curR) {
                if (curL < curR) {
                    curR = _right->next(curL);
                    if (curR == dbIterator::end) {
                        _cur = dbIterator::end;
                        return _cur;
                    }
                } else {
                    curL = _left->next(curR);
                    if (curL == dbIterator::end) {
                        _cur = dbIterator::end;
                        return _cur;
                    }
                }
            }

            _cur = curL;
            return _cur;
        };

        uint32_t next(uint32_t n) override {
            if (_cur == dbIterator::end)
                return dbIterator::end;

            auto curL = _left->cur();
            if (n < curL)
                curL = _left->next(n);
            if (curL == dbIterator::end) {
                _cur = dbIterator::end;
                return _cur;
            }

            auto curR = _right->cur();
            if (n < curR)
                curR = _right->next(n);
            if (curR == dbIterator::end) {
                _cur = dbIterator::end;
                return _cur;
            }

            while (curL != curR) {
                if (curL < curR) {
                    curR = _right->next(curL);
                    if (curR == dbIterator::end) {
                        _cur = dbIterator::end;
                        return _cur;
                    }
                } else {
                    curL = _left->next(curR);
                    if (curL == dbIterator::end) {
                        _cur = dbIterator::end;
                        return _cur;
                    }
                }
            }

            _cur = curL;
            return _cur;
        }

        uint32_t cur() override {
            return _cur;
        };

        void reset() override {
            _left->reset();
            _right->reset();
            _cur = _start;
        }

        dbIterator *clone() override {
            return new intersectIterator(_left->clone(), _right->clone(), _start);
        };

        void dump(const char *offset) override {
            std::cerr << offset << "Intersection [" << _cur << "]" << std::endl;
            std::string s(offset);
            s += "\t";
            _left->dump(s.c_str());
            _right->dump(s.c_str());
        }

    private:
        dbIterator *_left, *_right;
        uint32_t _start = dbIterator::end, _cur = dbIterator::end;
    };

    class unionIteratorBuilder {
    public:
        void add(dbIterator *it) {
            _iterators.push_back(it);
        };

        dbIterator *build() {
            if (_iterators.empty())
                return nullptr;

            while (_iterators.size() > 1) {
                auto insIt = _iterators.begin();
                dbIterator *tmp = nullptr;

                for (auto it: _iterators) {
                    if (tmp) {
                        *insIt = new unionIterator(tmp, it);
                        ++insIt;
                        tmp = nullptr;
                    } else {
                        tmp = it;
                    }
                }

                if (tmp) {
                    *insIt = tmp;
                    ++insIt;
                }

                _iterators.resize(static_cast<unsigned long>(std::distance(_iterators.begin(), insIt)));
            }

            return _iterators[0];
        }

    private:
        std::vector<dbIterator *> _iterators;
    };

    class intersectIteratorBuilder {
    public:
        void add(dbIterator *it) {
            _iterators.push_back(it);
        };

        dbIterator *build() {
            if (_iterators.empty())
                return nullptr;

            while (_iterators.size() > 1) {
                auto insIt = _iterators.begin();
                dbIterator *tmp = nullptr;

                for (auto it: _iterators) {
                    if (tmp) {
                        *insIt = new intersectIterator(tmp, it);
                        ++insIt;
                        tmp = nullptr;
                    } else {
                        tmp = it;
                    }
                }

                if (tmp) {
                    *insIt = tmp;
                    ++insIt;
                }

                _iterators.resize(static_cast<unsigned long>(std::distance(_iterators.begin(), insIt)));
            }

            return _iterators[0];
        }

        size_t size() {
            return _iterators.size();
        }

    private:
        std::vector<dbIterator *> _iterators;
    };
}

#endif //HLCUP2018_ITERATORS_H
