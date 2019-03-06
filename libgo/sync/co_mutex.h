#pragma once
#include "../common/config.h"
#include "../scheduler/processer.h"
#include <queue>
#include "co_condition_variable.h"
#include <boost/lockfree/queue.hpp>

namespace co
{

#define BY_MUTEX 0

/// 协程锁
class CoMutex
{
#if BY_MUTEX
    typedef std::mutex lock_t;
    lock_t lock_;
    bool notified_ = false;
    ConditionVariableAny<> cv_;
#else
    boost::lockfree::queue<Processer::SuspendEntry*, boost::lockfree::fixed_sized<false>> waitQueue_;
    LFLock notified_;
#endif

    std::atomic_long sem_;


public:
    CoMutex();
    ~CoMutex();

    void lock();
    bool try_lock();
    bool is_lock();
    void unlock();
};

typedef CoMutex co_mutex;

} //namespace co
