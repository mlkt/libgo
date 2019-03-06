#include "co_mutex.h"
#include "../scheduler/scheduler.h"

namespace co
{

CoMutex::CoMutex()
{
    sem_ = 1;
#if BY_MUTEX
#else
    notified_.try_lock();
#endif
}

CoMutex::~CoMutex()
{
//    assert(lock_.try_lock());
}

void CoMutex::lock()
{
    if (--sem_ == 0)
        return ;

#if BY_MUTEX
    std::unique_lock<lock_t> lock(lock_);
    if (notified_) {
        notified_ = false;
        return ;
    }
    cv_.wait(lock);
#else
    if (notified_.try_lock())
        return ;

    Processer::SuspendEntry* entry = new Processer::SuspendEntry(Processer::Suspend());
    waitQueue_.push(entry);

    if (notified_.try_lock()) {
        Processer::Wakeup(*entry);
    }

    Processer::StaticCoYield();
#endif
}

bool CoMutex::try_lock()
{
    if (--sem_ == 0)
        return true;

    ++sem_;
    return false;
}

bool CoMutex::is_lock()
{
    return sem_ < 1;
}

void CoMutex::unlock()
{
    if (++sem_ == 1)
        return ;

#if BY_MUTEX
    std::unique_lock<lock_t> lock(lock_);
    if (!cv_.notify_one())
        notified_ = true;
#else
    Processer::SuspendEntry* entry;
    while (waitQueue_.pop(entry)) {
        if (Processer::Wakeup(*entry)) {
            delete entry;
            return ;
        }
        delete entry;
    }

    notified_.unlock();
#endif
}

} //namespace co
