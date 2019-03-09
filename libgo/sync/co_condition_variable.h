#pragma once
#include "../common/config.h"
#include "../scheduler/processer.h"
#include <list>
#include <condition_variable>
#include "wait_queue.h"

namespace co
{

/// 协程条件变量
// 1.与std::condition_variable_any的区别在于析构时不能有正在等待的协程, 否则抛异常

template <typename T>
class ConditionVariableAnyT
{
public:
    typedef std::function<void(T &)> Functor;

    enum class cv_status { no_timeout, timeout, no_queued };

private:
    // 兼容原生线程
    struct NativeThreadEntry
    {
        std::mutex mtx;

        std::condition_variable_any cv;

        LFLock2 notified;
    };

    struct Entry : public WaitQueueHook, public RefObject
    {
        // 控制是否超时的标志位
        LFLock noTimeoutLock;

        Processer::SuspendEntry coroEntry;

        T value;

        NativeThreadEntry* nativeThreadEntry;

        bool isWaiting;

        Entry() : value(), nativeThreadEntry(nullptr), isWaiting(true) {}
        ~Entry() {
            if (nativeThreadEntry) {
                delete nativeThreadEntry;
                nativeThreadEntry = nullptr;
            }
        }

        bool notify(Functor const func) {
            // coroutine
            if (!nativeThreadEntry) {
                if (!noTimeoutLock.try_lock())
                    return false;;

                if (Processer::Wakeup(coroEntry, [&]{ if (func) func(value); })) {
//                    DebugPrint(dbg_channel, "notify %d.", value.id);
                    return true;
                }

                return false;;
            }

            // native thread
            std::unique_lock<std::mutex> threadLock(nativeThreadEntry->mtx);
            if (!noTimeoutLock.try_lock())
                return false;;

            if (func)
                func(value);
            nativeThreadEntry->cv.notify_one();
            return true;
        }
    };

    WaitQueue<Entry> queue_;

public:
    typedef typename WaitQueue<Entry>::CondRet CondRet;

public:
    explicit ConditionVariableAnyT(size_t nonblockingCapacity = 0,
            Functor convertToNonblockingFunctor = NULL)
        : queue_(&isValid, nonblockingCapacity,
                [=](Entry *entry)
                {
                    if (entry->notify(convertToNonblockingFunctor)) {
                        entry->isWaiting = false;
                        return true;
                    }
                    return false;
                })
    {
    }
    ~ConditionVariableAnyT() {}

    template <typename LockType>
    cv_status wait(LockType & lock,
            T value = T(),
            std::function<CondRet(size_t)> const& cond = NULL)
    {
        std::chrono::seconds* time = nullptr;
        return do_wait(lock, time, value, cond);
    }

    template <typename LockType, typename TimeDuration>
    cv_status wait_for(LockType & lock,
            TimeDuration duration,
            T value = T(),
            std::function<CondRet(size_t)> const& cond = NULL)
    {
        return do_wait(lock, &duration, value, cond);
    }

    template <typename LockType, typename TimePoint>
    cv_status wait_util(LockType & lock,
            TimePoint timepoint,
            T value = T(),
            std::function<CondRet(size_t)> const& cond = NULL)
    {
        return do_wait(lock, &timepoint, value, cond);
    }

    bool notify_one(Functor const& func = NULL)
    {
        Entry* entry = nullptr;
        while (queue_.pop(entry)) {
            AutoRelease<Entry> pEntry(entry);

            if (!entry->isWaiting) {
                if (func)
                    func(entry->value);
                return true;
            }

            if (entry->notify(func))
                return true;
        }

        return false;
    }

    size_t notify_all(Functor const& func = NULL)
    {
        size_t n = 0;
        while (notify_one(func))
            ++n;
        return n;
    }

    bool empty() {
        return queue_.empty();
    }

    bool size() {
        return queue_.size();
    }

private:
    template <typename TimeType>
    inline void coroSuspend(Processer::SuspendEntry & coroEntry, TimeType * time)
    {
        if (time)
            coroEntry = Processer::Suspend(*time);
        else
            coroEntry = Processer::Suspend();
    }

    template <typename LockType, typename Rep, typename Period>
    inline void threadSuspend(std::condition_variable_any & cv,
            LockType & lock, std::chrono::duration<Rep, Period> * dur)
    {
        if (dur)
            cv.wait_for(lock, *dur);
        else
            cv.wait(lock);
    }

    template <typename LockType, typename Clock, typename Duration>
    inline void threadSuspend(std::condition_variable_any & cv,
            LockType & lock, std::chrono::time_point<Clock, Duration> * tp)
    {
        if (tp)
            cv.wait_until(lock, *tp);
        else
            cv.wait(lock);
    }

    template <typename LockType, typename TimeType>
    cv_status do_wait(LockType & lock,
            TimeType* time, T value = T(),
            std::function<CondRet(size_t)> const& cond = NULL)
    {
        Entry *entry = new Entry;
        AutoRelease<Entry> pEntry(entry);
        entry->value = value;
        auto ret = queue_.push(entry, [&](size_t queueSize){
                CondRet ret{true, true};
                if (cond) {
                    ret = cond(queueSize);
                    if (!ret.canQueue)
                        return ret;
                }

                entry->IncrementRef();

                if (!ret.needWait) {
                    entry->isWaiting = false;
                    return ret;
                }

                if (Processer::IsCoroutine()) {
                    coroSuspend(entry->coroEntry, time);
                } else {
                    entry->nativeThreadEntry = new NativeThreadEntry;
                }
                    
                return ret;
                });

        lock.unlock();

        if (!ret.canQueue) {
            return cv_status::no_queued;
        }

        if (!ret.needWait) {
            return cv_status::no_timeout;
        }

        if (Processer::IsCoroutine()) {
            // 协程
            Processer::StaticCoYield();
            lock.lock();
            return entry->noTimeoutLock.try_lock() ?
                cv_status::timeout :
                cv_status::no_timeout;
        } else {
            // 原生线程
            std::unique_lock<std::mutex> threadLock(entry->nativeThreadEntry->mtx);
            threadSuspend(entry->nativeThreadEntry->cv, threadLock, time);
            lock.lock();
            return entry->noTimeoutLock.try_lock() ?
                cv_status::timeout :
                cv_status::no_timeout;
        }
    }

    static bool isValid(Entry* entry) {
        if (!entry->isWaiting) return true;
        if (!entry->nativeThreadEntry)
            return !entry->coroEntry.IsExpire();
        return entry->nativeThreadEntry->notified.is_lock();
    }
};

typedef ConditionVariableAnyT<bool> ConditionVariableAny;

} //namespace co
