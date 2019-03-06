#pragma once
#include "../common/config.h"
#include "../common/ts_queue.h"
#include "../scheduler/processer.h"
#include "../common/error.h"
#include <list>
#include <condition_variable>

namespace co
{

/// 协程条件变量
// 1.与std::condition_variable_any的区别在于析构时不能有正在等待的协程, 否则抛异常
// 2.notify_one触发原生线程的等待时, 可能误触发两个: 第一个正常触发, 第二个被判定为超时;
//   所以使用的时候要自己加pred, 让超时的重新进入等待状态, 才能完全兼容原生线程. (协程不存在这个问题)

enum notify_operate {
    notify_only,
    notify_read,
    notify_write,
};

template <typename T = bool>
class ConditionVariableAny
{
    typedef std::function<void()> Func;

    struct Entry
        : public TSQueueHook, public SharedRefObject
    {
        Processer::SuspendEntry suspendEntry;

        // 控制是否超时的标志位
        LFLock noTimeoutLock;

        T* value;
    };

    typedef FakeLock lock_t;
    lock_t lock_;
    TSQueue<Entry, false> queue_;
    Entry* checkIter_ = nullptr;

    // 兼容原生线程
    std::condition_variable_any cv_;

public:
    ~ConditionVariableAny();

    bool notify_one(notify_operate op = notify_only, T *pt = nullptr);
    size_t notify_all();

    bool empty();

    template <typename LockType>
    std::cv_status wait(LockType & lock, T* pt = nullptr, bool relock = true) {
        Entry *entry = new Entry;
        IncursivePtr<Entry> sptr(entry);
        entry->value = pt;

        if (Processer::IsCoroutine()) {
            // 协程
            entry->suspendEntry = Processer::Suspend();
            AddWaiter(entry);
            lock.unlock();
            Processer::StaticCoYield();
            if (relock)
                lock.lock();
        } else {
            // 原生线程
            AddWaiter(entry);
            cv_.wait(lock);
        }

        return entry->noTimeoutLock.try_lock() ? std::cv_status::timeout : std::cv_status::no_timeout;
    }

    template <typename LockType, typename Rep, typename Period>
    std::cv_status wait_for(LockType & lock, std::chrono::duration<Rep, Period> const& duration,
            T* pt = nullptr, bool relock = true)
    {
        Entry *entry = new Entry;
        IncursivePtr<Entry> sptr(entry);
        entry->value = pt;

        if (Processer::IsCoroutine()) {
            // 协程
            entry->suspendEntry = Processer::Suspend(duration);
            AddWaiter(entry);
            lock.unlock();
            Processer::StaticCoYield();
            if (relock)
                lock.lock();
        } else {
            // 原生线程
            AddWaiter(entry);
            cv_.wait_for(lock, duration);
        }

        return entry->noTimeoutLock.try_lock() ? std::cv_status::timeout : std::cv_status::no_timeout;
    }

    template <typename LockType, typename Clock, typename Duration>
    std::cv_status wait_until(LockType & lock, std::chrono::time_point<Clock, Duration> const& timepoint,
            T* pt = nullptr, bool relock = true)
    {
        Entry *entry = new Entry;
        IncursivePtr<Entry> sptr(entry);
        entry->value = pt;

        if (Processer::IsCoroutine()) {
            // 协程
            entry->suspendEntry = Processer::Suspend(timepoint);
            AddWaiter(entry);
            lock.unlock();
            Processer::StaticCoYield();
            if (relock)
                lock.lock();
        } else {
            // 原生线程
            AddWaiter(entry);
            cv_.wait_until(lock, timepoint);
        }

        return entry->noTimeoutLock.try_lock() ? std::cv_status::timeout : std::cv_status::no_timeout;
    }

    template <typename LockType, typename Rep, typename Period>
    std::cv_status timedWait(LockType & lock, std::chrono::duration<Rep, Period> const& duration) {
        return wait_for(lock, duration);
    }

    template <typename LockType, typename Clock, typename Duration>
    std::cv_status timedWait(LockType & lock, std::chrono::time_point<Clock, Duration> const& timepoint) {
        return wait_util(lock, timepoint);
    }

private:
    void AddWaiter(Entry *entry);
};

} //namespace co

#include "co_condition_variable.hpp"
