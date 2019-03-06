#pragma once
#include "../common/config.h"
#include "../scheduler/processer.h"
#include "../scheduler/scheduler.h"
#include "co_condition_variable.h"
#include <boost/lockfree/queue.hpp>

namespace co
{

template <typename T>
class Channel
{
private:
    class ChannelImpl;
    mutable std::shared_ptr<ChannelImpl> impl_;

public:
    explicit Channel(std::size_t capacity = 0)
    {
        impl_.reset(new ChannelImpl(capacity));
    }

    void SetDbgMask(uint64_t mask)
    {
        impl_->SetDbgMask(mask);
    }

    Channel const& operator<<(T t) const
    {
        impl_->Push(t, true);
        return *this;
    }

    Channel const& operator>>(T & t) const
    {
        impl_->Pop(t, true);
        return *this;
    }

    Channel const& operator>>(std::nullptr_t ignore) const
    {
        T t;
        impl_->Pop(t, true);
        return *this;
    }

    bool TryPush(T t) const
    {
        return impl_->Push(t, false);
    }

    bool TryPop(T & t) const
    {
        return impl_->Pop(t, false);
    }

    bool TryPop(std::nullptr_t ignore) const
    {
        T t;
        return impl_->Pop(t, false);
    }

    template <typename Rep, typename Period>
    bool TimedPush(T t, std::chrono::duration<Rep, Period> dur) const
    {
        return impl_->Push(t, true, dur + FastSteadyClock::now());
    }

    bool TimedPush(T t, FastSteadyClock::time_point deadline) const
    {
        return impl_->Push(t, true, deadline);
    }

    template <typename Rep, typename Period>
    bool TimedPop(T & t, std::chrono::duration<Rep, Period> dur) const
    {
        return impl_->Pop(t, true, dur + FastSteadyClock::now());
    }

    bool TimedPop(T & t, FastSteadyClock::time_point deadline) const
    {
        return impl_->Pop(t, true, deadline);
    }

    template <typename Rep, typename Period>
    bool TimedPop(std::nullptr_t ignore, std::chrono::duration<Rep, Period> dur) const
    {
        T t;
        return impl_->Pop(t, true, dur + FastSteadyClock::now());
    }

    bool TimedPop(std::nullptr_t ignore, FastSteadyClock::time_point deadline) const
    {
        T t;
        return impl_->Pop(t, true, deadline);
    }

    bool Unique() const
    {
        return impl_.unique();
    }

    void Close() const {
        impl_->Close();
    }

    // ------------- 兼容旧版接口
    bool empty() const
    {
        return impl_->Empty();
    }

    std::size_t size() const
    {
        return impl_->Size();
    }

private:
    class ChannelImpl : public IdCounter<ChannelImpl>
    {
        typedef std::mutex lock_t;
        lock_t lock_;
        const std::size_t capacity_;
        bool closed_;
        std::deque<T> queue_;
        uint64_t dbg_mask_;

#define BY_LOCKFREE_QUEUE 1

#if BY_LOCKFREE_QUEUE
        struct Entry {
            Processer::SuspendEntry entry;
            atomic_t<T*> pvalue;

            Entry() : pvalue(nullptr) {}
            Entry(Processer::SuspendEntry && e, T* p) : entry(e), pvalue(p) {}
        };

        struct Slot {
            Entry wait;
            atomic_t<int> sem {0};
        };
        static const size_t c_slot = 128;

        Slot slots_[c_slot];
        atomic_t<size_t> rSeek_ {0};
        atomic_t<size_t> wSeek_ {0};

//        typedef boost::lockfree::queue<Entry*, boost::lockfree::fixed_sized<false>> mpmc_queue_t;
//        typedef std::queue<Entry> mpmc_queue_t;
//        mpmc_queue_t rq_;
//        mpmc_queue_t wq_;
#else
        // 兼容原生线程
        ConditionVariableAny<T> wCv_;
        ConditionVariableAny<T> rCv_;
#endif

    public:
        explicit ChannelImpl(std::size_t capacity)
            : capacity_(capacity), closed_(false), dbg_mask_(dbg_all)
        {
            DebugPrint(dbg_mask_ & dbg_channel, "[id=%ld] Channel init. capacity=%lu", this->getId(), capacity);
        }

        ~ChannelImpl() {
            DebugPrint(dbg_mask_ & dbg_channel, "[id=%ld] Channel destory.", this->getId());

            assert(lock_.try_lock());
        }

        void SetDbgMask(uint64_t mask) {
            dbg_mask_ = mask;
        }

        bool Empty()
        {
            std::unique_lock<lock_t> lock(lock_);
            return queue_.empty();
        }

        std::size_t Size()
        {
            std::unique_lock<lock_t> lock(lock_);
            return queue_.size();
        }

        // write
        bool Push(T t, bool bWait, FastSteadyClock::time_point deadline = FastSteadyClock::time_point{})
        {
#if BY_LOCKFREE_QUEUE
            if (closed_) return false;

            size_t wSeek = ++wSeek_;
            Slot & slot = slots_[wSeek % c_slot];

            int sem = ++slot.sem;
            if (sem == 2) {
                // other first into
                T* pt = nullptr;
                for (;;) {
                    pt = slot.wait.pvalue.load(std::memory_order_acquire);
                    if (!pt)
                        continue;

                    break;
                }

//                *pt = t;
                slot.wait.pvalue.store(nullptr, std::memory_order_relaxed);
                slot.sem.store(0, std::memory_order_relaxed);
                Processer::Wakeup(slot.wait.entry);
                return true;
            } else if (sem == 1) {
                // me first into
                slot.wait.entry = Processer::Suspend();
                slot.wait.pvalue.store(&t, std::memory_order_release);
                Processer::StaticCoYield();
                return true;
            }

            assert(false);
            return false;

//            if (closed_) return false;
//            std::unique_lock<lock_t> lock(lock_);
//            if (closed_) return false;
//
//            T* pt = &t;
//            while (!rq_.empty()) {
//                Entry entryV = rq_.front();
//                rq_.pop();
//                Entry* entry = &entryV;
//                if (Processer::Wakeup(entry->entry, [=]{ *entry->pvalue = *pt; })) {
//                    return true;
//                }
//            }
////            T* pt = &t;
////            Entry *entry;
////            while (rq_.pop(entry)) {
////                if (Processer::Wakeup(entry->entry, [entry, pt]{ *entry->pvalue = *pt; })) {
////                    delete entry;
////                    return true;
////                }
////                delete entry;
////            }
//
//            if (closed_) return false;
//
////            entry = new Entry(Processer::Suspend(), &t);
////            wq_.push(entry);
//            wq_.emplace(Entry(Processer::Suspend(), &t));
//            lock.unlock();
//            Processer::StaticCoYield();
//            return true;
#else
            if (closed_) return false;

            std::unique_lock<lock_t> lock(lock_);
            if (closed_) return false;

            if (capacity_ == 0) {
                // zero capacity
                if (rCv_.notify_one(notify_write, &t))
                    return true;

                if (!bWait) return false;

                if (deadline == FastSteadyClock::time_point{})
                    wCv_.wait(lock, &t);
                else if (wCv_.wait_until(lock, deadline, &t) == std::cv_status::timeout) {
                    return false;
                }

                return true;
            }

            if (rCv_.notify_one(notify_write, &t))
                return true;

            while (queue_.size() >= capacity_) {
                if (!bWait) return false;

                if (deadline == FastSteadyClock::time_point{})
                    wCv_.wait(lock, &t);
                else if (wCv_.wait_until(lock, deadline, &t) == std::cv_status::timeout) {
                    return false;
                }
            }

            queue_.emplace_back(std::move(t));
            return true;
#endif
        }

        // read
        bool Pop(T & t, bool bWait, FastSteadyClock::time_point deadline = FastSteadyClock::time_point{})
        {
#if BY_LOCKFREE_QUEUE
            size_t rSeek = ++rSeek_;
            Slot & slot = slots_[rSeek % c_slot];

            int sem = ++slot.sem;
            if (sem == 2) {
                // other first into
                T* pt = nullptr;
                for (;;) {
                    pt = slot.wait.pvalue.load(std::memory_order_acquire);
                    if (!pt)
                        continue;

                    break;
                }

//                t = std::move(*pt);
                slot.wait.pvalue.store(nullptr, std::memory_order_relaxed);
                slot.sem.store(0, std::memory_order_relaxed);
                Processer::Wakeup(slot.wait.entry);
                return true;
            } else if (sem == 1) {
                // me first into
                slot.wait.entry = Processer::Suspend();
                slot.wait.pvalue.store(&t, std::memory_order_release);
                Processer::StaticCoYield();
                return true;
            }

            assert(false);
            return false;

//            std::unique_lock<lock_t> lock(lock_);
//
//            T* pt = &t;
//            while (!wq_.empty()) {
//                Entry entryV = wq_.front();
//                wq_.pop();
//                Entry* entry = &entryV;
//                if (Processer::Wakeup(entry->entry, [entry, pt]{ *pt = *entry->pvalue; })) {
//                    return true;
//                }
//            }
////            Entry *entry;
////            while (wq_.pop(entry)) {
////                if (Processer::Wakeup(entry->entry, [entry, pt]{ *pt = *entry->pvalue; })) {
////                    delete entry;
////                    return true;
////                }
////                delete entry;
////            }
//
//            if (closed_) return false;
//
////            entry = new Entry(Processer::Suspend(), &t);
////            rq_.push(entry);
//            rq_.emplace(Entry(Processer::Suspend(), &t));
//            lock.unlock();
//            Processer::StaticCoYield();
//            return true;
#else
            std::unique_lock<lock_t> lock(lock_);

            if (capacity_ == 0) {
                // zero capacity
                if (wCv_.notify_one(notify_read, &t))
                    return true;

                if (!bWait) return false;
                if (closed_) return false;

                if (deadline == FastSteadyClock::time_point{})
                    rCv_.wait(lock, &t);
                else if (rCv_.wait_until(lock, deadline, &t) == std::cv_status::timeout) {
                    return false;
                }

                return true;
            }

            if (!queue_.empty()) {
                t = std::move(queue_.front());
                queue_.pop_front();
                return true;
            }

            if (wCv_.notify_one(notify_read, &t))
                return true;

            while (queue_.empty()) {
                if (!bWait) return false;
                if (closed_) return false;
            }

            return true;
#endif
        }

        void Close()
        {
            std::unique_lock<lock_t> lock(lock_);
            if (closed_) return ;

            DebugPrint(dbg_mask_ & dbg_channel, "[id=%ld] Channel Closed. size=%d", this->getId(), (int)queue_.size());

            closed_ = true;
//            wCv_.notify_all();
//            rCv_.notify_all();
        }
    };
};


template <>
class Channel<void> : public Channel<std::nullptr_t>
{
public:
    explicit Channel(std::size_t capacity = 0)
        : Channel<std::nullptr_t>(capacity)
    {}
};

template <typename T>
using co_chan = Channel<T>;

} //namespace co
