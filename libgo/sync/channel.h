#pragma once
#include "../common/config.h"
#include "../scheduler/processer.h"
#include "../scheduler/scheduler.h"
#include "co_condition_variable.h"
#include <boost/lockfree/queue.hpp>
#include "co_mutex.h"
#include "wait_queue.h"

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
//        typedef std::mutex lock_t;
        typedef co_mutex lock_t;
        lock_t lock_;
        const std::size_t capacity_;
        bool closed_;
        std::deque<T> queue_;
        uint64_t dbg_mask_;

        struct Entry : public WaitQueueHook {
            Processer::SuspendEntry entry;
            T* pvalue;
            T value;
            bool waiting;

            Entry() : pvalue(nullptr), waiting(true) {}
            Entry(Processer::SuspendEntry && e, T* p) : entry(e), pvalue(p) {}
        };

//        typedef boost::lockfree::queue<Entry*, boost::lockfree::capacity<1>> wait_queue_t;
//        typedef boost::lockfree::queue<Entry*> wait_queue_t;
        typedef WaitQueue<Entry> wait_queue_t;
        wait_queue_t wq_;
        wait_queue_t rq_;

        // number of read wait << 32 | number of write wait
        atomic_t<size_t> wait_ {0};
        static const size_t write1 = 1;
        static const size_t writeMask = 0xffffffff;
        static const size_t read1 = ((size_t)1) << 32;
        static const size_t readMask = 0xffffffff00000000;
        static const int kSpinCount = 4000;

    public:
        explicit ChannelImpl(std::size_t capacity)
            : capacity_(capacity), closed_(false), dbg_mask_(dbg_all)
            , wq_(NULL, [this](Entry* entry, size_t size){ return this->onPush(entry, size); }
                    , capacity ? capacity + 1 : (size_t)-1, &onNotify)
            , rq_(NULL, NULL, (size_t)-1, NULL)
        {
            DebugPrint(dbg_mask_ & dbg_channel, "[id=%ld] Channel init. capacity=%lu", this->getId(), capacity);
        }

        static bool check(Entry* entry) {
            if (!entry->waiting) return true;
            return !entry->entry.IsExpire();
        }
        
        bool onPush(Entry* entry, size_t size) {
            if (size < capacity_) {
                entry->waiting = false;
                entry->value = *entry->pvalue;
                return false;
            } else {
                entry->entry = Processer::Suspend();
                return true;
            }
        }

        static bool onNotify(Entry* entry) {
            assert(entry->waiting);
            if (Processer::Wakeup(entry->entry, [=]{ entry->value = *entry->pvalue; })) {
                entry->waiting = false;
                return true;
            }
            return false;
        }

        // write
        bool Push(T t, bool bWait, FastSteadyClock::time_point deadline = FastSteadyClock::time_point{})
        {
            DebugPrint(dbg_channel, "[id=%ld] Push ->", this->getId());

            T* pt = &t;
            int spin = 0;

            size_t wait;
            for (;;) {
                wait = wait_.load(std::memory_order_relaxed);
                if (wait & readMask) {
                    Entry* entry = nullptr;
                    if (!rq_.pop(entry)) {
                        if (++spin >= kSpinCount) {
                            spin = 0;
//                            printf("spin by push\n");
                            Processer::StaticCoYield();
                        }
                        continue;
                    }
                    DebugPrint(dbg_channel, "[id=%ld] Push Notify.", this->getId());
                    std::unique_ptr<Entry> ep(entry);

                    if (Processer::Wakeup(entry->entry, [=]{ *entry->pvalue = *pt; })) {
                        wait_ -= read1;
                        return true;
                    }
                    continue;
                }

                if (wait_.compare_exchange_weak(wait, wait + write1,
                        std::memory_order_acq_rel, std::memory_order_relaxed))
                    break;
            }

            Entry *entry = new Entry();
            entry->pvalue = pt;
            if (wq_.push(entry)) {
                DebugPrint(dbg_channel, "[id=%ld] Push waiting.", this->getId());
                Processer::StaticCoYield();
                DebugPrint(dbg_channel, "[id=%ld] Push complete.", this->getId());
            } else {
                DebugPrint(dbg_channel, "[id=%ld] Push no wait.", this->getId());
            }
            return true;
        }

        // read
        bool Pop(T & t, bool bWait, FastSteadyClock::time_point deadline = FastSteadyClock::time_point{})
        {
            DebugPrint(dbg_channel, "[id=%ld] Pop ->", this->getId());

            T* pt = &t;
            int spin = 0;

            size_t wait;
            for (;;) {
                wait = wait_.load(std::memory_order_relaxed);
                if (wait & writeMask) {
                    Entry* entry = nullptr;
                    if (!wq_.pop(entry)) {
                        if (++spin >= kSpinCount) {
                            spin = 0;
                            Processer::StaticCoYield();
                        }
                        continue;
                    }
                    std::unique_ptr<Entry> ep(entry);
                    DebugPrint(dbg_channel, "[id=%ld] Pop Notify. waiting=%d", this->getId(), (int)entry->waiting);

                    if (!entry->waiting) {
                        *pt = std::move(entry->value);
                        wait_ -= write1;
                        return true;
                    }

                    if (Processer::Wakeup(entry->entry, [=]{ *pt = *entry->pvalue; })) {
                        wait_ -= write1;
                        return true;
                    }
                }

                if (wait_.compare_exchange_weak(wait, wait + read1,
                            std::memory_order_acq_rel, std::memory_order_relaxed))
                    break;
            }

            Entry *entry = new Entry();
            entry->entry = Processer::Suspend();
            entry->pvalue = pt;
            rq_.push(entry);
            DebugPrint(dbg_channel, "[id=%ld] Pop waiting.", this->getId());
            Processer::StaticCoYield();
            DebugPrint(dbg_channel, "[id=%ld] Pop complete.", this->getId());
            return true;
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
            return wq_.empty();
        }

        std::size_t Size()
        {
            return std::min(capacity_, wq_.size());
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
