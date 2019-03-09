#pragma once
#include "../common/config.h"
#include "../scheduler/processer.h"
#include "../scheduler/scheduler.h"
#include "co_condition_variable.h"
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
        uint64_t dbg_mask_;

        struct Entry {
            int id;
            T* pvalue;
            T value;

            Entry() : pvalue(nullptr) {}
        };

//        typedef boost::lockfree::queue<Entry*, boost::lockfree::capacity<1>> wait_queue_t;
//        typedef boost::lockfree::queue<Entry*> wait_queue_t;
        typedef FastSteadyClock::time_point time_point_t;
//        typedef WaitQueue<Entry, bool, bool, time_point_t> wait_queue_t;
        typedef ConditionVariableAnyT<Entry> cond_t;
        cond_t wq_;
        cond_t rq_;

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
            , wq_(capacity ? capacity + 1 : (size_t)-1, &onNotify)
        {
            DebugPrint(dbg_mask_ & dbg_channel, "[id=%ld] Channel init. capacity=%lu", this->getId(), capacity);
        }
        
        static void onNotify(Entry & entry) {
//            entry.value = *entry.pvalue;
//            entry.pvalue = &entry.value;
        }

        // write
        bool Push(T t, bool bWait, FastSteadyClock::time_point deadline = FastSteadyClock::time_point{})
        {
            DebugPrint(dbg_channel, "[id=%ld] Push ->", this->getId());

            if (closed_) {
                DebugPrint(dbg_channel, "[id=%ld] Push by closed", this->getId());
                return false;
            }

            int spin = 0;

            size_t wait;
            for (;;) {
                wait = wait_.load(std::memory_order_relaxed);
                if (wait & readMask) {
                    if (!rq_.notify_one(
                                [&](Entry & entry)
                                {
                                    *entry.pvalue = t;
                                }))
                    {
                        if (++spin >= kSpinCount) {
                            spin = 0;
//                            printf("spin by push\n");
                            Processer::StaticCoYield();
                        }

                        if (capacity_ == 0) {
                            if (closed_) {
                                DebugPrint(dbg_channel, "[id=%ld] Push failed by closed.", this->getId());
                                return false;
                            } else if (!bWait) {
                                DebugPrint(dbg_channel, "[id=%ld] TryPush failed.", this->getId());
                                return false;
                            }
                        }
                        continue;
                    }
                    DebugPrint(dbg_channel, "[id=%ld] Push Notify.", this->getId());

                    wait_ -= read1;
                    return true;
                } else if (capacity_ == 0) {
                    if (closed_) {
                        DebugPrint(dbg_channel, "[id=%ld] Push failed by closed.", this->getId());
                        return false;
                    } else if (!bWait) {
                        DebugPrint(dbg_channel, "[id=%ld] TryPush failed.", this->getId());
                        return false;
                    }
                }

                if (wait_.compare_exchange_weak(wait, wait + write1,
                        std::memory_order_acq_rel, std::memory_order_relaxed))
                    break;
            }

            FakeLock lock;
            Entry entry;
            entry.id = GetCurrentCoroID();
            entry.value = t;
            auto cond = [&](size_t size) -> typename cond_t::CondRet {
                typename cond_t::CondRet ret{true, true};
                if (closed_) {
                    ret.canQueue = false;
                    return ret;
                }

                if (size < capacity_) {
                    ret.needWait = false;
                    DebugPrint(dbg_channel, "[id=%ld] Push no wait.", this->getId());
                    return ret;
                }

                if (!bWait) {
                    ret.canQueue = false;
                    return ret;
                }

                DebugPrint(dbg_channel, "[id=%ld] Push wait.", this->getId());
                return ret;
            };
            typename cond_t::cv_status cv_status;
            if (deadline == time_point_t())
                cv_status = wq_.wait(lock, entry, cond);
            else
                cv_status = wq_.wait_util(lock, deadline, entry, cond);

            switch ((int)cv_status) {
                case (int)cond_t::cv_status::no_timeout:
                    if (closed_) {
                        DebugPrint(dbg_channel, "[id=%ld] Push failed by closed.", this->getId());
                        return false;
                    }

                    DebugPrint(dbg_channel, "[id=%ld] Push complete.", this->getId());
                    return true;

                case (int)cond_t::cv_status::timeout:
                    DebugPrint(dbg_channel, "[id=%ld] Push timeout.", this->getId());
                    wait_ -= write1;
                    return false;

                case (int)cond_t::cv_status::no_queued:
                    if (closed_)
                        DebugPrint(dbg_channel, "[id=%ld] Push failed by closed.", this->getId());
                    else if (!bWait)
                        DebugPrint(dbg_channel, "[id=%ld] TryPush failed.", this->getId());
                    else
                        DebugPrint(dbg_channel, "[id=%ld] Push failed.", this->getId());
                    wait_ -= write1;
                    return false;

                default:
                    assert(false);
                    return false;
            }

            return false;
        }

        // read
        bool Pop(T & t, bool bWait, FastSteadyClock::time_point deadline = FastSteadyClock::time_point{})
        {
            DebugPrint(dbg_channel, "[id=%ld] Pop ->", this->getId());

            int spin = 0;

            size_t wait;
            for (;;) {
                wait = wait_.load(std::memory_order_relaxed);
                if (wait & writeMask) {

                    if (!wq_.notify_one(
                                [&](Entry & entry)
                                {
                                    t = entry.value;
                                }))
                    {
                        if (++spin >= kSpinCount) {
                            spin = 0;
                            Processer::StaticCoYield();
                        }

                        if (closed_) {
                            DebugPrint(dbg_channel, "[id=%ld] Pop failed by closed.", this->getId());
                            return false;
                        } else if (!bWait) {
                            DebugPrint(dbg_channel, "[id=%ld] TryPop failed.", this->getId());
                            return false;
                        }
                        continue;
                    }

                    DebugPrint(dbg_channel, "[id=%ld] Pop Notify.", this->getId());

                    wait_ -= write1;
                    return true;
                } else {
                    if (closed_) {
                        DebugPrint(dbg_channel, "[id=%ld] Pop failed by closed.", this->getId());
                        return false;
                    } else if (!bWait) {
                        DebugPrint(dbg_channel, "[id=%ld] TryPop failed.", this->getId());
                        return false;
                    }
                }

                if (wait_.compare_exchange_weak(wait, wait + read1,
                            std::memory_order_acq_rel, std::memory_order_relaxed))
                    break;
            }

            FakeLock lock;
            Entry entry;
            entry.id = GetCurrentCoroID();
            entry.pvalue = &t;
            auto cond = [&](size_t size) -> typename cond_t::CondRet {
                typename cond_t::CondRet ret{true, true};
                if (closed_) {
                    ret.canQueue = false;
                    return ret;
                }

                DebugPrint(dbg_channel, "[id=%ld] Pop wait.", this->getId());
                return ret;
            };
            typename cond_t::cv_status cv_status;
            if (deadline == time_point_t())
                cv_status = rq_.wait(lock, entry, cond);
            else
                cv_status = rq_.wait_util(lock, deadline, entry, cond);

            switch ((int)cv_status) {
                case (int)cond_t::cv_status::no_timeout:
                    if (closed_) {
                        DebugPrint(dbg_channel, "[id=%ld] Pop failed by closed.", this->getId());
                        return false;
                    }

                    DebugPrint(dbg_channel, "[id=%ld] Pop complete.", this->getId());
                    return true;

                case (int)cond_t::cv_status::timeout:
                    DebugPrint(dbg_channel, "[id=%ld] Pop timeout.", this->getId());
                    wait_ -= read1;
                    return false;

                case (int)cond_t::cv_status::no_queued:
                    if (closed_)
                        DebugPrint(dbg_channel, "[id=%ld] Pop failed by closed.", this->getId());
                    else
                        DebugPrint(dbg_channel, "[id=%ld] Pop failed.", this->getId());
                    wait_ -= read1;
                    return false;

                default:
                    assert(false);
                    return false;
            }

            return false;
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
            return std::min<size_t>(capacity_, wq_.size());
        }

        void Close()
        {
            std::unique_lock<lock_t> lock(lock_);
            if (closed_) return ;

            DebugPrint(dbg_mask_ & dbg_channel, "[id=%ld] Channel Closed. size=%d", this->getId(), (int)size());

            closed_ = true;
            rq_.notify_all();
            wq_.notify_all();
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
