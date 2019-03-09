#pragma once
#include "../common/config.h"
#include "../common/spinlock.h"
#include <mutex>

namespace co
{

struct WaitQueueHook
{
    WaitQueueHook* next = nullptr;
};

template <typename T>
class WaitQueue
{
    static_assert(std::is_base_of<WaitQueueHook, T>::value, "");

public:
    typedef std::function<bool(T*)> Functor;
    typedef std::function<bool(T*, size_t)> OnPushFunctor;
    typedef std::mutex lock_t;
    lock_t lock_;
    WaitQueueHook dummy_;
    WaitQueueHook* head_;
    WaitQueueHook* tail_;
    WaitQueueHook* check_;
    Functor checkFunctor_;
    WaitQueueHook* pos_;
    const size_t posDistance_;
    volatile size_t count_;
    Functor posFunctor_;
    OnPushFunctor onPush_;

    explicit WaitQueue(Functor checkFunctor = NULL,
            OnPushFunctor onPush = NULL,
            size_t posD = -1, Functor const& posFunctor = NULL)
        : posDistance_(posD)
    {
        head_ = &dummy_;
        tail_ = &dummy_;
        check_ = &dummy_;
        checkFunctor_ = checkFunctor;
        pos_ = nullptr;
        posFunctor_ = posFunctor;
        onPush_ = onPush;
        count_ = 0;
    }

    ~WaitQueue() {
        check_ = pos_ = nullptr;
        count_ = 0;
        T* ptr = nullptr;
        while (pop(ptr))
            delete ptr;
    }

    size_t empty()
    {
        return count_ == 0;
    }

    size_t size()
    {
        return count_;
    }

    bool push(T* ptr)
    {
        std::unique_lock<lock_t> lock(lock_);
        bool ret = true;
        if (onPush_)
            ret = onPush_(ptr, count_);

        tail_->next = ptr;
        ptr->next = nullptr;
        tail_ = ptr;
        if (++count_ == posDistance_) {
            pos_ = ptr;
        }

        // check
        if (!checkFunctor_) return ret;

        if (!check_->next)
            check_ = head_;

        for (int i = 0; check_->next && i < 2; ++i) {
            if (!checkFunctor_(static_cast<T*>(check_->next))) {
                if (pos_ == check_->next)
                    pos_ = pos_->next;

                if (tail_ == check_->next)
                    tail_ = check_;

                auto temp = check_->next;
                check_->next = check_->next->next;
                temp->next = nullptr;
                delete temp;
                continue;
            }

            check_ = check_->next;
        }
        return ret;
    }

    bool pop(T* & ptr)
    {
        std::unique_lock<lock_t> lock(lock_);
        if (head_ == tail_) return false;

        ptr = static_cast<T*>(head_->next);
        if (tail_ == head_->next) tail_ = head_;
        head_->next = head_->next->next;
        ptr->next = nullptr;

        for (;;) {
            if (!pos_ || !posFunctor_) break;

            bool ok = posFunctor_(static_cast<T*>(pos_));
            pos_ = pos_->next;
            if (ok) break;
        }
        return true;
    }

    bool tryPop(T* & ptr)
    {
        if (!count_) return false;
        return pop(ptr);
    }
};

} // namespace co