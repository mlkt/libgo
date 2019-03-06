namespace co
{

template <typename T>
ConditionVariableAny<T>::~ConditionVariableAny()
{
    std::unique_lock<lock_t> lock(lock_);

    checkIter_ = nullptr;
    for (;;) {
        Entry *entry = queue_.pop();
        if (!entry)
            break;

        if (!entry->noTimeoutLock.try_lock()) {
            continue;
        }

        if (entry->suspendEntry && entry->suspendEntry.IsExpire()) {
            continue;
        }

        assert(false);
        ThrowException("libgo.ConditionVariableAny<T> still have waiters when deconstructed.");
    }
}

template <typename T>
bool ConditionVariableAny<T>::notify_one(notify_operate op, T *pt)
{
    std::unique_lock<lock_t> lock(lock_);
    for (;;) {
        Entry *entry = queue_.pop();
        if (!entry)
            return false;

        IncursivePtr<Entry> sptr(entry);
        entry->DecrementRef();

        if (checkIter_ == entry)
            checkIter_ = nullptr;

        if (!entry->noTimeoutLock.try_lock()) {
            continue;
        }

        if (!entry->suspendEntry) {
            // 原生线程
            cv_.notify_one();
        } else if (!Processer::Wakeup(entry->suspendEntry)) {
            continue;
        }

        switch (op) {
            case notify_read:
                if (pt && entry->value)
                    *pt = std::move(*entry->value);
                break;

            case notify_write:
                if (pt && entry->value)
                    *entry->value = std::move(*pt);
                break;
            default:
                break;
        }

        return true;
    }
}

template <typename T>
size_t ConditionVariableAny<T>::notify_all()
{
    std::unique_lock<lock_t> lock(lock_);

    size_t n = 0;
    for (;;) {
        Entry *entry = queue_.pop();
        if (!entry)
            break;

        IncursivePtr<Entry> sptr(entry);
        entry->DecrementRef();

        if (checkIter_ == entry)
            checkIter_ = nullptr;

        if (!entry->noTimeoutLock.try_lock()) {
            continue;
        }

        if (!entry->suspendEntry) {
            // 原生线程
            cv_.notify_one();
            ++n;
            continue;
        }

        if (!Processer::Wakeup(entry->suspendEntry)) {
            continue;
        }

        ++n;
    }

    return n;
}

template <typename T>
bool ConditionVariableAny<T>::empty()
{
    std::unique_lock<lock_t> lock(lock_);
    return queue_.empty();
}

template <typename T>
void ConditionVariableAny<T>::AddWaiter(Entry * entry) {
    std::unique_lock<lock_t> lock(lock_);
    queue_.push(entry);

    if (queue_.size() < 8) {
        return ;
    }

    // 每次新增时, check大于1个entry的有效性, 即可防止泄露
    if (checkIter_ == nullptr)
        queue_.front(checkIter_);

    for (int i = 0; i < 2 && checkIter_ != nullptr; ++i) {
        Entry *entry = checkIter_;
        if (entry->suspendEntry && entry->suspendEntry.IsExpire()) {
            queue_.erase(checkIter_++);
            continue;
        }

        queue_.next(checkIter_, checkIter_);
    }
}

} //namespace co
