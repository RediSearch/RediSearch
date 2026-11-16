/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once

#include <shared_mutex>
#include <array>

// A lockable resource, so that different mutex-like primitives (a shared_mutex, HNSW's own
// data guard, or nothing at all) can be composed under one RAII type below.
class Lockable {
public:
    virtual void lock() const = 0;
    virtual void unlock() const = 0;
    virtual ~Lockable() = default;
};

// Adapts a std::shared_mutex, locked/unlocked for shared (read) access, to Lockable.
class SharedMutexLockable : public Lockable {
    std::shared_mutex &guard;

public:
    explicit SharedMutexLockable(std::shared_mutex &guard) : guard(guard) {}
    void lock() const override { guard.lock_shared(); }
    void unlock() const override { guard.unlock_shared(); }
};

// RAII acquisition of zero, one or two Lockables, so callers can plug in whatever locking
// (or none) they need without repeating the lock/unlock bookkeeping.
class ScopedLocks {
    std::array<const Lockable *, 2> locks{};
    size_t count = 0;

public:
    ScopedLocks() = default;
    explicit ScopedLocks(const Lockable &a) : locks{&a, nullptr}, count(1) { a.lock(); }
    ScopedLocks(const Lockable &a, const Lockable &b) : locks{&a, &b}, count(2) {
        a.lock();
        b.lock();
    }
    ScopedLocks(ScopedLocks &&other) noexcept : locks(other.locks), count(other.count) {
        other.count = 0;
    }
    ScopedLocks(const ScopedLocks &) = delete;
    ScopedLocks &operator=(ScopedLocks &&) = delete;
    ~ScopedLocks() {
        // Unlock in reverse acquisition order.
        for (size_t i = count; i-- > 0;) {
            locks[i]->unlock();
        }
    }
};
