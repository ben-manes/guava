/*
 * Copyright (C) 2015 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.common.cache;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author ben.manes@gmail.com (Ben Manes)
 */
final class BoundedBuffer<E> implements Buffer<E> {
  /** Mask value for indexing into the ring buffer. */
  static final int BUFFER_MASK = BUFFER_SIZE - 1;

  static final Consumer<Object> NULL_CONSUMER = new Consumer<Object>() {
    @Override public void accept(Object e) {}
  };

  final AtomicLong readCounter;
  final AtomicLong writeCounter;
  final AtomicReference<E>[] buffer;

  volatile boolean full;

  @SuppressWarnings({"unchecked", "cast", "rawtypes"})
  public BoundedBuffer() {
    readCounter = new AtomicLong();
    writeCounter = new AtomicLong();
    buffer = new AtomicReference[BUFFER_SIZE];
    for (int i = 0; i < BUFFER_SIZE; i++) {
      buffer[i] = new AtomicReference<E>();
    }
  }

  @Override
  public int size() {
    return (int) (writeCounter.get() - readCounter.get());
  }

  @Override
  public boolean isEmpty() {
    return size() == 0;
  }

  @Override
  public boolean isFull() {
    return full;
  }

  @Override
  public void add(E e) {
    long head = readCounter.get();
    long tail = writeCounter.get();
    long size = (tail - head);

    if ((size < BUFFER_SIZE) && writeCounter.compareAndSet(tail, tail + 1)) {
      int index = (int) (tail & BUFFER_MASK);
      buffer[index].lazySet(e);
      if (size == BUFFER_MASK) {
        full = true;
      }
    }
  }

  @Override
  public void drainTo(Consumer<E> consumer) {
    long head = readCounter.get();
    long tail = writeCounter.get();
    long size = (tail - head);
    if (size == 0) {
      full = false;
      return;
    }
    do {
      int index = (int) (head & BUFFER_MASK);
      AtomicReference<E> slot = buffer[index];
      E e = slot.get();
      if (e == null) {
        // not published yet
        break;
      }
      slot.lazySet(null);
      consumer.accept(e);
      head++;
    } while (head != tail);
    readCounter.lazySet(head);
    full = false;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void clear() {
    drainTo((Consumer<E>) NULL_CONSUMER);
  }

  @Override
  @VisibleForTesting
  public ImmutableList<E> copy() {
    ImmutableList.Builder<E> builder = ImmutableList.builder();
    for (long i = readCounter.get(); i < writeCounter.get(); i++) {
      int index = (int) (i & BUFFER_MASK);
      AtomicReference<E> slot = buffer[index];
      E e = slot.get();
      if (e == null) {
        // not published yet
        break;
      }
      builder.add(e);
    }
    return builder.build();
  }
}
