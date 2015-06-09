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

import com.google.common.collect.ImmutableList;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * @author ben.manes@gmail.com (Ben Manes)
 */
final class BoundedBuffer<E> implements Buffer<E> {
  // Assume 4-byte references and 64-byte cache line (16 elements per line)
  static final int SPACED_SIZE = BUFFER_SIZE << 4;
  static final int SPACED_MASK = SPACED_SIZE - 1;
  static final int OFFSET = 16;

  static final Consumer<Object> NULL_CONSUMER = new Consumer<Object>() {
    @Override public void accept(Object e) {}
  };

  final AtomicLong readCounter;
  final AtomicLong writeCounter;
  final AtomicReferenceArray<E> buffer;

  volatile boolean full;

  @SuppressWarnings({"unchecked", "cast", "rawtypes"})
  public BoundedBuffer() {
    readCounter = new AtomicLong();
    writeCounter = new AtomicLong();
    buffer = new AtomicReferenceArray<E>(SPACED_SIZE);
  }

  @Override
  public int size() {
    return (int) (writeCounter.get() - readCounter.get()) / OFFSET;
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

    if ((size < SPACED_SIZE) && writeCounter.compareAndSet(tail, tail + OFFSET)) {
      int index = (int) (tail & SPACED_MASK);
      buffer.lazySet(index, e);
      if (size == SPACED_MASK) {
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
      int index = (int) (head & SPACED_MASK);
      E e = buffer.get(index);
      if (e == null) {
        // not published yet
        break;
      }
      buffer.lazySet(index, null);
      consumer.accept(e);
      head += OFFSET;
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
  public ImmutableList<E> copy() {
    ImmutableList.Builder<E> builder = ImmutableList.builder();
    for (long i = readCounter.get(); i < writeCounter.get(); i += OFFSET) {
      int index = (int) (i & SPACED_MASK);
      E e = buffer.get(index);
      if (e == null) {
        // not published yet
        break;
      }
      builder.add(e);
    }
    return builder.build();
  }
}
