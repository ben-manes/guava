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

import javax.annotation.Nonnull;

/**
 * A multiple-producer / single-consumer buffer that rejects new elements if it is full or
 * fails spuriously due to contention. Unlike a queue and stack, a buffer does not guarantee an
 * ordering of elements in either FIFO or LIFO order.
 * <p>
 * Beware that it is the responsibility of the caller to ensure that a consumer has exclusive read
 * access to the buffer. This implementation does <em>not</em> include fail-fast behavior to guard
 * against incorrect consumer usage.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 * @param <E> the type of elements maintained by this buffer
 */
interface Buffer<E> {
  /** The maximum number of elements per buffer. */
  static final int BUFFER_SIZE = 16;

  /** Returns whether the buffer is full and should be drained. */
  boolean isFull();

  /**
   * Inserts the specified element into this buffer if it is possible to do so immediately without
   * violating capacity restrictions. The addition is allowed to fail spuriously if multiple
   * threads insert concurrently.
   *
   * @param e the element to add
   */
  void add(@Nonnull E e);

  /**
   * Drains the buffer, sending each element to the consumer for processing. The caller must ensure
   * that a consumer has exclusive read access to the buffer.
   *
   * @param consumer the action to perform on each element
   */
  void drainTo(@Nonnull Consumer<E> consumer);

  /** Clears the buffer. */
  void clear();

  @VisibleForTesting
  int size();

  @VisibleForTesting
  boolean isEmpty();

  @VisibleForTesting
  ImmutableList<E> copy();

  interface Consumer<E> {

    /**
     * Performs this operation on the given element.
     *
     * @param e the input element
     */
    void accept(E e);
  }
}
