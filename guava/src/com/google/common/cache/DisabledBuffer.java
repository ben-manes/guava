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

/**
 * @author ben.manes@gmail.com (Ben Manes)
 */
public enum DisabledBuffer implements Buffer<Object> {
  INSTANCE;

  /** Returns a no-op implementation. */
  @SuppressWarnings("unchecked")
  static <E> Buffer<E> get() {
    return (Buffer<E>) INSTANCE;
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public boolean isEmpty() {
    return true;
  }

  @Override
  public boolean isFull() {
    return false;
  }

  @Override
  public void add(Object e) {}

  @Override
  public void drainTo(Consumer<Object> consumer) {}

  @Override
  public void clear() {}

  @Override
  public ImmutableList<Object> copy() {
    return ImmutableList.of();
  }
}
