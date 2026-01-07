/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef AERON_UTIL_EXPANDABLE_ARRAY_BUFFER_H
#define AERON_UTIL_EXPANDABLE_ARRAY_BUFFER_H

#include <vector>
#include <cstdint>
#include <cstddef>

namespace aeron { namespace util {

/**
 * Expandable buffer implementation using std::vector.
 * This is a C++ wrapper equivalent of org.agrona.ExpandableArrayBuffer.
 */
class ExpandableArrayBuffer
{
public:
    /**
     * Construct an ExpandableArrayBuffer with an initial capacity.
     *
     * @param initialCapacity the initial capacity of the buffer.
     */
    explicit ExpandableArrayBuffer(std::size_t initialCapacity = 1024) : m_buffer(initialCapacity) {}

    /**
     * Get a pointer to the underlying buffer data.
     *
     * @return a pointer to the buffer data.
     */
    std::uint8_t* buffer() { return m_buffer.data(); }

    /**
     * Get a const pointer to the underlying buffer data.
     *
     * @return a const pointer to the buffer data.
     */
    const std::uint8_t* buffer() const { return m_buffer.data(); }

    /**
     * Get the current capacity of the buffer.
     *
     * @return the current capacity.
     */
    std::size_t capacity() const { return m_buffer.capacity(); }

    /**
     * Ensure the buffer has at least the specified capacity.
     * If the current capacity is less than the requested capacity, the buffer will be resized.
     *
     * @param capacity the minimum capacity required.
     */
    void ensureCapacity(std::size_t capacity)
    {
        if (m_buffer.capacity() < capacity)
        {
            m_buffer.resize(capacity);
        }
    }

private:
    std::vector<std::uint8_t> m_buffer;
};

}}

#endif // AERON_UTIL_EXPANDABLE_ARRAY_BUFFER_H

