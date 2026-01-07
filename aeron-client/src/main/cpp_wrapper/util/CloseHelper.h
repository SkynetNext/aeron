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

#ifndef AERON_UTIL_CLOSE_HELPER_H
#define AERON_UTIL_CLOSE_HELPER_H

#include <memory>
#include <functional>
#include "Exceptions.h"

namespace aeron { namespace util {

/**
 * Helper class for safely closing resources. This is a C++ wrapper equivalent of org.agrona.CloseHelper.
 */
class CloseHelper
{
public:
    /**
     * Close a resource with error handling.
     *
     * @param errorHandler the error handler to use if an exception occurs.
     * @param resource the resource to close (std::shared_ptr).
     */
    template<typename T>
    static void close(const exception_handler_t& errorHandler, std::shared_ptr<T>& resource)
    {
        if (resource)
        {
            try
            {
                resource.reset();
            }
            catch (const std::exception& ex)
            {
                errorHandler(ex);
            }
        }
    }

    /**
     * Close a resource without error handling.
     *
     * @param resource the resource to close (std::shared_ptr).
     */
    template<typename T>
    static void close(std::shared_ptr<T>& resource)
    {
        resource.reset();
    }

    /**
     * Close a raw pointer resource with error handling.
     *
     * @param errorHandler the error handler to use if an exception occurs.
     * @param resource the resource to close (raw pointer).
     */
    template<typename T>
    static void close(const exception_handler_t& errorHandler, T* resource)
    {
        if (resource)
        {
            try
            {
                delete resource;
            }
            catch (const std::exception& ex)
            {
                errorHandler(ex);
            }
        }
    }

    /**
     * Close a raw pointer resource without error handling.
     *
     * @param resource the resource to close (raw pointer).
     */
    template<typename T>
    static void close(T* resource)
    {
        if (resource)
        {
            delete resource;
        }
    }
};

}}

#endif // AERON_UTIL_CLOSE_HELPER_H

