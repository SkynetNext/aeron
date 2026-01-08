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

#ifndef AERON_CONCURRENT_COUNTED_ERROR_HANDLER_H
#define AERON_CONCURRENT_COUNTED_ERROR_HANDLER_H

#include <exception>
#include <memory>

namespace aeron { namespace concurrent {

/**
 * Handler for errors that also counts them.
 * This is a C++ wrapper equivalent of org.agrona.concurrent.CountedErrorHandler.
 */
class CountedErrorHandler
{
public:
    virtual ~CountedErrorHandler() = default;

    /**
     * Handle an error.
     *
     * @param ex the exception to handle.
     */
    virtual void onError(const std::exception& ex) = 0;
};

}}

#endif // AERON_CONCURRENT_COUNTED_ERROR_HANDLER_H

