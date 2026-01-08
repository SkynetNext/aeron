/*
 * Copyright 2014-2025 Justin Zhu.
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
#pragma once

#include "util/Exceptions.h"

namespace aeron { namespace cluster { namespace client
{

using namespace aeron::util;

/**
 * A means to capture a Cluster event of significance that does not require a stack trace, so it can be lighter-weight
 * and take up less space in a DistinctErrorLog.
 */
class ClusterEvent : public SourcedException
{
public:
    /**
     * Cluster event with provided message and ExceptionCategory::EXCEPTION_CATEGORY_WARN.
     *
     * @param message to detail the event.
     */
    ClusterEvent(const std::string& message) :
        SourcedException(ExceptionCategory::EXCEPTION_CATEGORY_WARN, message, SOURCEINFO)
    {
    }

    /**
     * Cluster event with provided message and ExceptionCategory.
     *
     * @param message  to detail the event.
     * @param category of the event.
     */
    ClusterEvent(const std::string& message, ExceptionCategory category) :
        SourcedException(category, message, SOURCEINFO)
    {
    }

    /**
     * Cluster event with provided message and source location.
     *
     * @param message  to detail the event.
     * @param function the function name.
     * @param file     the file name.
     * @param line     the line number.
     */
    ClusterEvent(const std::string& message, const char* function, const char* file, int line) :
        SourcedException(ExceptionCategory::EXCEPTION_CATEGORY_WARN, message, function, file, line)
    {
    }

    /**
     * Cluster event with provided message, category, and source location.
     *
     * @param message  to detail the event.
     * @param category of the event.
     * @param function the function name.
     * @param file     the file name.
     * @param line     the line number.
     */
    ClusterEvent(const std::string& message, ExceptionCategory category, const char* function, const char* file, int line) :
        SourcedException(category, message, function, file, line)
    {
    }
};

}}}

