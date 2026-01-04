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

namespace aeron { namespace cluster { namespace service
{

using namespace aeron::util;

/**
 * Used to terminate the Agent within a cluster in an expected/unexpected fashion.
 */
class ClusterTerminationException : public SourcedException
{
private:
    bool m_isExpected;

public:
    /**
     * Construct an exception used to terminate the cluster with isExpected() set to true.
     */
    ClusterTerminationException() :
        ClusterTerminationException(true)
    {
    }

    /**
     * Construct an exception used to terminate the cluster.
     *
     * @param isExpected true if the termination is expected, i.e. it was requested.
     */
    ClusterTerminationException(bool isExpected) :
        SourcedException(
            isExpected ? ExceptionCategory::EXCEPTION_CATEGORY_WARN : ExceptionCategory::EXCEPTION_CATEGORY_ERROR,
            isExpected ? "expected termination" : "unexpected termination",
            SOURCEINFO),
        m_isExpected(isExpected)
    {
    }

    /**
     * Whether the termination is expected.
     *
     * @return true if expected otherwise false.
     */
    bool isExpected() const
    {
        return m_isExpected;
    }
};

}}}

