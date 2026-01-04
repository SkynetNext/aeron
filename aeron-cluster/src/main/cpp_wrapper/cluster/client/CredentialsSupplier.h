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

#include <functional>
#include <vector>
#include <cstdint>

namespace aeron { namespace cluster { namespace client
{

/**
 * Supplier of credentials for authentication with the cluster.
 * 
 * This class corresponds to io.aeron.security.CredentialsSupplier
 */
class CredentialsSupplier
{
public:
    /**
     * Type for encoded credentials supplier function.
     * Returns a pair of (credentials data pointer, credentials length).
     */
    using encoded_credentials_supplier_t = std::function<std::pair<const std::uint8_t *, std::uint32_t>()>;

    /**
     * Type for challenge response supplier function.
     * Takes encoded challenge and returns a pair of (response data pointer, response length).
     */
    using challenge_response_supplier_t = std::function<std::pair<const std::uint8_t *, std::uint32_t>(
        const std::uint8_t *, std::uint32_t)>;

    /**
     * Type for credentials free function.
     * Called to free credentials data when no longer needed.
     */
    using credentials_free_t = std::function<void(const std::uint8_t *, std::uint32_t)>;

    /**
     * Default encoded credentials supplier that returns empty credentials.
     */
    static inline std::pair<const std::uint8_t *, std::uint32_t> defaultEncodedCredentials()
    {
        return { nullptr, 0 };
    }

    /**
     * Default challenge response supplier that returns empty response.
     */
    static inline std::pair<const std::uint8_t *, std::uint32_t> defaultOnChallenge(
        const std::uint8_t *encodedChallenge, std::uint32_t challengeLength)
    {
        return { nullptr, 0 };
    }

    /**
     * Default credentials free function that deletes the credentials data.
     */
    static inline void defaultOnFree(const std::uint8_t *encodedCredentials, std::uint32_t credentialsLength)
    {
        delete[] encodedCredentials;
    }

    /**
     * Construct a CredentialsSupplier with default implementations.
     */
    CredentialsSupplier() :
        m_encodedCredentials(defaultEncodedCredentials),
        m_onChallenge([this](const std::uint8_t *challenge, std::uint32_t length)
        {
            return defaultOnChallenge(challenge, length);
        }),
        m_onFree(defaultOnFree)
    {
    }

    /**
     * Construct a CredentialsSupplier with custom implementations.
     */
    explicit CredentialsSupplier(
        encoded_credentials_supplier_t encodedCredentials,
        challenge_response_supplier_t onChallenge = [](const std::uint8_t *, std::uint32_t) { return defaultOnChallenge(nullptr, 0); },
        credentials_free_t onFree = defaultOnFree) :
        m_encodedCredentials(std::move(encodedCredentials)),
        m_onChallenge(std::move(onChallenge)),
        m_onFree(std::move(onFree))
    {
    }

    /**
     * Get encoded credentials for initial authentication.
     *
     * @return pair of (credentials data pointer, credentials length)
     */
    inline std::pair<const std::uint8_t *, std::uint32_t> encodedCredentials() const
    {
        return m_encodedCredentials();
    }

    /**
     * Get challenge response for authentication challenge.
     *
     * @param encodedChallenge the challenge data
     * @param challengeLength  the length of the challenge
     * @return pair of (response data pointer, response length)
     */
    inline std::pair<const std::uint8_t *, std::uint32_t> onChallenge(
        const std::uint8_t *encodedChallenge, std::uint32_t challengeLength) const
    {
        return m_onChallenge(encodedChallenge, challengeLength);
    }

    /**
     * Free credentials data when no longer needed.
     *
     * @param encodedCredentials the credentials data to free
     * @param credentialsLength the length of the credentials
     */
    inline void onFree(const std::uint8_t *encodedCredentials, std::uint32_t credentialsLength) const
    {
        m_onFree(encodedCredentials, credentialsLength);
    }

private:
    encoded_credentials_supplier_t m_encodedCredentials;
    challenge_response_supplier_t m_onChallenge;
    credentials_free_t m_onFree;
};

}}}

