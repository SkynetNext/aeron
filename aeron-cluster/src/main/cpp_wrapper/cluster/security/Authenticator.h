#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

namespace aeron {
namespace cluster {
namespace security {

/**
 * Interface for authenticating incoming cluster client connections.
 * This is a C++ translation of the Java Authenticator interface.
 */
class Authenticator {
public:
  virtual ~Authenticator() = default;

  /**
   * Called when a client requests to connect to the cluster.
   *
   * @param sessionId the client session id
   * @param encodedCredentials the encoded credentials provided by the client
   * @param nowMs the current time in milliseconds
   */
  virtual void
  onConnectRequest(std::int64_t sessionId,
                   const std::vector<std::uint8_t> &encodedCredentials,
                   std::int64_t nowMs) = 0;

  /**
   * Called when a client sends a challenge response.
   *
   * @param sessionId the client session id
   * @param encodedCredentials the encoded credentials provided by the client
   * @param nowMs the current time in milliseconds
   */
  virtual void
  onChallengeResponse(std::int64_t sessionId,
                      const std::vector<std::uint8_t> &encodedCredentials,
                      std::int64_t nowMs) = 0;

  /**
   * Called on each cluster tick to allow the authenticator to progress state.
   *
   * @param sessionId the client session id
   * @param nowMs the current time in milliseconds
   */
  virtual void onConnectedSession(std::int64_t sessionId,
                                  std::int64_t nowMs) = 0;

  /**
   * Called when a session is in CHALLENGED state and response publication is
   * connected.
   *
   * @param sessionId the client session id
   * @param nowMs the current time in milliseconds
   */
  virtual void onChallengedSession(std::int64_t sessionId,
                                   std::int64_t nowMs) = 0;

  /**
   * Called to poll the authenticator for actions that need to be taken.
   *
   * @param nowMs the current time in milliseconds
   * @return the number of actions taken
   */
  virtual std::int32_t poll(std::int64_t nowMs) = 0;
};

} // namespace security
} // namespace cluster
} // namespace aeron
