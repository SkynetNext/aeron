#pragma once

#include <cstdint>
#include <vector>

namespace aeron {
namespace cluster {
namespace security {

/**
 * Interface for authorising cluster operations.
 * This is a C++ translation of the Java AuthorisationService interface.
 */
class AuthorisationService {
public:
  virtual ~AuthorisationService() = default;

  /**
   * Check if a session is authorised to perform an operation.
   *
   * @param protocolId the protocol identifier
   * @param actionId the action identifier
   * @param type the type of action
   * @param encodedPrincipal the encoded principal/credentials
   * @return true if authorised, false otherwise
   */
  virtual bool
  isAuthorised(std::int32_t protocolId, std::int32_t actionId,
               std::int32_t type,
               const std::vector<std::uint8_t> &encodedPrincipal) = 0;
};

/**
 * Default implementation that allows all operations.
 */
class AllowAllAuthorisationService : public AuthorisationService {
public:
  bool isAuthorised(
      std::int32_t /*protocolId*/, std::int32_t /*actionId*/,
      std::int32_t /*type*/,
      const std::vector<std::uint8_t> & /*encodedPrincipal*/) override {
    return true;
  }
};

} // namespace security
} // namespace cluster
} // namespace aeron
