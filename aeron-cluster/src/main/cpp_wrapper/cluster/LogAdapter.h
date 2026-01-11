#pragma once
#include "BufferBuilder.h"
#include "Image.h"
#include "Subscription.h"
#include <memory>

// ControlledFragmentHandler is replaced by ControlledPollAction in C++
// #include "concurrent/logbuffer/ControlledFragmentHandler.h"
#include "Context.h"
#include "concurrent/AtomicBuffer.h"
#include "concurrent/logbuffer/Header.h"
#include "generated/aeron_cluster_codecs/ClusterActionRequest.h"
#include "generated/aeron_cluster_codecs/MessageHeader.h"
#include "generated/aeron_cluster_codecs/NewLeadershipTermEvent.h"
#include "generated/aeron_cluster_codecs/SessionCloseEvent.h"
#include "generated/aeron_cluster_codecs/SessionMessageHeader.h"
#include "generated/aeron_cluster_codecs/SessionOpenEvent.h"
#include "generated/aeron_cluster_codecs/TimerEvent.h"
#include "util/Exceptions.h"

namespace aeron {
namespace cluster {
// Avoid using namespace directives in headers to prevent namespace pollution in
// nested namespaces
using namespace aeron::cluster::codecs; // Keep this for codec types

// Forward declaration - full definition needed for inline methods
class ConsensusModuleAgent;

class LogAdapter {
public:
  LogAdapter(ConsensusModuleAgent &consensusModuleAgent,
             std::int32_t fragmentLimit);

  std::int64_t disconnect(const exception_handler_t &errorHandler);

  void disconnect(const exception_handler_t &errorHandler,
                  std::int64_t maxLogPosition);

  std::shared_ptr<Subscription> subscription() const;

  ConsensusModuleAgent &consensusModuleAgent() const;

  std::int64_t position() const;

  std::int32_t poll(std::int64_t boundPosition);

  bool isImageClosed() const;

  bool isLogEndOfStream() const;

  bool isLogEndOfStreamAt(std::int64_t position) const;

  std::shared_ptr<Image> image() const;

  void image(std::shared_ptr<Image> image);

  void asyncRemoveDestination(const std::string &destination);

  ControlledPollAction onFragment(AtomicBuffer &buffer, std::int32_t offset,
                                  std::int32_t length, Header &header);

  // Call operator for use with Image::boundedControlledPoll
  ControlledPollAction operator()(AtomicBuffer &buffer, std::int32_t offset,
                                  std::int32_t length, Header &header) {
    return onFragment(buffer, offset, length, header);
  }

private:
  ControlledPollAction onMessage(AtomicBuffer &buffer, std::int32_t offset,
                                 std::int32_t length, Header &header);

  std::int32_t m_fragmentLimit;
  std::int64_t m_logPosition = 0;
  std::shared_ptr<Image> m_image;
  std::shared_ptr<Subscription> m_subscription; // Store subscription separately
                                                // since Image doesn't expose it
  ConsensusModuleAgent &m_consensusModuleAgent;
  BufferBuilder m_builder;
  MessageHeader m_messageHeaderDecoder;
  SessionOpenEvent m_sessionOpenEventDecoder;
  SessionCloseEvent m_sessionCloseEventDecoder;
  SessionMessageHeader m_sessionHeaderDecoder;
  TimerEvent m_timerEventDecoder;
  ClusterActionRequest m_clusterActionRequestDecoder;
  NewLeadershipTermEvent m_newLeadershipTermEventDecoder;
};

} // namespace cluster
} // namespace aeron
