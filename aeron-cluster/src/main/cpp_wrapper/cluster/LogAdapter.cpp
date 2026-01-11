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

#include "LogAdapter.h"
#include "ConsensusModuleAgent.h"
#include "concurrent/AtomicBuffer.h"
#include "concurrent/logbuffer/Header.h"
#include "service/ClusterClock.h"
#include "util/BitUtil.h"
#include "util/CloseHelper.h"

namespace aeron {
namespace cluster {

// Implementation
LogAdapter::LogAdapter(ConsensusModuleAgent &consensusModuleAgent,
                       std::int32_t fragmentLimit)
    : m_fragmentLimit(fragmentLimit),
      m_consensusModuleAgent(consensusModuleAgent) {}

std::int64_t LogAdapter::disconnect(const exception_handler_t &errorHandler) {
  std::int64_t registrationId = NULL_VALUE;

  if (m_image) {
    m_logPosition = m_image->position();
    CloseHelper::close(errorHandler, m_subscription);
    registrationId =
        m_subscription ? m_subscription->registrationId() : NULL_VALUE;
    m_image.reset();
  }

  return registrationId;
}

// Implementation moved to ConsensusModuleAgent.h to avoid circular dependency
//  void LogAdapter::disconnect(const exception_handler_t& errorHandler,
// std::int64_t maxLogPosition)

std::shared_ptr<Subscription> LogAdapter::subscription() const {
  return m_subscription;
}

ConsensusModuleAgent &LogAdapter::consensusModuleAgent() const {
  return m_consensusModuleAgent;
}

std::int64_t LogAdapter::position() const {
  if (!m_image) {
    return m_logPosition;
  }

  return m_image->position();
}

std::int32_t LogAdapter::poll(std::int64_t boundPosition) {
  if (!m_image) {
    return 0;
  }

  return m_image->boundedControlledPoll(*this, boundPosition, m_fragmentLimit);
}

bool LogAdapter::isImageClosed() const {
  return !m_image || m_image->isClosed();
}

bool LogAdapter::isLogEndOfStream() const {
  return m_image && m_image->isEndOfStream();
}

bool LogAdapter::isLogEndOfStreamAt(std::int64_t position) const {
  return m_image && position == m_image->endOfStreamPosition();
}

std::shared_ptr<Image> LogAdapter::image() const { return m_image; }

void LogAdapter::image(std::shared_ptr<Image> image) {
  if (m_image) {
    m_logPosition = m_image->position();
  }

  m_image = image;
  // Note: In C++, Image doesn't expose subscription(), so we need to get it
  // from elsewhere This will need to be set separately or retrieved from the
  // ConsensusModuleAgent For now, we'll leave m_subscription as is and it
  // should be set when image is set
}

void LogAdapter::asyncRemoveDestination(const std::string &destination) {
  if (m_subscription && !m_subscription->isClosed()) {
    m_subscription->removeDestination(destination);
  }
}

ControlledPollAction LogAdapter::onFragment(AtomicBuffer &buffer,
                                            std::int32_t offset,
                                            std::int32_t length,
                                            Header &header) {
  ControlledPollAction action = ControlledPollAction::CONTINUE;
  const std::uint8_t flags = header.flags();

  if ((flags & FrameDescriptor::UNFRAGMENTED) ==
      FrameDescriptor::UNFRAGMENTED) {
    action = onMessage(buffer, offset, length, header);
  } else if ((flags & FrameDescriptor::BEGIN_FRAG) ==
             FrameDescriptor::BEGIN_FRAG) {
    m_builder.reset().captureHeader(header).append(buffer, offset, length);
    m_builder.nextTermOffset(
        BitUtil::align(offset + length + DataFrameHeader::LENGTH,
                       FrameDescriptor::FRAME_ALIGNMENT));
  } else if (offset == m_builder.nextTermOffset()) {
    const std::uint32_t limit = m_builder.limit();

    m_builder.append(buffer, offset, length);

    if ((flags & FrameDescriptor::END_FRAG) == FrameDescriptor::END_FRAG) {
      AtomicBuffer assembledBuffer(m_builder.buffer(), m_builder.limit());
      action = onMessage(assembledBuffer, 0, m_builder.limit(), header);

      if (ControlledPollAction::ABORT == action) {
        m_builder.limit(limit);
      } else {
        m_builder.reset();
      }
    } else {
      m_builder.nextTermOffset(
          BitUtil::align(offset + length + DataFrameHeader::LENGTH,
                         FrameDescriptor::FRAME_ALIGNMENT));
    }
  } else {
    m_builder.reset();
  }

  return action;
}

ControlledPollAction LogAdapter::onMessage(AtomicBuffer &buffer,
                                           std::int32_t offset,
                                           std::int32_t length,
                                           Header &header) {
  m_messageHeaderDecoder.wrap(reinterpret_cast<char *>(buffer.buffer()), offset,
                              MessageHeader::sbeSchemaVersion(),
                              buffer.capacity());

  const std::uint16_t schemaId = m_messageHeaderDecoder.schemaId();
  const std::uint16_t expectedSchemaId = MessageHeader::sbeSchemaId();
  if (schemaId != expectedSchemaId) {
    return m_consensusModuleAgent.onReplayExtensionMessage(
        m_messageHeaderDecoder.blockLength(),
        m_messageHeaderDecoder.templateId(), schemaId,
        m_messageHeaderDecoder.version(), buffer, offset, length, header);
  }

  const std::int32_t templateId = m_messageHeaderDecoder.templateId();
  const std::int32_t actingVersion = m_messageHeaderDecoder.version();
  const std::int32_t actingBlockLength = m_messageHeaderDecoder.blockLength();

  switch (templateId) {
  case SessionMessageHeader::SBE_TEMPLATE_ID: {
    m_sessionHeaderDecoder.wrapForDecode(
        reinterpret_cast<char *>(buffer.buffer()),
        offset + MessageHeader::encodedLength(), actingBlockLength,
        actingVersion, buffer.capacity());

    m_consensusModuleAgent.onReplaySessionMessage(
        m_sessionHeaderDecoder.clusterSessionId(),
        m_sessionHeaderDecoder.timestamp());

    return ControlledPollAction::CONTINUE;
  }

  case TimerEvent::SBE_TEMPLATE_ID: {
    m_timerEventDecoder.wrapForDecode(reinterpret_cast<char *>(buffer.buffer()),
                                      offset + MessageHeader::encodedLength(),
                                      actingBlockLength, actingVersion,
                                      buffer.capacity());

    m_consensusModuleAgent.onReplayTimerEvent(
        m_timerEventDecoder.correlationId());
    break;
  }

  case SessionOpenEvent::SBE_TEMPLATE_ID: {
    m_sessionOpenEventDecoder.wrapForDecode(
        reinterpret_cast<char *>(buffer.buffer()),
        offset + MessageHeader::encodedLength(), actingBlockLength,
        actingVersion, buffer.capacity());

    m_consensusModuleAgent.onReplaySessionOpen(
        header.position(), m_sessionOpenEventDecoder.correlationId(),
        m_sessionOpenEventDecoder.clusterSessionId(),
        m_sessionOpenEventDecoder.timestamp(),
        m_sessionOpenEventDecoder.responseStreamId(),
        m_sessionOpenEventDecoder.responseChannel());
    break;
  }

  case SessionCloseEvent::SBE_TEMPLATE_ID: {
    m_sessionCloseEventDecoder.wrapForDecode(
        reinterpret_cast<char *>(buffer.buffer()),
        offset + MessageHeader::encodedLength(), actingBlockLength,
        actingVersion, buffer.capacity());

    m_consensusModuleAgent.onReplaySessionClose(
        m_sessionCloseEventDecoder.clusterSessionId(),
        m_sessionCloseEventDecoder.closeReason());
    break;
  }

  case ClusterActionRequest::SBE_TEMPLATE_ID: {
    m_clusterActionRequestDecoder.wrapForDecode(
        reinterpret_cast<char *>(buffer.buffer()),
        offset + MessageHeader::encodedLength(), actingBlockLength,
        actingVersion, buffer.capacity());

    const std::int32_t flags = (m_clusterActionRequestDecoder.flags() !=
                                ClusterActionRequest::flagsNullValue())
                                   ? m_clusterActionRequestDecoder.flags()
                                   : 0; // CLUSTER_ACTION_FLAGS_DEFAULT = 0

    m_consensusModuleAgent.onReplayClusterAction(
        m_clusterActionRequestDecoder.leadershipTermId(),
        m_clusterActionRequestDecoder.logPosition(),
        m_clusterActionRequestDecoder.timestamp(),
        m_clusterActionRequestDecoder.action(), flags);
    return ControlledPollAction::BREAK;
  }

  case NewLeadershipTermEvent::SBE_TEMPLATE_ID: {
    m_newLeadershipTermEventDecoder.wrapForDecode(
        reinterpret_cast<char *>(buffer.buffer()),
        offset + MessageHeader::encodedLength(), actingBlockLength,
        actingVersion, buffer.capacity());

    m_consensusModuleAgent.onReplayNewLeadershipTermEvent(
        m_newLeadershipTermEventDecoder.leadershipTermId(),
        m_newLeadershipTermEventDecoder.logPosition(),
        m_newLeadershipTermEventDecoder.timestamp(),
        m_newLeadershipTermEventDecoder.termBaseLogPosition(),
        ClusterClock::map(m_newLeadershipTermEventDecoder.timeUnit()),
        m_newLeadershipTermEventDecoder.appVersion());
    break;
  }
  }

  return ControlledPollAction::CONTINUE;
}

} // namespace cluster
} // namespace aeron