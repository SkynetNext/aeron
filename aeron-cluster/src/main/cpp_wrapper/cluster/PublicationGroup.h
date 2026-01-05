#pragma once
#include <memory>
#include <vector>
#include <string>
#include <random>
#include "Aeron.h"
#include "Publication.h"
#include "ExclusivePublication.h"
#include "ChannelUri.h"
#include "util/CloseHelper.h"

namespace aeron { namespace cluster
{
using namespace aeron::util;

/**
 * Group of publications that can be rotated through.
 */
template<typename P>
class PublicationGroup
{
public:
    using PublicationFactory = std::function<std::shared_ptr<P>(std::shared_ptr<Aeron> aeron, const std::string& channel, std::int32_t streamId)>;

    PublicationGroup(
        const std::vector<std::string>& endpoints,
        const std::string& channelTemplate,
        std::int32_t streamId,
        PublicationFactory publicationFactory);

    ~PublicationGroup() = default;

    std::shared_ptr<P> next(std::shared_ptr<Aeron> aeron);

    std::shared_ptr<P> current() const;

    void clearExclusion();

    void closeAndExcludeCurrent();

    void shuffle();

    void close();

    bool isConnected() const;

private:
    int nextCursor();

    std::vector<std::string> m_endpoints;
    std::string m_channelTemplate;
    std::int32_t m_streamId;
    PublicationFactory m_publicationFactory;
    int m_cursor = 0;
    int m_excludedPublicationCursorValue = -1;
    std::shared_ptr<P> m_current;
    std::mt19937 m_random;
};

}}

