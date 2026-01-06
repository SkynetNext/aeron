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

    std::string toString() const;

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

// Implementation
template<typename P>
inline PublicationGroup<P>::PublicationGroup(
    const std::vector<std::string>& endpoints,
    const std::string& channelTemplate,
    std::int32_t streamId,
    PublicationFactory publicationFactory) :
    m_endpoints(endpoints),
    m_channelTemplate(channelTemplate),
    m_streamId(streamId),
    m_publicationFactory(publicationFactory),
    m_random(std::random_device{}())
{
}

template<typename P>
inline std::shared_ptr<P> PublicationGroup<P>::next(std::shared_ptr<Aeron> aeron)
{
    const int cursor = nextCursor();
    const std::string& endpoint = m_endpoints[cursor];

    auto channelUri = ChannelUri::parse(m_channelTemplate);
    channelUri->put(ENDPOINT_PARAM_NAME, endpoint);
    const std::string channel = channelUri->toString();

    if (m_current)
    {
        CloseHelper::quietClose(m_current);
    }
    m_current = m_publicationFactory(aeron, channel, m_streamId);
    return m_current;
}

template<typename P>
inline int PublicationGroup<P>::nextCursor()
{
    do
    {
        ++m_cursor;
        if (static_cast<int>(m_endpoints.size()) <= m_cursor)
        {
            m_cursor = 0;
        }
    }
    while (m_cursor == m_excludedPublicationCursorValue);

    return m_cursor;
}

template<typename P>
inline std::shared_ptr<P> PublicationGroup<P>::current() const
{
    return m_current;
}

template<typename P>
inline void PublicationGroup<P>::clearExclusion()
{
    m_excludedPublicationCursorValue = -1;
}

template<typename P>
inline void PublicationGroup<P>::closeAndExcludeCurrent()
{
    m_excludedPublicationCursorValue = m_cursor;
    CloseHelper::quietClose(m_current);
}

template<typename P>
inline void PublicationGroup<P>::shuffle()
{
    close();
    clearExclusion();
    // Fisher-Yates shuffle algorithm (1:1 with Java version)
    for (int i = static_cast<int>(m_endpoints.size()); --i > -1;)
    {
        const int j = m_random() % (i + 1);
        const std::string tmp = m_endpoints[i];
        m_endpoints[i] = m_endpoints[j];
        m_endpoints[j] = tmp;
    }
}

template<typename P>
inline void PublicationGroup<P>::close()
{
    if (m_current)
    {
        CloseHelper::quietClose(m_current);
        m_current.reset();
    }
}

template<typename P>
inline bool PublicationGroup<P>::isConnected() const
{
    return m_current && m_current->isConnected();
}

template<typename P>
inline std::string PublicationGroup<P>::toString() const
{
    std::string result = "PublicationGroup{endpoints=[";
    for (std::size_t i = 0; i < m_endpoints.size(); ++i)
    {
        if (i > 0)
        {
            result += ", ";
        }
        result += m_endpoints[i];
    }
    result += "], channelTemplate='";
    result += m_channelTemplate;
    result += "', streamId=";
    result += std::to_string(m_streamId);
    result += ", cursor=";
    result += std::to_string(m_cursor);
    result += ", excludedPublicationCursorValue=";
    result += std::to_string(m_excludedPublicationCursorValue);
    result += ", current=";
    result += (m_current ? "connected" : "null");
    result += "}";
    return result;
}

}}
