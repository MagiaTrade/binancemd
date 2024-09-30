//
// Created by Arthur Motelevicz on 17/09/24.
//
#ifdef __APPLE__
#include <pthread.h>
#endif

#include <mgutils/Utils.h>
#include <mgutils/Logger.h>
#include <mgutils/Exceptions.h>
#include <mgutils/Json.h>

#include "managers/BMDManager.h"

namespace bmd
{
  std::shared_ptr<BMDManager> BMDManager::create()
  {
// Use a private constructor and create a shared_ptr
    auto instance = std::shared_ptr<BMDManager>(new BMDManager());

    // Now that the instance is managed by a shared_ptr, shared_from_this() will work
    instance->initialize();

    return instance;
  }

  void BMDManager::initialize()
  {
    _streamer = std::make_shared<bb::Streamer>();
    _worker = std::thread([&]() {
#ifdef __APPLE__
      pthread_setname_np("BMD-Manager-Worker");
#endif
      try {
        _ioc.run();
      }
      catch (const boost::system::system_error &e)
      {
        logE << "Error running BMDManager ioContext for timers: " << e.what();
      }
    });
  }

  BMDManager::BMDManager():
  _workGuard(boost::asio::make_work_guard(_ioc))
  {}

  BMDManager::~BMDManager()
  {
    logW << "BMDManager destructor";
    _workGuard.reset(); // Allow io_context to stop when no work remains
    _ioc.stop();
    if(_worker.joinable())
      _worker.join();

    _streams.clear();
  }

  std::shared_ptr< bb::network::ws::Stream> BMDManager::createAggTradeStream(
      BinanceServiceType type,
      const std::string& symbolCode,
      uint32_t reconnectInSeconds,
      const AggTradeStreamCallback& aggTradeCB,
      const ReconnetUserDataStreamCallback& cb)
  {
    auto cpSymbol = mgutils::string::toLower(symbolCode);

    auto url = ( type == BinanceServiceType::SPOT ) ? _spotSocketBaseUrl : _futuresUsdSocketBaseUrl;
    std::weak_ptr<bb::network::ws::Stream> stream =
        _streamer->openStream(
            url,
            "443",
            "/ws/" + cpSymbol + "@aggTrade",
            true,
            [self = shared_from_this(), aggTradeCB, cb, symbolCode](bool success, const std::string& data, SharedStream stream)
            {
              if (!success)
              {
                logW << "Stream @aggTrade closed with msg: " << data;
                aggTradeCB(false, models::AggTrade());

                // Stop the stream, which will trigger the close callback
                stream->stopWithCloseCallbackTriggered();
                return;
              }

              try
              {
                auto document = mgutils::Json::parse(data);
                models::AggTrade aggTrade;
                aggTrade.deserialize(document);
                aggTradeCB(success, aggTrade);
              }
              catch (const mgutils::JsonParseException& error)
              {
                logE << "FuturesUSD Stream @aggTrade parse error: " << error.what();
                return;
              }
              catch (const std::exception& ex)
              {
                logE << "Unexpected error while parsing aggTrade data: " << ex.what();
              }
            }
        );

    auto sharedStream = stream.lock();
    sharedStream->setCloseStreamCallback(
        [self = shared_from_this(), symbolCode, aggTradeCB, reconnectInSeconds, type, cb] (SharedStream closedStream)
        {
          self->reconnectionHandlerAggTradeStream(
              closedStream,
              type,
              symbolCode,
              reconnectInSeconds,
              aggTradeCB,
              true,
              cb);
        });

    sharedStream->setPingStreamCallback([&](const std::shared_ptr<bb::network::ws::Stream>& stream)
    {
      logD << "Stream ping received!";
    });

    return std::move(sharedStream);
  }

  void BMDManager::reconnectionHandlerAggTradeStream(
      std::shared_ptr<bb::network::ws::Stream> stream,
      BinanceServiceType type,
      const std::string &symbolCode, uint32_t reconnectInSeconds,
      const AggTradeStreamCallback &aggTradeCB,
      bool timerSuccess,
      const ReconnetUserDataStreamCallback &cb)
  {
    std::lock_guard<std::mutex> lock(_streamsMutex);

    auto itStream = _streams.find(stream->getId());
    if (itStream == _streams.end()) {
      logI << "Stream " << stream->getId() << " has been closed, skipping reconnection.";
      return;
    }

    if (!timerSuccess)
    {
      logC << "Futures Trade stream: Time expired error!";
      return;
    }

    logI << "Futures Trade stream: Time expired, reconnecting..";

    auto oldStreamId = stream->getId();
    stream->stop();
    stream = createAggTradeStream(type, symbolCode, reconnectInSeconds, aggTradeCB, cb);
    auto newStreamId = stream->getId();

    // Update the StreamInfo Map
    auto it = _streams.find(oldStreamId);
    if (it != _streams.end())
    {
      // Cancel the old timer
      it->second.timer->cancel();
      // Remove the old entry
      _streams.erase(it);
    }

    auto tradeStreamTimer = std::make_shared<boost::asio::steady_timer>(_ioc);
    StreamInfo streamInfo{stream, tradeStreamTimer};

    _streams.emplace(newStreamId, std::move(streamInfo));

    scheduleTaskAfter(
      reconnectInSeconds,
      tradeStreamTimer,
      [self = shared_from_this(), stream, symbolCode, aggTradeCB, reconnectInSeconds, type, cb] (bool timerSuccess)
      {
        // It can be destructed by other reasons other than expiring.
        // In these cases just return.
        if(!timerSuccess)
          return;

        self->reconnectionHandlerAggTradeStream(
            stream,
            type,
            symbolCode,
            reconnectInSeconds,
            aggTradeCB,
            true,
            cb);
      });


    // Call the callback to inform the client that streams have changed
    if(cb)
      cb(newStreamId, oldStreamId);
  }

  uint32_t BMDManager::openAggTradeStream(
      BinanceServiceType type,
      const std::string& symbol,
      uint32_t reconnectInSeconds,
      const AggTradeStreamCallback& aggTradeCB,
      const ReconnetUserDataStreamCallback& cb)
  {
    std::lock_guard<std::mutex> lock(_streamsMutex);

    auto cpSymbol = mgutils::string::toLower(symbol);

    auto stream = createAggTradeStream(type, symbol, reconnectInSeconds, aggTradeCB, cb);
    auto tradeStreamTimer = std::make_shared<boost::asio::steady_timer>(_ioc);

    StreamInfo streamInfo{stream, tradeStreamTimer};

    scheduleTaskAfter(
        reconnectInSeconds,
        tradeStreamTimer,
        [self = shared_from_this(), reconnectInSeconds, stream, symbol, aggTradeCB, type, cb](bool success)
          {
            self->reconnectionHandlerAggTradeStream(
                stream,
                type,
                symbol,
                reconnectInSeconds,
                aggTradeCB,
                success,
                cb);
          });

    _streams.emplace(stream->getId(), std::move(streamInfo));
    return stream->getId();
  }

  void BMDManager::scheduleTaskAfter(
      uint32_t seconds,
      const std::shared_ptr<boost::asio::steady_timer>& timer,
      const ScheduleCallback& cb)
  {
    timer->expires_after(std::chrono::seconds(seconds));
    timer->async_wait([cb](const boost::system::error_code& error)
    {
      cb(!error);
    });
  }

  size_t BMDManager::getNumberOfStreams() const
  {
    return _streams.size();
  }

  void BMDManager::closeStream(uint32_t streamID)
  {
    std::lock_guard<std::mutex> lock(_streamsMutex);
    auto it = _streams.find(streamID);

    if (it != _streams.end())
    {
      it->second.timer->cancel();
      it->second.stream->stop();
      _streams.erase(it);
      logI << "Stream " << streamID << " closed successfully.";
    } else
    {
      logW << "Stream " << streamID << " not found to close.";
    }
  }

}
