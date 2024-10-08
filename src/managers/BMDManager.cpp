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
    auto instance = std::shared_ptr<BMDManager>(new BMDManager());
    instance->initialize();
    return instance;
  }

  void BMDManager::initialize()
  {
    _streamer = std::make_shared<bb::Streamer>();
    auto self = shared_from_this(); // Captura um shared_ptr de si mesmo
    _worker = std::thread([self]() {
#ifdef __APPLE__
      pthread_setname_np("BMD-Manager-Worker");
#endif
      try {
        while (!self->_stopWorker)
        {
          self->_ioc.run_one();
        }
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
    _stopWorker = true;
    _ioc.stop();
    if(_worker.joinable())
    {
      if (std::this_thread::get_id() == _worker.get_id())
        logE << "Destructor called from worker thread; cannot join from within the same thread.";
      else
        _worker.join();
    }

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

    auto url = _shouldUseTestUrl ? _testsUrl : (( type == BinanceServiceType::SPOT ) ? _spotSocketBaseUrl : _futuresUsdSocketBaseUrl);

    std::weak_ptr<bb::network::ws::Stream> stream =
        _streamer->openStream(
            url,
            _shouldUseTestUrl ?  _testsPort : "443" ,
            "/ws/" + cpSymbol + "@aggTrade",
            !_shouldUseTestUrl,
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

    sharedStream->setPingStreamCallback([this](const std::shared_ptr<bb::network::ws::Stream>& stream)
    {
      if(_heartBeatCallback)
        _heartBeatCallback();

      // Restart the heartbeat for stream
      auto it = _streams.find(stream->getId());
      if (it != _streams.end())
      {
        it->second.heartBeatChecker->restart();
      }
      else
      {
        logE << "Stream not tracked! Something wrong! Stream Id (" << stream->getId() << ")";
        assert(false && "Stream not tracked! Something wrong!");
      }
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
      logW << "Trade stream: Timer canceled for " << (type == BinanceServiceType::SPOT ? "spot" : "futures");
      return;
    }

    logI << "Trade stream: Time expired for " << (type == BinanceServiceType::SPOT ? "spot" : "futures") << ", reconnecting..";

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

    auto streamInfo = createStreamInfo(stream, symbolCode, type, reconnectInSeconds, aggTradeCB, cb);

    scheduleTaskAfter(
      reconnectInSeconds,
      streamInfo.timer,
      [self = shared_from_this(), stream, symbolCode, aggTradeCB, reconnectInSeconds, type, cb] (bool timerSuccess)
      {
        self->reconnectionHandlerAggTradeStream(
            stream,
            type,
            symbolCode,
            reconnectInSeconds,
            aggTradeCB,
            timerSuccess,
            cb);
      });

    _streams.emplace(newStreamId, std::move(streamInfo));

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

    auto streamInfo = createStreamInfo(stream, symbol, type, reconnectInSeconds, aggTradeCB, cb);

    scheduleTaskAfter(
        reconnectInSeconds,
        streamInfo.timer,
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

  BMDManager::StreamInfo BMDManager::createStreamInfo(
      std::shared_ptr<bb::network::ws::Stream> stream,
      const std::string& symbol,
      BinanceServiceType type,
      uint32_t reconnectInSeconds,
      const AggTradeStreamCallback& aggTradeCB,
      const ReconnetUserDataStreamCallback& cb)
  {
    auto tradeStreamTimer = std::make_shared<boost::asio::steady_timer>(_ioc);

    return StreamInfo
    {
      stream,
      tradeStreamTimer,
      std::make_unique<mgutils::HeartBeatChecker>(_heartBeatTimeOutInMillis, [self = shared_from_this(), stream, symbol, aggTradeCB, reconnectInSeconds, type, cb]()
      {
        logW << "Heartbeat timeout for stream " << stream->getId() << ". Reconnecting...";
        self->reconnectionHandlerAggTradeStream(stream, type, symbol, reconnectInSeconds, aggTradeCB, true, cb);
      })
    };
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
      it->second.heartBeatChecker->stop();
      _streams.erase(it);
      logI << "Stream " << streamID << " closed successfully.";
    } else
    {
      logW << "Stream " << streamID << " not found to close.";
    }
  }

  void BMDManager::setHeartBeatCallback(const HeartBeatCallback& hearBeat)
  {
    _heartBeatCallback = hearBeat;
  }

  void BMDManager::setUseTestsUrl()
  {
    _shouldUseTestUrl = true;
  }

}
