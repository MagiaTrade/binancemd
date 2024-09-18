//
// Created by Arthur Motelevicz on 17/09/24.
//


#ifdef __APPLE__
#include <pthread.h>
#endif

#include <mgutils/Utils.h>
#include <mgutils/Exceptions.h>
#include <mgutils/Json.h>

#include "common/Logger.h"
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

    //to start the ping streams loop
    _timerToPingStreams = std::make_shared<boost::asio::steady_timer>(_ioc);
    pingStreams();
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

  std::shared_ptr< bb::network::ws::Stream> BMDManager::createFuturesUsdAggTradeStream(
      const std::string& symbolCode,
      uint32_t reconnectInSeconds,
      const FuturesUsdAggTradeStreamCallback& aggTradeCB,
      const ReconnetUserDataStreamCallback& cb)
  {
    auto cpSymbol = mgutils::string::toLower(symbolCode);

    std::weak_ptr<bb::network::ws::Stream> stream =
        _streamer->openStream(
            _futuresUsdSocketBaseUrl,
            "443",
            "/ws/" + cpSymbol + "@aggTrade",
            true,
            [self = shared_from_this(), aggTradeCB, cb, symbolCode](bool success, const std::string& data, SharedStream stream)
            {
              if (!success)
              {
                logW << "Futures Stream @aggTrade closed with msg: " << data;
                aggTradeCB(false, futuresUSD::models::AggTrade());

                // Stop the stream, which will trigger the close callback
                stream->stopWithCloseCallbackTriggered();
                return;
              }

              try
              {
                auto document = mgutils::Json::parse(data);
                futuresUSD::models::AggTrade aggTrade;
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
        [self = shared_from_this(),
            symbolCode,
            aggTradeCB,
            reconnectInSeconds,
            cb]
            (SharedStream closedStream)
        {
          self->reconnectionHandlerFuturesUsdAggTradeStream(
              closedStream,
              symbolCode,
              reconnectInSeconds,
              aggTradeCB,
              true,
              cb);
        });

    sharedStream->setPongStreamCallback([&](const std::shared_ptr<bb::network::ws::Stream>& stream){
      pongStream(stream);
    });

    return std::move(sharedStream);
  }


  void BMDManager::reconnectionHandlerFuturesUsdAggTradeStream(
      std::shared_ptr<bb::network::ws::Stream> stream,
      const std::string &symbolCode, uint32_t reconnectInSeconds,
      const FuturesUsdAggTradeStreamCallback &aggTradeCB,
      bool timerSuccess,
      const ReconnetUserDataStreamCallback &cb)
  {
    std::lock_guard<std::mutex> lock(_streamsMutex);

    if(!timerSuccess)
    {
      logE<< "Futures Trade stream: Time expired error!";
      return;
    }

    logI << "Futures Trade stream: Time expired, reconnecting..";

    auto oldStreamId = stream->getId();
    stream->stop();
    stream = createFuturesUsdAggTradeStream(symbolCode,reconnectInSeconds,aggTradeCB,cb);
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
    StreamInfo streamInfo{stream, tradeStreamTimer, false};

    scheduleTaskAfter(
      reconnectInSeconds,
      tradeStreamTimer,
      [self = shared_from_this(), stream, symbolCode, aggTradeCB, reconnectInSeconds, cb] (bool timerSuccess)
      {
        //it can be destructed by other reason ex: pong check, so just returns
        if(!timerSuccess)
          return;

        self->reconnectionHandlerFuturesUsdAggTradeStream(
            stream,
            symbolCode,
            reconnectInSeconds,
            aggTradeCB,
            true,
            cb);
      });

    _streams.emplace(newStreamId, std::move(streamInfo));

    // Call the callback to inform the client that streams have changed
    if(cb)
      cb(newStreamId, oldStreamId);
  }

  uint32_t BMDManager::openFutureAggTradeStream(
      const std::string& symbol,
      uint32_t reconnectInSeconds,
      const FuturesUsdAggTradeStreamCallback& aggTradeCB,
      const ReconnetUserDataStreamCallback& cb)
  {
    std::lock_guard<std::mutex> lock(_streamsMutex);

    auto cpSymbol = mgutils::string::toLower(symbol);

    auto stream = createFuturesUsdAggTradeStream(symbol, reconnectInSeconds, aggTradeCB, cb);
    auto tradeStreamTimer = std::make_shared<boost::asio::steady_timer>(_ioc);

    StreamInfo streamInfo{stream, tradeStreamTimer, false};

    scheduleTaskAfter(
        reconnectInSeconds,
        tradeStreamTimer,
        [self = shared_from_this(), reconnectInSeconds, stream, symbol, aggTradeCB, cb](bool success)
          {
            self->reconnectionHandlerFuturesUsdAggTradeStream(
                stream,
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

  /**
* PING - PONG STREAMS LOGIC
* ###############################################
*/
  void BMDManager::pingStreams()
  {
    std::lock_guard<std::mutex> lock(_streamsMutex);

    logW << "pingStreams";

    for(auto &kv : _streams)
    {
      kv.second.pongReceived = false;
      kv.second.stream->ping();

//      if(_futuresSymbolsStreams.count(kv.first)) {
//        logD << "Skip Pong Check Future symbol stream: " << kv.first <<"!\n";
//        continue;
//      }
    }

    scheduleTaskAfter(
      _timeBetweenPingPong,
      _timerToPingStreams,
      [self = shared_from_this()]
        (bool success)
        {
          if(!success) {
            logE << "Error scheduling pong check!";
            return;
          }
          self->checkPongs();
        });
  }

  void BMDManager::pongStream(const std::shared_ptr<bb::network::ws::Stream>& stream)
  {
    std::lock_guard<std::mutex> lock(_streamsMutex);

    logW << "pongStream";

    int streamId = (int)stream->getId();
    auto it = _streams.find(streamId);
    if (it != _streams.end())
      it->second.pongReceived = true;
    else
      logE << "Stream pong is not in the map! Something is wrong!";
  }

  void BMDManager::checkPongs()
  {
    std::lock_guard<std::mutex> lock(_streamsMutex);

    logW << "checkPongs";
    for (auto& kv : _streams)
    {
      if (!kv.second.pongReceived)
        kv.second.stream->stopWithCloseCallbackTriggered();
    }

    scheduleTaskAfter(
      _timeBetweenPingPong,
      _timerToPingStreams,
      [self = shared_from_this()](bool success)
      {
        if(!success) {
          logE << "Error scheduling ping streams!";
          return;
        }
        self->pingStreams();
      });
  }

  void BMDManager::closeStream(uint32_t id)
  {
    for(auto &kv: _streams)
    {
      if(kv.first == id)
        kv.second.stream->stopWithCloseCallbackTriggered();
    }
  }
}
