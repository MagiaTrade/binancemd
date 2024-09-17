//
// Created by Arthur Motelevicz on 17/09/24.
//

#include "managers/BMDManager.h"
#include "common/Logger.h"
#include <mgutils/Utils.h>
#include <mgutils/Json.h>
#include <mgutils/Exceptions.h>
#ifdef __APPLE__
#include <pthread.h>
#endif
namespace bmd
{
  BMDManager::BMDManager()
  {
    _streamer = std::make_shared<bb::Streamer>();
    _work = std::make_shared<boost::asio::io_context::work>(_ioc);
    _worker = std::thread([&]() {
#ifdef __APPLE__
      pthread_setname_np("Binance-Beast-Manager-Worker");
#endif
      try {
        _ioc.run();
      }
      catch (const boost::system::system_error &e)
      {
        logE << "Error running Manager ioContext for timers: " << e.what() << "\n";
      }
    });

    //to start the ping streams loop
    checkPongs();
  }

  BMDManager::~BMDManager()
  {
    _ioc.stop();
//    _work.reset();
    if(_worker.joinable())
      _worker.join();
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
            [&, aggTradeCB, cb, symbolCode, reconnectInSeconds](bool success, const std::string& data, SharedStream stream)
            {
              if(!success)
              {
                std::cout << "Futures Stream @aggTrade closed with msg: " << data << "\n\n";
                aggTradeCB(false, futuresUSD::models::AggTrade());
                //two seconds to attempt to connect again if it not succeeds
                scheduleTaskAfter(_timeToReconnectOnError, _timerFuturesUsdAggTradeStream,[&, stream, symbolCode, reconnectInSeconds, aggTradeCB, cb](bool success)
                {
                  if(!success)
                  {
                    std::cerr << "The timer for reschedule Futures AggTrade Stream after error, failed!\n";
                    return;
                  }

                  reconnectionHandlerFuturesUsdAggTradeStream(stream,
                                                              symbolCode,
                                                              reconnectInSeconds,
                                                              aggTradeCB,
                                                              true,
                                                              cb);
                });
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
            }
        );

    auto sharedStream = stream.lock();
    sharedStream->setCloseStreamCallback(
        [&,
            symbolCode,
            aggTradeCB,
            reconnectInSeconds,
            cb]
            (SharedStream closedStream){

          reconnectionHandlerFuturesUsdAggTradeStream(closedStream,
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


  void BMDManager::reconnectionHandlerFuturesUsdAggTradeStream(std::shared_ptr<bb::network::ws::Stream> stream,
                                                            const std::string &symbolCode, uint32_t reconnectInSeconds,
                                                            const FuturesUsdAggTradeStreamCallback &aggTradeCB,
                                                            bool timerSuccess,
                                                            const ReconnetUserDataStreamCallback &cb) {


    if(!timerSuccess) {
      logD<< "Futures Trade stream: Time expired error!\n";
      return;
    }


    logD << "Futures Trade stream: Time expired, reconnecting.. \n";
    auto oldStreamId = stream->getId();
    stream->stop();
    stream = createFuturesUsdAggTradeStream(symbolCode,reconnectInSeconds,aggTradeCB,cb);
    auto newStreamId = stream->getId();

    //we need a new timer because the stream id changed, so we need to update it
    if(_tradeStreamsTimers.find(oldStreamId) != _tradeStreamsTimers.end())
    {
      _tradeStreamsTimers.at(oldStreamId)->cancel();
      _tradeStreamsTimers.erase(oldStreamId);
    }

    auto traderStreamTimer = std::make_shared<boost::asio::steady_timer>(_ioc);
    _tradeStreamsTimers.insert_or_assign(newStreamId,traderStreamTimer);

    if(_streams.find(oldStreamId) != _streams.end())
      _streams.erase(oldStreamId);

    _streams.emplace(newStreamId, stream);

    scheduleTaskAfterForTimer(reconnectInSeconds, traderStreamTimer,
                              [&, stream, symbolCode, aggTradeCB, reconnectInSeconds, cb] (bool timerSuccess)
                              {
                                //it can be destructed by other reason ex: pong check, so just returns
                                if(!timerSuccess)
                                  return;

                                reconnectionHandlerFuturesUsdAggTradeStream(stream,
                                                                            symbolCode,
                                                                            reconnectInSeconds,
                                                                            aggTradeCB,
                                                                            true,
                                                                            cb);
                              });


    //call the callback warning the client that streams changed
    if(cb)
      cb(newStreamId, oldStreamId);
  }

  uint32_t BMDManager::openFutureAggTradeStream(
      const std::string& symbol,
      uint32_t reconnectInSeconds,
      const FuturesUsdAggTradeStreamCallback& aggTradeCB,
      const ReconnetUserDataStreamCallback& cb)
  {

    auto cpSymbol = mgutils::string::toLower(symbol);

    auto stream = createFuturesUsdAggTradeStream(symbol, reconnectInSeconds, aggTradeCB, cb);
    auto tradeStreamTimer = std::make_shared<boost::asio::steady_timer>(_ioc);
    _tradeStreamsTimers.insert_or_assign(stream->getId(), tradeStreamTimer);

    scheduleTaskAfterForTimer(reconnectInSeconds, tradeStreamTimer,
                              [&, reconnectInSeconds, stream, symbol, aggTradeCB, cb](bool success) {
                                reconnectionHandlerFuturesUsdAggTradeStream(stream,
                                                                            symbol,
                                                                            reconnectInSeconds,
                                                                            aggTradeCB,
                                                                            success,
                                                                            cb);
                              });

    _streams.emplace(stream->getId(), stream);
    return stream->getId();
  }

  void BMDManager::scheduleTaskAfter(uint32_t seconds,
                                  std::shared_ptr<boost::asio::steady_timer>& timer,
                                  const ScheduleCallback& cb)
  {
    timer = std::make_shared<boost::asio::steady_timer>(_ioc);
    timer->expires_after(std::chrono::seconds(seconds));
    timer->async_wait([&, cb](const boost::system::error_code& error)
    {
      if (!error)
      {
        cb(true);
        return;
      }

      cb(false);
    });
  }

  void BMDManager::scheduleTaskAfterForTimer(uint32_t seconds,
                                          const std::shared_ptr<boost::asio::steady_timer>& timer,
                                          const ScheduleCallback& cb)
                                          {
    timer->expires_after(std::chrono::seconds(seconds));
    timer->async_wait([&, cb](const boost::system::error_code& error)
    {
      if (!error)
      {
        cb(true);
        return;
      }
      cb(false);
    });
  }

  /**
* PING - PONG STREAMS LOGIC
* ###############################################
*/
  void BMDManager::pingStreams() {
    _streamsPongTracker.clear();
    for(auto &kv : _streams){

//            logD << "Ping Stream Id: " << kv.first ;
      kv.second->ping();

      if(_futuresSymbolsStreams.count(kv.first)) {
        logD << "Skip Pong Check Future symbol stream: " << kv.first <<"!\n";
        continue;
      }

      //resets the pong tracker to the check soon
      _streamsPongTracker.insert_or_assign(kv.first, false);
    }

    scheduleTaskAfter(_timeBetweenPingPong, _timerToPingStreams,[&](bool success){
      if(!success) {
        std::cerr << "Error scheduling pong check!\n";
        return;
      }

      checkPongs();
    });
  }

  void BMDManager::pongStream(const std::shared_ptr<bb::network::ws::Stream>& stream){

//        logD << "PongStream Id: " << stream->getId() ;

    int streamId = (int)stream->getId();
    if(_streamsPongTracker.count(streamId) <= 0){
      std::cerr << "Stream pong is not in the map! Something wrong!\n";
      return;
    }

    _streamsPongTracker.at(streamId) = true;
  }

  void BMDManager::checkPongs()
  {

    auto streamsCopy = _streams;
    for(auto &kv : streamsCopy) {

      //safe check
      if(_streamsPongTracker.count(kv.first) <= 0)
        continue;

      //if pong not ok force restart the stream
      if (!_streamsPongTracker.at(kv.first)) {
        kv.second->stopWithCloseCallbackTriggered();
      }
    }

    scheduleTaskAfter(_timeBetweenPingPong, _timerToPingStreams,[&](bool success){
      if(!success) {
        std::cerr << "Error scheduling ping streams!\n";
        return;
      }

      pingStreams();
    });
  }
}
