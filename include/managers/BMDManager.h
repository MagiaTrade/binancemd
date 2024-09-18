//
// Created by Arthur Motelevicz on 17/09/24.
//

#ifndef BINANCEMD_BMDMANAGER_H
#define BINANCEMD_BMDMANAGER_H

#include <functional>
#include "models/futures/AggTrade.h"
#include <unordered_map>
#include <map>
#include <vector>
#include <beastboys>

namespace bmd
{
  using FuturesUsdAggTradeStreamCallback = std::function<void(bool, const futuresUSD::models::AggTrade& aggTrade)>;
  using ReconnetUserDataStreamCallback = std::function<void(uint32_t newStreamId, uint32_t oldStreamId)>;
  using ScheduleCallback = std::function<void(bool success)>;


  class BMDManager : public std::enable_shared_from_this<BMDManager>
  {
  public:
    explicit BMDManager();
    ~BMDManager();

    uint32_t openFutureAggTradeStream(const std::string& symbol,
                                      uint32_t reconnectInSeconds,
                                      const FuturesUsdAggTradeStreamCallback& aggTradeCB,
                                      const ReconnetUserDataStreamCallback& cb);


  private:
    std::map<uint32_t, std::shared_ptr<bb::network::ws::Stream>> _streams;

    std::string _futuresUsdSocketBaseUrl = "fstream.binance.com";
    std::string _spotSocketBaseUrl = "stream.binance.com";
    std::shared_ptr<bb::Streamer> _streamer{nullptr};
    uint32_t _timeToReconnectOnError = 5; //seconds
    uint32_t _timeOut = 20000;
    std::map<int, bool> _streamsPongTracker;
    std::shared_ptr<boost::asio::steady_timer> _timerToPingStreams{nullptr};
    std::map<int, std::shared_ptr<bb::network::ws::Stream>> _futuresSymbolsStreams;
    std::unordered_map<uint32_t, std::shared_ptr<boost::asio::steady_timer>> _tradeStreamsTimers;
    std::shared_ptr<boost::asio::steady_timer> _timerFuturesUsdAggTradeStream{nullptr};
    boost::asio::io_context _ioc;
//    std::shared_ptr<boost::asio::io_context::work> _work{nullptr};
    boost::asio::executor_work_guard<boost::asio::io_context::executor_type> _workGuard;
    std::thread _worker;

    void scheduleTaskAfter(uint32_t seconds,
                           std::shared_ptr<boost::asio::steady_timer>& timer,
                           const ScheduleCallback& cb);

    static void scheduleTaskAfterForTimer(uint32_t seconds,
                                   const std::shared_ptr<boost::asio::steady_timer>& timer,
                                   const ScheduleCallback& cb);

    void pingStreams();
    void pongStream(const std::shared_ptr<bb::network::ws::Stream>& stream);
    void checkPongs();
    uint32_t _timeBetweenPingPong = 25; //seconds -> wait 25s to ping + 25s to check pongs and ping again

    void reconnectionHandlerFuturesUsdAggTradeStream(std::shared_ptr<bb::network::ws::Stream> stream,
                                                     const std::string& symbolCode,
                                                     uint32_t reconnectInSeconds,
                                                     const FuturesUsdAggTradeStreamCallback& tradeCB,
                                                     bool timerSuccess,
                                                     const ReconnetUserDataStreamCallback& cb);

    std::shared_ptr< bb::network::ws::Stream> createFuturesUsdAggTradeStream(
        const std::string& symbolCode,
        uint32_t reconnectInSeconds,
        const FuturesUsdAggTradeStreamCallback& aggTradeCB,
        const ReconnetUserDataStreamCallback& cb);
  };
}
#endif //BINANCEMD_BMDMANAGER_H
