//
// Created by Arthur Motelevicz on 17/09/24.
//

#ifndef BINANCEMD_BMDMANAGER_H
#define BINANCEMD_BMDMANAGER_H

#include <functional>
#include <unordered_map>
#include <map>
#include <vector>

#include <beastboys>
#include <boost/asio.hpp> // Para usar boost::asio::steady_timer e strand

#include <mgutils/HeartBeatChecker.h>
// Remova a inclusão do Scheduler
// #include <mgutils/Scheduler.h>

#include "models/AggTrade.h"

namespace bmd
{
  enum class BinanceServiceType
  {
    SPOT,
    FUTURES
  };

  using AggTradeStreamCallback = std::function<void(bool, const models::AggTrade& aggTrade)>;
  using ReconnetUserDataStreamCallback = std::function<void(uint32_t newStreamId, uint32_t oldStreamId)>;
  using ScheduleCallback = std::function<void(bool success)>;
  using HeartBeatCallback = std::function<void(const std::shared_ptr<bb::network::ws::Stream>& stream)>;

  class BMDManager : public std::enable_shared_from_this<BMDManager>
  {
  public:
    static std::shared_ptr<BMDManager> create();
    virtual ~BMDManager();

    size_t getNumberOfStreams() const;

    /**
     * @brief Opens a Futures USD AggTrade stream for a given symbol.
     *
     * @param symbol The trading symbol (e.g., "btcusdt").
     * @param reconnectIntervalSeconds The interval in seconds to reconnect the stream.
     * @param aggTradeCB Callback invoked when an aggTrade message is received.
     * @param cb Callback invoked when the stream is reconnected.
     * @return The ID of the opened stream.
     */
    uint32_t openAggTradeStream(BinanceServiceType type,
                                const std::string& symbol,
                                uint32_t reconnectInSeconds,
                                const AggTradeStreamCallback& aggTradeCB,
                                const ReconnetUserDataStreamCallback& cb);

    void closeStream(uint32_t streamID);

    void setHeartBeatCallback(const HeartBeatCallback& hearBeat);
    void setUseTestsUrl();
  private:
    explicit BMDManager();
    void initialize();

    struct StreamInfo
    {
      std::shared_ptr<bb::network::ws::Stream> stream;
      // Removido o Scheduler
      // std::unique_ptr<mgutils::Scheduler> scheduler;
      std::unique_ptr<mgutils::HeartBeatChecker> heartBeatChecker;
      // Adicionado o reconnectTimer
      std::unique_ptr<boost::asio::steady_timer> reconnectTimer;
    };
    StreamInfo createStreamInfo(
        const std::shared_ptr<bb::network::ws::Stream>& stream,
        const std::string& symbol,
        BinanceServiceType type,
        uint32_t reconnectInSeconds,
        const AggTradeStreamCallback& aggTradeCB,
        const ReconnetUserDataStreamCallback& cb);

    std::unordered_map<uint32_t, StreamInfo> _streams;

    std::string _futuresUsdSocketBaseUrl = "fstream.binance.com";
    std::string _spotSocketBaseUrl = "stream.binance.com";
    std::string _testsUrl = "localhost";
    std::string _testsPort = "1234";

    std::shared_ptr<bb::Streamer> _streamer{nullptr};

    boost::asio::io_context _ioc;
    boost::asio::executor_work_guard<boost::asio::io_context::executor_type> _workGuard;
    std::thread _worker;
    std::atomic<bool> _stopWorker{false};

    uint64_t _heartBeatTimeOutInMillis = 60*1000*3 + 10*1000; //3 minutes + 10 seconds (3 minutes is the limit)

    HeartBeatCallback _heartBeatCallback;

    // Adicionado strand para sincronização
    boost::asio::strand<boost::asio::io_context::executor_type> strand_;

    void reconnectionHandlerAggTradeStream(std::shared_ptr<bb::network::ws::Stream> stream,
                                           BinanceServiceType type,
                                           const std::string& symbolCode,
                                           uint32_t reconnectInSeconds,
                                           const AggTradeStreamCallback& tradeCB,
                                           const ReconnetUserDataStreamCallback& cb);

    std::shared_ptr< bb::network::ws::Stream> createAggTradeStream(
        BinanceServiceType type,
        const std::string& symbolCode,
        uint32_t reconnectInSeconds,
        const AggTradeStreamCallback& aggTradeCB,
        const ReconnetUserDataStreamCallback& cb);

    bool _shouldUseTestUrl = false;

    void triggerStreamReconnection(
        uint32_t delayInSec,
        const std::shared_ptr<bb::network::ws::Stream>& stream,
        const std::string& symbol,
        BinanceServiceType type,
        uint32_t reconnectInSeconds,
        const AggTradeStreamCallback& aggTradeCB,
        const ReconnetUserDataStreamCallback& cb
    );
  };

}
#endif //BINANCEMD_BMDMANAGER_H
