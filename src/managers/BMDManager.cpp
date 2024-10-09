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
      _workGuard(boost::asio::make_work_guard(_ioc)),
      strand_(_ioc.get_executor()) // Inicializa o strand
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
            [self = shared_from_this(), aggTradeCB, cb, symbolCode, type, reconnectInSeconds](bool success, const std::string& data, SharedStream stream)
            {
              if (!success)
              {
                logW << "Stream (" << stream->getId() << ") @aggTrade closed with msg: " << data << ". Reconnecting in 1 seconds ...";
                aggTradeCB(false, models::AggTrade());
                self->triggerStreamReconnection(1,stream, symbolCode, type, reconnectInSeconds, aggTradeCB, cb);
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
          // Posta a operação no strand para garantir a sincronização
          boost::asio::post(self->strand_, [self, closedStream, symbolCode, aggTradeCB, reconnectInSeconds, type, cb]() {
            logW << "[BMDManager] Close callback triggered for stream (" << closedStream->getId() << "). Reconnecting in 10 seconds ...";
            self->triggerStreamReconnection(10, closedStream, symbolCode, type, reconnectInSeconds, aggTradeCB, cb);
          });
        });

    sharedStream->setPingStreamCallback([this](const std::shared_ptr<bb::network::ws::Stream>& stream)
                                        {
                                          if(_heartBeatCallback)
                                            _heartBeatCallback();

                                          // Restart the heartbeat for stream
                                          auto self = shared_from_this();
                                          boost::asio::post(strand_, [this, self, stream]() {
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
                                        });

    return std::move(sharedStream);
  }

  void BMDManager::reconnectionHandlerAggTradeStream(
      std::shared_ptr<bb::network::ws::Stream> stream,
      BinanceServiceType type,
      const std::string &symbolCode,
      uint32_t reconnectInSeconds,
      const AggTradeStreamCallback &aggTradeCB,
      const ReconnetUserDataStreamCallback &cb)
  {
    auto self = shared_from_this();
    boost::asio::post(strand_, [self, stream, type, symbolCode, reconnectInSeconds, aggTradeCB, cb]() mutable {
      if (stream->wasClosedByClient())
      {
        logI << "Stream " << stream->getId() << " has been closed by client, skipping reconnection.";
        return;
      }

      logI << "[BMDManager] Reconnecting stream (" << stream->getId() << ") for " << (type == BinanceServiceType::SPOT ? "spot" : "futures ") << symbolCode << " , reconnecting..";

      auto oldStreamId = stream->getId();
      stream->stop();
      stream = self->createAggTradeStream(type, symbolCode, reconnectInSeconds, aggTradeCB, cb);
      logI << "Stream " << stream->getId() << " created!";
      auto newStreamId = stream->getId();

      // Update the StreamInfo Map
      auto it = self->_streams.find(oldStreamId);
      if (it != self->_streams.end())
      {
        // Remove the old entry
        self->_streams.erase(it);
      }

      auto streamInfo = self->createStreamInfo(stream, symbolCode, type, reconnectInSeconds, aggTradeCB, cb);

      self->_streams.emplace(newStreamId, std::move(streamInfo));
      logI << "Stream " << newStreamId << " emplaced!";

      // Call the callback to inform the client that streams have changed
      if(cb)
        cb(newStreamId, oldStreamId);
    });
  }

  uint32_t BMDManager::openAggTradeStream(
      BinanceServiceType type,
      const std::string& symbol,
      uint32_t reconnectInSeconds,
      const AggTradeStreamCallback& aggTradeCB,
      const ReconnetUserDataStreamCallback& cb)
  {
    auto self = shared_from_this();
    auto promise = std::make_shared<std::promise<uint32_t>>();
    auto future = promise->get_future();

    if(_shouldUseTestUrl)
      reconnectInSeconds = 60;

    boost::asio::post(strand_, [this, self, type, symbol, reconnectInSeconds, aggTradeCB, cb, promise]() {
      auto cpSymbol = mgutils::string::toLower(symbol);

      auto stream = createAggTradeStream(type, symbol, reconnectInSeconds, aggTradeCB, cb);

      auto streamInfo = createStreamInfo(stream, symbol, type, reconnectInSeconds, aggTradeCB, cb);

      _streams.emplace(stream->getId(), std::move(streamInfo));

      promise->set_value(stream->getId());
    });

    // Como estamos em um contexto assíncrono, precisamos esperar o futuro
    return future.get();
  }

  void BMDManager::triggerStreamReconnection(
      uint32_t delayInSec,
      const std::shared_ptr<bb::network::ws::Stream>& stream,
      const std::string& symbol,
      BinanceServiceType type,
      uint32_t reconnectInSeconds,
      const AggTradeStreamCallback& aggTradeCB,
      const ReconnetUserDataStreamCallback& cb)
  {
    auto self = shared_from_this();
    boost::asio::post(strand_, [this, self, delayInSec, stream, symbol, type, reconnectInSeconds, aggTradeCB, cb]() {
      auto it = _streams.find(stream->getId());
      if(it != _streams.end())
      {
        it->second.reconnectTimer->cancel();
        it->second.reconnectTimer->expires_after(std::chrono::seconds(delayInSec));
        it->second.reconnectTimer->async_wait(
          boost::asio::bind_executor(
              strand_,
              [self, stream, symbol, type, reconnectInSeconds, aggTradeCB, cb](const boost::system::error_code& ec)
              {
                if (!ec)
                {
                  self->reconnectionHandlerAggTradeStream(stream, type, symbol, reconnectInSeconds, aggTradeCB, cb);
                }
                else if (ec != boost::asio::error::operation_aborted)
                {
                  logE << "Error in reconnect timer: " << ec.message();
                }
              }
          )
        );
      }
      else
      {
        logW << "[BMDManager] Stream (" << stream->getId() << ") not found to schedule a reconnection";
        // Posta a reconexão no strand para garantir a ordem
        boost::asio::post(strand_, [self, delayInSec, stream, type, symbol, reconnectInSeconds, aggTradeCB, cb]() {
          // Usa um timer do asio para agendar a reconexão
          auto timer = std::make_shared<boost::asio::steady_timer>(self->_ioc, std::chrono::seconds(delayInSec));
          timer->async_wait([self, stream, type, symbol, reconnectInSeconds, aggTradeCB, cb](const boost::system::error_code& ec) {
            if (!ec)
            {
              self->reconnectionHandlerAggTradeStream(stream, type, symbol, reconnectInSeconds, aggTradeCB, cb);
            }
            else
            {
              logE << "Error in timer for reconnection: " << ec.message();
            }
          });
        });
      }
    });
  }

  BMDManager::StreamInfo BMDManager::createStreamInfo(
      const std::shared_ptr<bb::network::ws::Stream>& stream,
      const std::string& symbol,
      BinanceServiceType type,
      uint32_t reconnectInSeconds,
      const AggTradeStreamCallback& aggTradeCB,
      const ReconnetUserDataStreamCallback& cb)
  {
    StreamInfo streamInfo
      {
        stream,
        std::make_unique<mgutils::HeartBeatChecker>(_heartBeatTimeOutInMillis, [self = shared_from_this(), stream, symbol, aggTradeCB, reconnectInSeconds, type, cb]()
        {
          logW << "Heartbeat timeout for stream " << stream->getId() << ". Reconnecting in 1 seconds ...";
          self->triggerStreamReconnection(1, stream, symbol, type, reconnectInSeconds, aggTradeCB, cb);
        }),
        // Inicialize o reconnectTimer
        std::make_unique<boost::asio::steady_timer>(_ioc)
      };

    // Agende a reconexão usando o reconnectTimer
    logW << "Schedule reconnect timer for stream " << stream->getId() << ". Reconnecting in " << reconnectInSeconds << " seconds";
    streamInfo.reconnectTimer->expires_after(std::chrono::seconds(reconnectInSeconds));
    streamInfo.reconnectTimer->async_wait(
      boost::asio::bind_executor(
        strand_,
        [self = shared_from_this(), stream, symbol, type, reconnectInSeconds, aggTradeCB, cb](const boost::system::error_code& ec)
        {
          if (!ec)
          {
            logW << "Reconnect timer expired for stream " << stream->getId() << ". Reconnecting...";
            self->reconnectionHandlerAggTradeStream(
                stream,
                type,
                symbol,
                reconnectInSeconds,
                aggTradeCB,
                cb);
          }
          else if (ec != boost::asio::error::operation_aborted)
          {
            logE << "Error in reconnect timer: " << ec.message();
          }
        }
      )
    );

    return streamInfo;
  }


  size_t BMDManager::getNumberOfStreams() const
  {
    // Como é uma função const, precisamos garantir que não haja modificações
    // Se o _streams for acessado apenas dentro do strand, isso está seguro
    auto self = const_cast<BMDManager*>(this)->shared_from_this();
    std::promise<size_t> promise;
    auto future = promise.get_future();

    boost::asio::post(strand_, [this, self, &promise]() {
      promise.set_value(_streams.size());
    });

    return future.get();
  }

  void BMDManager::closeStream(uint32_t streamID)
  {
    auto self = shared_from_this();
    boost::asio::post(strand_, [this, self, streamID]() {
      auto it = _streams.find(streamID);

      if (it != _streams.end())
      {
        boost::system::error_code ec;
        it->second.reconnectTimer->cancel(ec);
        if (ec && ec != boost::asio::error::operation_aborted)
          logE << "Error cancelling reconnectTimer for stream " << streamID << ": " << ec.message();

        it->second.heartBeatChecker->stop();
        it->second.stream->stop();
        _streams.erase(it);
        logI << "Stream " << streamID << " closed successfully.";
      } else
      {
        logW << "Stream " << streamID << " not found to close.";
      }
    });
  }

  void BMDManager::setHeartBeatCallback(const HeartBeatCallback& hearBeat)
  {
    auto self = shared_from_this();
    boost::asio::post(strand_, [this, self, hearBeat]() {
      _heartBeatCallback = hearBeat;
    });
  }

  void BMDManager::setUseTestsUrl()
  {
    auto self = shared_from_this();
    boost::asio::post(strand_, [this, self]() {
      _shouldUseTestUrl = true;
    });
  }

}
