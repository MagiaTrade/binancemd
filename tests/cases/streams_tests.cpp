//
// Created by Arthur Motelevicz on 17/09/24.
//
#include <binancemd>
#include <catch2/catch.hpp>
#include <mgutils/Utils.h>

TEST_CASE("Open - Close Stream", "[streams]") {
  auto manager = bmd::BMDManager::create();

  auto streamID = manager->openFutureAggTradeStream(
      "btcusdt",
      10,
      [](bool success, const bmd::futuresUSD::models::AggTrade& aggTrade) {},
      [](uint32_t newStreamId, uint32_t oldStreamId) {}
  );

  std::this_thread::sleep_for(std::chrono::seconds(2));

  REQUIRE(manager->getNumberOfStreams() == 1);

  manager->closeStream(streamID);

  std::this_thread::sleep_for(std::chrono::seconds(2));

  REQUIRE(manager->getNumberOfStreams() == 0);

  manager.reset();
}

TEST_CASE("Reconnection", "[streams][reconnection]")
{
  auto manager = bmd::BMDManager::create();

  auto counter = std::make_shared<int>(0);
  auto sendPromise = std::make_shared<std::promise<bool>>();
  std::future<bool> sendFuture = sendPromise->get_future();

  auto streamIDToClose = std::make_shared<uint32_t>(0);
  (*streamIDToClose) = manager->openFutureAggTradeStream(
      "btcusdt",
      2,
      [](bool success, const bmd::futuresUSD::models::AggTrade& aggTrade){},
      [counter, sendPromise, streamIDToClose](uint32_t newStreamId, uint32_t oldStreamId)
      {
        (*counter)++;
        logI << "Reconnection " << " NewId:  " << newStreamId << " OldId: " << oldStreamId;

        if( (*counter) == 3)
        {
          (*streamIDToClose) = newStreamId;
          sendPromise->set_value(true);
        }
      }
  );

  sendFuture.wait();

  manager->closeStream(*streamIDToClose);
  std::this_thread::sleep_for(std::chrono::seconds(2));

  REQUIRE( (*counter) >= 3 );
  manager.reset();
}

TEST_CASE("Messages", "[streams]")
{
  auto manager = bmd::BMDManager::create();

  auto countMsgs = std::make_shared<int>(0);
  auto sendPromise = std::make_shared<std::promise<bool>>();
  std::future<bool> sendFuture = sendPromise->get_future();

  auto streamID = manager->openFutureAggTradeStream(
    "btcusdt",
    20,
    [countMsgs, sendPromise](bool success, const bmd::futuresUSD::models::AggTrade& aggTrade)
    {
      (*countMsgs)++;
      if((*countMsgs) > 10)
        return;

      if(success)
      {
        logI << "Symbol: " << aggTrade.symbol
              << " Price: " << aggTrade.price
             << " Amount: " << aggTrade.amount
             << " Time: " << aggTrade.lastTradeExecutedTime;


        REQUIRE(aggTrade.price != dNaN);
        REQUIRE( aggTrade.amount != dNaN);
        REQUIRE(aggTrade.lastTradeExecutedTime != INVALID_INT64);
      }
      else
      {
        sendPromise->set_value(true);
      }

      if( (*countMsgs) == 10)
        sendPromise->set_value(true);
    },
    [&](uint32_t newStreamId, uint32_t oldStreamId){

    }
  );

  sendFuture.wait();

  REQUIRE( (*countMsgs) >= 10 );

  manager->closeStream(streamID);
  std::this_thread::sleep_for(std::chrono::seconds(2));

  manager.reset();
}
