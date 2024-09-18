//
// Created by Arthur Motelevicz on 17/09/24.
//
#include <binancemd>
#include <catch2/catch.hpp>
#include <mgutils/Utils.h>

TEST_CASE("STREAMS", "[streams]")
{
  auto manager = bmd::BMDManager::create();

  std::promise<bool> sendPromise;
  std::future<bool> sendFuture = sendPromise.get_future();

  int countMsgs = 0;
  manager->openFutureAggTradeStream(
    "btcusdt",
    20,
    [&countMsgs, &sendPromise](bool success, const bmd::futuresUSD::models::AggTrade& aggTrade)
    {
      countMsgs++;
      if(countMsgs > 10)
      {
        sendPromise.set_value(true);
        return;
      }

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
        sendPromise.set_value(true);
      }
    },
    [&](uint32_t newStreamId, uint32_t oldStreamId){

    }
  );

  sendFuture.wait();

  REQUIRE(countMsgs >= 10);
}