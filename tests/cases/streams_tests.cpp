//
// Created by Arthur Motelevicz on 17/09/24.
//
#include <binancemd>
#include <catch2/catch.hpp>
#include <mgutils/Utils.h>

TEST_CASE("STREAMS", "[streams]")
{
  bmd::BMDManager manager;

  std::promise<bool> sendPromise;
  std::future<bool> sendFuture = sendPromise.get_future();

  std::shared_ptr<bb::network::rs::Stream> streamPtr;

  int countMsgs = 0;
  manager.openFutureAggTradeStream(
    "btcusdt",
    20,
    [&](bool success, const bmd::futuresUSD::models::AggTrade& aggTrade)
    {
      countMsgs++;
      if(success)
      {
        logI << "Price: " << aggTrade.price
             << "Amount: " << aggTrade.amount
             << "Time: " << aggTrade.time;

        REQUIRE(aggTrade.price != dNaN);
        REQUIRE( aggTrade.amount != dNaN);
        REQUIRE(aggTrade.amount != INVALID_INT64);
      }
      else
      {
        sendPromise.set_value(true);
      }
      if(countMsgs >= 10)
      {
        sendPromise.set_value(true);
      }
    },
    [&](uint32_t newStreamId, uint32_t oldStreamId){

    }
  );

  REQUIRE(countMsgs >= 10);
  sendFuture.wait();
}