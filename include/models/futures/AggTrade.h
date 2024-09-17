//
// Created by Arthur Motelevicz on 17/09/24.
//

#ifndef BINANCEMD_FUTURES_USD_AGGTRADE_H
#define BINANCEMD_FUTURES_USD_AGGTRADE_H

#include <string>
#include <mgutils/json/JsonDocument.h>
#include <mgutils/Utils.h>
#include <mgutils/json/JsonValue.h>

namespace bmd::futuresUSD::models
{
  struct AggTrade
  {
    std::string type;  // e - Event type. In this case, "trade" indicates that this is a trade message.
    int64_t time = INVALID_INT64;      // E - Event time in milliseconds.
    std::string symbol;  // s - Trading symbol, i.e., the cryptocurrency pair being traded. For example, "BTCUSDT" indicates that the trading pair is Bitcoin/USDT.
    int64_t tradeID = INVALID_INT64;      // a - Aggregate Trade ID.
    double price = dNaN;      // p = Price at which the trade was executed.
    double amount = dNaN;      // q - Quantity of cryptocurrency that was traded.
    int64_t firstTradeID = INVALID_INT64;      // f - First Trade ID.
    int64_t lastTradeID = INVALID_INT64;      // l - Last Trade ID.
    int64_t lastTradeExecutedTime = INVALID_INT64;      // T - Aggregate Trade time in milliseconds. This is the timestamp of the time when last trade was executed.
    bool maker = false;         // m - Maker/taker indicator indicating whether the order was executed as a maker (true) or taker (false).

    void deserialize(const std::shared_ptr<mgutils::JsonDocument>& document)
    {
      mgutils::JsonValue root = document->getRoot();
      if(root.getString("e").has_value()) type = root.getString("e").value();
      if(root.getInt64("E").has_value()) time = root.getInt64("E").value();
      if(root.getString("s").has_value()) symbol = root.getString("s").value();
      if(root.getInt64("a").has_value()) tradeID = root.getInt64("a").value();
      if(root.getString("p").has_value()) price = std::stod(root.getString("p").value());
      if(root.getString("q").has_value()) amount = std::stod(root.getString("q").value());
      if(root.getInt64("f").has_value()) firstTradeID = root.getInt64("f").value();
      if(root.getInt64("l").has_value()) lastTradeID = root.getInt64("l").value();
      if(root.getInt64("T").has_value()) lastTradeExecutedTime = root.getInt64("T").value();
      if(root.getBool("m").has_value()) maker = root.getBool("m").value();
    }
  };
}

#endif //BINANCEMD_FUTURES_USD_AGGTRADE_H
