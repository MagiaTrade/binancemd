//
// Created by Arthur Motelevicz on 17/09/24.
//
#include <binancemd>
#include <catch2/catch.hpp>
#define SYMBOL_TO_TEST "wdov24"

TEST_CASE("COMMANDS")
{
//cedro::md::CMDBaseManager manager(CEDRO_USERNAME, CEDRO_PASSWORD, CEDRO_SOFTKEY);
//
//std::promise<bool> sendPromise;
//std::future<bool> sendFuture = sendPromise.get_future();
//
//std::shared_ptr<bb::network::rs::Stream> streamPtr;
//
//manager.connect([&](bool success, const std::shared_ptr<bb::network::rs::Stream>& stream)
//{
//streamPtr = stream;
//sendPromise.set_value(success);
//});
//
//TestHelper::waitForSuccess(sendFuture, streamPtr, 3);
//
//manager.setErrorCallback([&](const cedro::md::Error& error, const std::string& msg)
//{
//LOG_WARNING("Error: " + error.toString() + " Msg: " + msg);
//});
//
//SECTION("SQT snapshot")
//{
//std::promise<bool> sendSQTPromise;
//std::future<bool> sendSQTFuture = sendSQTPromise.get_future();
//bool isSnapshot = true;
//bool promiseSet = false;
//
//manager.subscribeQuote(SYMBOL_TO_TEST,
//[&](bool success, const char* data, size_t size)
//{
//if(promiseSet)
//return;
//
//auto *startMsg = std::strstr(data, "T:");
//auto *endMsg = std::strchr(data, '!');
//REQUIRE(startMsg != nullptr);
//REQUIRE(endMsg != nullptr);
//REQUIRE(success);
//REQUIRE(size > 250);
//LOG_INFO(data);
//sendSQTPromise.set_value(success);
//promiseSet = true;
//
//}, isSnapshot);

}