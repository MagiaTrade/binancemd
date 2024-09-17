//
// Created by Arthur Motelevicz on 17/09/24.
//

#ifndef BINANCEMD_LOGGER_H
#define BINANCEMD_LOGGER_H

#include <memory>
#include <mgutils/Logger.h>

namespace bmd
{
  class Logger
  {
  public:
    inline static mgutils::Logger& instance()
    {
      static mgutils::Logger logger;
      return logger;
    }
  };
}

#define logT bmd::Logger::instance().log(mgutils::Trace)
#define logD bmd::Logger::instance().log(mgutils::Debug)
#define logI bmd::Logger::instance().log(mgutils::Info)
#define logW bmd::Logger::instance().log(mgutils::Warning)
#define logE bmd::Logger::instance().log(mgutils::Error)
#define logC bmd::Logger::instance().log(mgutils::Critical)


#endif //BINANCEMD_LOGGER_H
