from liquid_realtime_api import LiquidRealtimeApi
from liquid_rest_api import LiquidRestApi
import json
import os
from datetime import datetime,timezone,timedelta
from threading import Thread
import time

class BotBase():
    def __init__(self,config,logger):
        """
        :param logger:ロガーインスタンス
        """
        self.rest = LiquidRestApi(config,logger)
        self.realtime = LiquidRealtimeApi(logger)
        self.logger = logger
        self.stop_flg = True
        self.logic_thread = None
        self.tz = timezone(timedelta(hours=+9), 'Asia/Tokyo')

    def _log(self,message):
        message = "[BotBase]" + message
        self.logger.info(message)

    def start(self):
        self._log("start bot.")
        self.realtime.start()
        self.logic_thread = Thread(self._logic)
        self.logic_thread.setDaemon(True)
        self.logic_thread.start()

    def stop(self):
        self._log("stopping bot.")
        self.realtime.stop()
        self.stop_flg = True
        if self.logic_thread is not None:self.logic_thread.join()
        self._log("stop bot.")

    def _logic(self):
        while self.stop_flg == False:
            print(self.realtime.get_ticker())
            print(self.rest.get_ticker())

import logging
def setup_logger(name,logfile = None):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(fmt="%(asctime)s - %(levelname)s - %(message)s",datefmt="%Y-%m-%d %H:%M:%S")
    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    sh.setLevel(logging.INFO)
    logger.addHandler(sh)
    if logfile is not None:
        fh = logging.FileHandler(logfile)
        fh.setFormatter(formatter)
        fh.setLevel(logging.INFO)
        logger.addHandler(fh)
    return logger

if __name__=='__main__':
    current_dir = os.path.dirname(__file__)
    config_file = os.path.join(current_dir,"config.json")
    config = json.load(open(config_file, "r"))
    logger = setup_logger(__name__)
    bot = BotBase(config,logger)
    bot.start()
    time.sleep(10)
    bot.stop()
