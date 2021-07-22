from liquid_realtime_api import LiquidRealtimeApi
from liquid_rest_api import LiquidRestApi
from setup_logger import setup_logger
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
        message = "[BotBase]" + str(message)
        self.logger.info(message)

    def start(self):
        self._log("start bot.")
        self.realtime.start()
        self.stop_flg = False
        self.logic_thread = Thread(target=self._logic)
        self.logic_thread.setDaemon(True)
        self.logic_thread.start()

    def stop(self):
        self._log("stopping bot.")
        self.realtime.stop()
        self.rest.cancel_all_orders()
        self.stop_flg = True
        if self.logic_thread is not None:self.logic_thread.join()
        self._log("stop bot.")

    def _logic(self):
        """
        子クラスでオーバーライドする。
        """
        while self.stop_flg == False:
            print(self.realtime.get_ticker())

    def _sleep(self,n):
        time.sleep(n)
    
    def _get_now_timestamp(self):
        return datetime.now(self.tz).timestamp()

if __name__=='__main__':
    current_dir = os.path.dirname(__file__)
    config_file = os.path.join(current_dir,"config.json")
    config = json.load(open(config_file, "r"))
    logger = setup_logger(setup_logger(os.path.basename(__file__)))
    bot = BotBase(config,logger)
    bot.start()
    time.sleep(10)
    bot.stop()
