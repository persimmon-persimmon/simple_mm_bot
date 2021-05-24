import liquidtap
import json
from datetime import datetime,timezone,timedelta
from threading import Thread
import json
import time

class LiquidRealtimeApi():
    """
    LiquidのRealtimeApiに接続する。最新のTicker情報を保持する。
    開始する時はstartメソッド、終了するときはstopメソッドを呼ぶ。
    情報取得時は各getメソッドを呼ぶ。
    取得可能データ:Ticker、1分ローソク足、最終メッセージ受信時からの経過時間
    """
    def __init__(self,logger):
        """
        :param logger:ロガーインスタンス
        """
        self.ticker = None
        self.stop_flg = True
        self.channel_thread = None
        self.ohlcv_1m = [{"timestamp":0} for _ in range(60)]
        self.logger = logger
        self.tz = timezone(timedelta(hours=+9), 'Asia/Tokyo')

    def _log_info(self,message):
        message = "[LiquidRealtimeApi]" + message
        self.logger.info(message)

    def start(self):
        """
        外部から呼び出される。処理を開始する。
        """
        self._log_info("start liquid realtime api.")
        self.stop_flg = False
        self.channel_thread = Thread(target=self._connect)
        self.channel_thread.setDaemon(True)
        self.channel_thread.start()
        self.last_massage_timestamp = 0
        while self.ticker is None:
            time.sleep(3)
            self._log_info("... waiting first message.")
        self._log_info("recieved first message.")

    def _subscribe(self, *args, **kwarg):
        """
        websocketの購読を設定するメソッド。_connectメソッド内でpusherオブジェクトのバインドする。
        """
        self.tap.pusher.subscribe("product_cash_btcjpy_5").bind("updated", self._update_callback_market)
        self.tap.pusher.subscribe("executions_cash_btcjpy").bind("created", self._update_callback_executions)

    def _connect(self):
        """
        websocketの購読を開始し、接続状況を監視する。
        最終メッセージ受信時から10秒経過した場合、接続に不備があるとみなし、websocketを再接続する。
        """
        while self.stop_flg == False:
            self.tap = liquidtap.Client()
            self.tap.pusher.connection.bind("pusher:connection_established", self._subscribe)
            self.tap.pusher.connect()
            time.sleep(30)
            while self.stop_flg == False:
                time.sleep(3)
                if  self.get_seconds_from_last_message() > 10:
                    self.tap.pusher.disconnect()
                    self._log_info("too latency, reconnect liquid realtime api.")
                    break

    def get_seconds_from_last_message(self):
        """
        最終メッセージ受信時から経過時間を返す。
        """
        return datetime.now(self.tz).timestamp() - self.last_massage_timestamp

    def stop(self):
        """
        外部から呼び出される。処理を終了する。
        """
        self._log_info("stop liquid realtime api.")
        self.stop_flg = True
        self.tap.pusher.disconnect()

    def get_ticker(self):
        """
        外部から呼び出される。Ticker情報を返す。
        """        
        return self.ticker

    def get_ohlcv(self,n):
        """
        外部から呼び出される。直近n期間の1分ローソク足配列を返す。配列は時系列順。最新のデータが最後尾に入っている。
        :param n:期間を指定。上限は60。
        """
        minute = datetime.now(self.tz).minute
        if minute-n < 0:
            return self.ohlcv_1m[minute-n:] + self.ohlcv_1m[:minute+1]
        else:
            return self.ohlcv_1m[minute-n:minute+1]

    def _update_callback_market(self,message):
        """
        Ticker情報を受信した時の処理。必要な情報のみ抽出し保持する。
        """
        ticker = json.loads(message)
        ticker = {
            "timestamp":float(ticker["timestamp"]),
            "ltp":float(ticker["last_traded_price"]),
            "ask":float(ticker["market_ask"]),
            "bid":float(ticker["market_bid"]),
            "high":float(ticker["high_market_ask"]),
            "low":float(ticker["low_market_bid"]),
            "volume":float(ticker["volume_24h"]),
            "latency":datetime.now(self.tz).timestamp()-float(ticker["timestamp"])
            }
        self.ticker = ticker
        self.last_massage_timestamp = ticker["timestamp"]

    def _update_callback_executions(self,message):
        """
        約定情報を受信した時の処理
        """
        execution = json.loads(message)
        execution = {
            "id":execution["id"],
            "price":execution["price"],
            "quantity":execution["quantity"],
            "taker_side":execution["taker_side"],
            "timestamp":float(execution["timestamp"])
        }
        self.ticker["ltp"] = execution["price"]
        self.last_massage_timestamp = execution["timestamp"]
        self._update_ohlcv_1m(execution)

    def _update_ohlcv_1m(self,execution):
        """
        約定情報から1分ローソク足を作る。
        1分間約定情報がないという状況が起きると不正確なデータになる。未対応。
        """
        minute = datetime.fromtimestamp(execution["timestamp"]).minute
        if execution["timestamp"] - self.ohlcv_1m[minute]["timestamp"] > 60:
            self.ohlcv_1m[minute]["timestamp"] = execution["timestamp"]
            self.ohlcv_1m[minute]["open"] = execution["price"]
            self.ohlcv_1m[minute]["high"] = execution["price"]
            self.ohlcv_1m[minute]["low"] = execution["price"]
            self.ohlcv_1m[minute]["close"] = execution["price"]
            self.ohlcv_1m[minute]["volume"] = execution["quantity"]
            self.ohlcv_1m[minute]["buy_volume"] = execution["quantity"] if execution["taker_side"] == "buy" else 0
            self.ohlcv_1m[minute]["sell_volume"] = execution["quantity"] if execution["taker_side"] == "sell" else 0
        else:
            self.ohlcv_1m[minute]["timestamp"] = execution["timestamp"]
            self.ohlcv_1m[minute]["high"] = max(self.ohlcv_1m[minute]["high"],execution["price"])
            self.ohlcv_1m[minute]["low"] = min(self.ohlcv_1m[minute]["low"],execution["price"])
            self.ohlcv_1m[minute]["close"] = execution["price"]
            self.ohlcv_1m[minute]["volume"] += execution["quantity"]
            self.ohlcv_1m[minute]["buy_volume"] += execution["quantity"] if execution["taker_side"] == "buy" else 0
            self.ohlcv_1m[minute]["sell_volume"] += execution["quantity"] if execution["taker_side"] == "sell" else 0


if __name__=='__main__':
    import logging
    logger = logging.getLogger(__name__)
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.DEBUG)
    liquid_realtime_api = LiquidRealtimeApi(logger)
    liquid_realtime_api.start()
    time.sleep(3)
    for _ in range(5):
        print(liquid_realtime_api.get_ticker(),liquid_realtime_api.get_ohlcv(1))
        time.sleep(3)
    liquid_realtime_api.stop()
