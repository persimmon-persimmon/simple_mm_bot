from bot_base import BotBase
from setup_logger import setup_logger
import json
import os
import traceback
from datetime import datetime
import time
from concurrent.futures import ThreadPoolExecutor

class HigeCatchBot(BotBase):
    def _logic(self):
        # ロジックパラメータ
        self.interval = 5
        self.alpha = 0.0025
        self.beta = 0.0001
        self.lot = 0.1
        self.max_lot = .1

        # 注文情報格納変数
        self.orders = []
        self.prices = {"ask":0,"bid":0,"ask_cancel":0,"bid_cancel":0,"close":0}

        # ポジション変数
        self.zero_position = 1e-4
        self.position = 0
        self.pre_position = 0

        # 評価指標計算用変数
        self.total_pnl = 0
        self.pre_total_pnl = 0
        self.total_loss = 0
        self.total_profit = 0
        self.hold_seconds = 0

        # 各種カウンター
        self.counter = {"ask_entry":0,"bid_entry":0,"ask_cancel":0,"bid_cancel":0,"trade":0,"win":0,"lose":0}

        # 発注用スレッドプール
        self.executor = ThreadPoolExecutor(max_workers=10)

        # emaパラメータ
        self.ema_span = 5
        self.ema = 0

        # 初期処理
        # ロジックに必要なデータを準備
        self._log("preperate data")
        num = 0
        for _ in range(self.ema_span):
            self._sleep(1)
            self.ema += self.realtime.get_ticker()["ltp"]
            num += 1
        self.ema /= num

        self.lot = 0.001

        # 本処理開始
        self.start_datetime = datetime.strftime(datetime.fromtimestamp(self._get_now_timestamp()),"%Y%m%d %H%M")
        timelog_5m = self._get_now_timestamp()
        timelog_1h = self._get_now_timestamp()
        timelog_1d = self._get_now_timestamp()
        self.no_order = False
        self.interval_counter = 0
        self._log("order logic start.")
        while self.stop_flg == False:
            self._output_price_log()
            self._sleep(1)
            try:
                self.interval_counter += 1
                if self._get_now_timestamp() - timelog_5m > 60 * 5:
                    timelog_5m = 0
                    self._5m_processing()

                if self._get_now_timestamp() - timelog_1h > 60 * 60:
                    timelog_1h = 0
                    self._1h_processing()
            
                if self._get_now_timestamp() - timelog_1d > 60 * 60 * 24:
                    timelog_1d = 0
                    self._1d_processing()
                
                if self.interval_counter < self.interval:
                    ltp = self.realtime.get_ticker()["ltp"]
                    self.ema = self._calc_ema(self.ema,ltp,self.ema_span)
                    self._monitor_price_cancel_order()
                    continue

                # インターバル明け。発注ロジック開始
                self.interval_counter = 0

                # 前回注文が残っていればキャンセル
                for order in self.orders:
                    if order is None:continue
                    self.rest.cancel_order(order["id"])
                self.orders = []

                # ポジションと損益を取得
                self.position,_,closed_pnl=self.rest.get_position_and_open_closed_pnl()
                self.total_pnl += closed_pnl
                if closed_pnl!=0:
                    if closed_pnl < 0:
                        self.counter["lose"] += 1
                        self.total_loss -= closed_pnl
                    else:
                        self.counter["win"] += 1
                        self.total_profit += closed_pnl

                if abs(self.pre_position) < self.zero_position and abs(self.position) >= self.zero_position:
                    """
                    ポジション新規
                    """
                    if self.position < 0:
                        self.counter["ask_entry"] += 1
                    else:
                        self.counter["bid_entry"] += 1
                    self.entry_timestamp = self._get_now_timestamp()
                elif  abs(self.pre_position) >= self.zero_position and abs(self.position) < self.zero_position:
                    """
                    ポジション決済
                    """
                    self.counter["trade"] += 1
                    self.hold_seconds += self._get_now_timestamp() - self.entry_timestamp
                self.pre_position = self.position

                ltp = self.realtime.get_ticker()["ltp"]
                self.ema = self._calc_ema(self.ema,ltp,self.ema_span)

                latency = self.realtime.get_seconds_from_last_message()
                if latency > 3:
                    self.no_order = True
                elif latency < 1:
                    self.no_order = False
                self.prices = {
                    "ask":int(round(self.ema * (1 + self.alpha))),
                    "bid":int(round(self.ema * (1 - self.alpha))),
                    "ask_cancel":int(round(self.ema * (1 + self.beta))),
                    "bid_cancel":int(round(self.ema * (1 - self.beta))),
                    "close":int(round(self.ema)),
                    }

                if abs(self.position) < self.zero_position and self.no_order==False:
                    ask_order = {"id":None,
                                "price":max(ltp,self.prices["ask"]),
                                "cancel_price":self.prices["ask_cancel"],
                                "quantity":-self.lot,
                                "side":"ask",
                                }
                    bid_order = {"id":None,
                                "price":min(ltp,self.prices["bid"]),
                                "cancel_price":self.prices["bid_cancel"],
                                "quantity":self.lot,
                                "side":"bid",
                                }
                    self.orders.append(ask_order)
                    self.orders.append(bid_order)
                elif abs(self.position) >= self.zero_position:
                    close_order = {"id":None,
                                "price":self.prices["close"],
                                "cancel_price":None,
                                "quantity":-self.position,
                                "side":None,
                                }
                    self.orders.append(close_order)

                for i in range(len(self.orders)):
                    self.executor.submit(self._create_limit_order_multithread,args=(i,))

                # ログ出力
                log_str = f"pos={round(self.position,4)}:total_pnl={round(self.total_pnl,6)}"
                log_str += f":ltp={ltp}"
                log_str += f":order_price={sorted([x['price'] for x in self.orders])}"
                log_str += f":counter={self.counter}"
                log_str += f":latency={round(latency,3)}"
                log_str += f":no_order={self.no_order}"
                self._log(log_str)

            except Exception as e:
                self._log(f"error occered in _logic. message={e}.traceback={traceback.format_exc()}")
                self.stop_flg = False

    def _5m_processing(self):
        """
        5分ごとに行う処理
        """
        self.rest.cancel_all_orders()
        self.orders = []

    def _1h_processing(self):
        """
        1時間ごとに行う処理
        損失が大きい時にbotを止めるなど
        """
        pass

    def _1d_processing(self):
        """
        1日おきに行う処理
        discordにメッセージを送るなど。
        """
        pass

    def _calc_ema(self,ema,ltp,ema_span):
        return (ema * (ema_span - 1) + ltp * 2) / (ema_span + 1)

    def _monitor_price_cancel_order(self):
        for i in range(len(self.orders)):
            if self.orders[i] is None:continue
            if self.orders[i]["side"] is None:continue
            if self.orders[i]["side"] == "ask":
                if self.orders[i]["cancel_price"] < self.ema:
                    self.rest.cancel_order(self.orders[i]["id"])
                    self.orders[i] = None
                    self.counter["ask_cancel"] += 1
            elif self.orders[i]["side"] == "bid":
                if self.orders[i]["cancel_price"] > self.ema:
                    self.rest.cancel_order(self.orders[i]["id"])
                    self.orders[i] = None
                    self.counter["bid_cancel"] += 1
        if self.interval_counter > 3:
            for i in range(len(self.orders)):
                if self.orders[i] is None:continue
                if self.orders[i]["side"] is None:continue
                self.rest.cancel_order(self.orders[i]["id"])
    
    def _output_price_log(self):
        """
        現在価格、ema、注文価格、キャンセル価格をファイル出力する。
        """
        if self.prices["ask"] == 0:return

        ltp = self.realtime.get_ticker()["ltp"]
        data = {
            "timestamp":self._get_now_timestamp(),
            "position":self.position,
            "pnl":self.total_pnl,
            "trade_count":self.counter["trade"],
            "ltp":ltp,
            "ema":self.ema,
            "ask":self.prices["ask"],
            "bid":self.prices["bid"],
            "ask_cancel":self.prices["ask_cancel"],
            "bid_cancel":self.prices["bid_cancel"],
        }
        """
        with open("","") as f:
            f.wirte()
        """

    def _get_result(self):
        """
        トレード成績を計算する
        pf:profit factor,
        wr:win rate,
        tc:trade count,
        hs:average hold seconds,
        """
        pf,wr,hs=None,None,None
        pf = round(self.total_profit / abs(self.total_loss),3) if self.total_loss != 0 else None
        wr = round(self.counter["win"] / self.counter["trade"] ,3) if self.counter["trade"] else None
        hs = round(sum(self.hold_seconds)/self.counter["trade"],3) if self.counter["trade"] else None
        return {"pnl":round(self.total_pnl,2),"pf":pf,"wr":wr,"tc":self.counter["trade"],"hs":hs}

    def _create_limit_order_multithread(self,args):
        i = args[0]
        price = self.orders[i]["price"]
        quantity = self.orders[i]["quantity"]
        order = self.rest.limit_order(quantity,price)
        self.orders[i]["id"] = order["id"]
    
if __name__=='__main__':
    current_dir = os.path.dirname(__file__)
    config_file = os.path.join(current_dir,"config.json")
    config = json.load(open(config_file, "r"))
    logger = setup_logger(setup_logger(os.path.basename(__file__)))
    bot = HigeCatchBot(config,logger)
    bot.start()
    while bot.stop_flg==False:
        try:
            time.sleep(10)
        except KeyboardInterrupt:
            print("recieve KeyboardInterrupt. stop bot.")
            bot.stop()
            time.sleep(3)
