import os
import sys
import time
from datetime import datetime,timedelta,timezone
import ccxt
import json
import traceback
import jwt
import requests

class LiquidRestApi():
    """
    LiquidのRestApiを扱う。
    private apiを使用するにはapiキーが必要。configに設定する。
    同一キーで連続してapi呼び出しを行うとnonceエラーが起きる。複数のキーを使い回すことでエラーを回避する。
    """
    def __init__(self,config,logger):
        """
        :param config:コンフィグインスタンス。apiキーを保持。
        :param logger:ロガーインスタンス。
        """
        self.config = config
        self.logger = logger
        self.symbol = 'BTC/JPY'
        self.product_id = 5
        self.try_num = 3
        self.tz = timezone(timedelta(hours=+9), 'Asia/Tokyo')
        self.last_closed_pnl_timestamp = datetime.now(self.tz).timestamp()
        """self.ccxt_api = []
        for key01,key02 in self.config["liquid_api_keys_for_ccxt"]:
            ccxt_api = ccxt.liquid()
            ccxt_api.apiKey = key01
            ccxt_api.secret = key02
            self.ccxt_api.append(ccxt_api)
        self._ccxt_api_index = 0"""

        self.liquid_api_key = []
        for key01,key02 in self.config["liquid_api_keys"]:
            self.liquid_api_key.append([key01,key02])
        self._api_key_index = 0

    def _log_error(self,message):
        self.logger.error(f"[LiquidRestApi]{message}")

    def _log_info(self,message):
        self.logger.info(f"[LiquidRestApi]{message}")

    def _api_key(self):
        """
        次に使用するapiキーを返す。
        """
        self._api_key_index = self._api_key_index + 1 if self._api_key_index + 1 < len(self.liquid_api_key) else 0
        return self.liquid_api_key[self._api_key_index]
    '''
    def _ccxt_api(self):
        """
        次に使用するccxtインスタンスを返す。
        """
        self._ccxt_api_index = self._ccxt_api_index + 1 if self._ccxt_api_index + 1 < len(self.ccxt_api) else 0
        return self.ccxt_api[self._ccxt_api_index]'''
    
    def _create_request_param(self,path,query):
        url = 'https://api.liquid.com' + path + query
        token,secret = self._api_key()
        timestamp = datetime.now(self.tz).timestamp()
        payload = {
            "path": path,
            "nonce": timestamp,
            "token_id": token
        }
        signature = jwt.encode(payload, secret, algorithm='HS256')
        headers = {
            'X-Quoine-API-Version': '2',
            'X-Quoine-Auth': signature,
            'Content-Type' : 'application/json'
        }
        return headers,url

    def _to_my_order_format(self,order):
        return {
                "id": order["id"],
                "timestamp": order["created_at"],
                "symbol": order["currency_pair_code"],
                "status": order["status"],
                "side": order["side"],
                "price": float(order["price"]),
                "quantity": abs(float(order["quantity"])) if order["side"]=="buy" else -abs(float(order["quantity"])),
                "order_type": order["order_type"],
                "remaining": float(order["filled_quantity"]),
            }

    def get_ticker(self):
        url = f"https://api.liquid.com/products/{self.product_id}"
        res = requests.get(url)
        ticker = json.loads(res.text)
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
        return ticker

    def market(self,quantity):
        path = '/orders/'
        query = ''
        headers,url = self._create_request_param(path,query)
        side = "buy" if quantity > 0 else "sell"
        data = {
            "order":{
            "order_type":"market",
            "margin_type":"cross",
            "product_id":self.product_id,
            "side":side,
            "quantity":quantity,
            "leverage_level":2,
            "funding_currency":'JPY',
            "order_direction":'netout',
            }
        }
        json_data = json.dumps(data)

        for _ in range(self.try_num):
            try:
                res = requests.post(url, headers=headers, data=json_data)
                value = json.loads(res.text)
                return self._to_my_order_format(value)
            except Exception as e:
                self.logger.error(f"error in {sys._getframe().f_code.co_name}.{e}")
                self.logger.error(traceback.format_exc())
                time.sleep(1)
        raise Exception("over try_num.")

    # 指値注文する関数
    def limit(self,quantity,price):
        path = '/orders/'
        query = ''
        headers,url = self._create_request_param(path,query)

        side = "buy" if quantity > 0 else "sell"
        data = {
            "order":{
            "order_type":"limit",
            "margin_type":"cross",
            "product_id":self.product_id,
            "side":side,
            "quantity":quantity,
            "price":price,
            "leverage_level":2,
            "funding_currency":'JPY',
            "order_direction":'netout',
            }
        }
        json_data = json.dumps(data)

        for _ in range(self.try_num):
            try:
                res = requests.post(url, headers=headers, data=json_data)
                value = json.loads(res.text)
                return self._to_my_order_format(value)
            except Exception as e:
                self.logger.error(f"error in {sys._getframe().f_code.co_name}.{e}")
                self.logger.error(traceback.format_exc())
                time.sleep(1)
        raise Exception("over try_num.")

    """# 注文を確認
    def get_orders(self,status=None):
        orders=[]
        if status is None:
            orders=self._ccxt_api().fetch_orders(symbol=self.symbol,params = { "product_code" : self.symbol })
        elif status=='open':
            orders=self._ccxt_api().fetch_open_orders(symbol=self.symbol,params = { "product_code" : self.symbol })
        elif status=='closed':
            orders=self._ccxt_api().fetch_closed_orders(symbol=self.symbol,params = { "product_code" : self.symbol })
        return list(map(self._to_my_order_format,orders))"""

    def get_orders(self,status=None):
        for _ in range(self.try_num):
            try:
                path = '/orders/'
                query = ''
                headers,url = self._create_request_param(path,query)
                res = requests.get(url, headers=headers)
                value = json.loads(res.text)
                if status is None:
                    orders = [order for order in map(self._to_my_order_format,value)]
                    return orders
                else:
                    orders = [order for order in map(self._to_my_order_format,value) if order["status"]==status]
                    return orders
            except Exception as e:
                self.logger.error(f"error in {sys._getframe().f_code.co_name}.{e}")
                self.logger.error(traceback.format_exc())
                time.sleep(1)
        raise Exception("over try_num.")

    def get_order(self,order_id):
        orders = self.get_orders()
        for order in orders:
            if order['id'] == order_id:
                return self._to_my_order_format(order)
        raise Exception("no such order.")

    # 注文をキャンセルする。
    def cancel_order(self,order_id):
        try:
            path = f'/orders/{order_id}/cancel'
            query = ""
            headers,url = self._create_request_param(path,query)
            res = requests.put(url, headers=headers)
            order = json.loads(res.text)
            return self._to_my_order_format(order)
        except Exception as e:
            # 約定済みのケース
            return None

    def cancel_all_orders(self):
        """
        全ての注文をキャンセルする。
        """
        try:
            orders = self.get_orders(status="open")
            for order in orders:
                self.cancel_order(order["id"])
            return
        except Exception as e:
            self.logger.error(f"error in {sys._getframe().f_code.co_name}.{e}")
            self.logger.error(traceback.format_exc())
            raise Exception(e)

    # 全ポジションを決済
    def position_close_all(self):
        for _ in range(self.try_num):
            try:
                path = '/trades/close_all/'
                query = ''
                headers,url = self._create_request_param(path,query)
                res = requests.put(url, headers=headers)
                return json.loads(res.text)
            except Exception as e:
                self.logger.error(f"error in {sys._getframe().f_code.co_name}.{e}")
                self.logger.error(traceback.format_exc())
                time.sleep(1)
        raise Exception("over try_num.")

    def get_jpy(self):
        """
        jpy残高を取得
        """
        for _ in range(self.try_num):
            try:
                path = '/fiat_accounts/'
                query = ''
                headers,url = self._create_request_param(path,query)
                res = requests.get(url, headers=headers)
                return json.loads(res.text)
            except Exception as e:
                self.logger.error(f"error in {sys._getframe().f_code.co_name}.{e}")
                self.logger.error(traceback.format_exc())
                time.sleep(1)
        raise Exception("over try_num.")

    def get_trades(self):
        for _ in range(self.try_num):
            try:
                path = '/trades/'
                query = ''
                headers,url = self._create_request_param(path,query)
                res = requests.get(url, headers=headers)
                print(res)
                print(res.text)
                time.sleep(10)
                ret = json.loads(res.text)
                ret = [trade for trade in res["models"]]
                return ret
            except Exception as e:
                self.logger.error(f"error in {sys._getframe().f_code.co_name}.{e}")
                self.logger.error(traceback.format_exc())
                time.sleep(1)
        raise Exception("over try_num.")

    def get_position(self):
        """
        ポジションを取得
        """
        try:
            trades = self.get_trades()
            position = 0
            for trade in trades["models"]:
                if trade["currency_pair_code"] == self.symbol:
                    if trade["side"] == "long":
                        position += float(trade["open_quantity"])
                    elif trade["side"] == "short":
                        position -= float(trade["open_quantity"])
            return position
        except Exception as e:
            self.logger.error(f"error in {sys._getframe().f_code.co_name}.{e}")
            self.logger.error(traceback.format_exc())
            raise e

    def get_position_and_open_closed_pnl(self):
        """
        ポジションと実現損益、未実現損益を返す。
        実現損益は前回実行時からの差分を返す。
        初回実行時はinit時からの差分を返す。
        実行間隔が空きすぎると正確な情報は取れなくなる。
        :return: ポジション、未実現損益、実現損益
        """
        try:
            trades = self.get_trades()
            position = 0
            closed_pnl = 0
            open_pnl = 0
            next_lastest_timestamp = self.last_closed_pnl_timestamp
            for trade in trades["models"]:
                if trade["currency_pair_code"] == self.symbol:
                    if trade["status"] == "open":
                        open_pnl += float(trade["open_pnl"])
                        if trade["side"] == "long":
                            position += float(trade["open_quantity"])
                        elif trade["side"] == "short":
                            position -= float(trade["open_quantity"])
                    elif trade["status"] == "closed":
                        if self.last_closed_pnl_timestamp < trade["updated_at"]:
                            next_lastest_timestamp = max(next_lastest_timestamp,trade["updated_at"])
                            closed_pnl += float(trade["pnl"])
            self.last_closed_pnl_timestamp = next_lastest_timestamp
            return position,open_pnl,closed_pnl
        except Exception as e:
            self.logger.error(f"error in {sys._getframe().f_code.co_name}.{e}")
            self.logger.error(traceback.format_exc())
            raise e


if __name__=='__main__':
    import logging
    current_dir=os.path.dirname(__file__)
    config = os.path.join(current_dir,"config.json")
    config = json.loads(open(config,"r").read())

    logger = logging.getLogger(__name__)
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.DEBUG)
    rest_api = LiquidRestApi(config,logger)
    ticker = rest_api.get_ticker()
    print("get_position:",rest_api.get_position())
    print("get_jpy:",rest_api.get_jpy())
    print("get_orders:",rest_api.get_orders())
    exit()
    ask = ticker["ask"] + 5 * 10**5
    quantity = 0.0001
    print(f"create limit order, type=ask price={ask},quantity={quantity}")
    order = rest_api.limit(quantity,ask)
    print("limit:",order)
    print("get_orders:",rest_api.get_orders())
    print("get_order:",rest_api.get_order(order["id"]))
    print("cancel_order:",rest_api.cancel_order(order["id"]))
