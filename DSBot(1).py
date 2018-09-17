"""
This is a template for Project 1, Task 1 (Induced demand-supply)
"""
import copy
from enum import Enum

import sys
from fmclient import Agent, OrderSide, Order, OrderType

# Group details
GROUP_MEMBERS = {"796799": "Rishab Garg", "831865": "Kevin Xu", "834063" : "Austen McClernon"}


# ------ Add a variable called DS_REWARD_CHARGE -----
DS_REWARD_CHARGE = 100




# Enum for the roles of the bot
class Role(Enum):
    BUYER = 0,
    SELLER = 1


# Let us define another enumeration to deal with the type of bot
class BotType(Enum):
    MARKET_MAKER = 0,
    REACTIVE = 1


class DSBot(Agent):
    # ------ Add an extra argument bot_type to the constructor -----
    def __init__(self, account, email, password, marketplace_id, bot_type):

        name = "H1Bot"
        super().__init__(account, email, password, marketplace_id,name)

        #the market_id is updated in the initialize method.
        self._market_id = -1

        # It can be either Buyer or seller depending on cash or assets availaible at start
        self._role = None
        self.MAXIMUM = 10001
        self.MINIMUM= -1

        #it stores all the trade_opportunities of buy and sell order in the dict.
        self._trade_opportunity = {"buy":{},"sell":{}}

        # Robot type can be either Market_Maker or Reactive.
        self._bot_type = bot_type

        self._waiting_for_server= False

        #it evaluates to true if we have send the order and waiting for the order to be accepted.
        #wait_buy_server_mm checks if are waiting for buy order sent by MM.
        #wait_sell_server_mm checks if we are waitinr sell order sent by MM
        self._wait_buy_server_mm = False
        self._wait_sell_server_mm =False

    #it tells the type of the role of the bot, if it is SELLER or BUYER.
    def role(self):

        return self._role

    def initialised(self):

        #cash_info holds the available cash with bot.
        cash_info = self.holdings["cash"]["available_cash"]

        #self.holdings is a dictionary

        ##will get the information from market id.
        asset_info = None
        for market_id, market_holding in self.holdings["markets"].items():
            self._market_id = market_id
            asset_info = market_holding["available_units"]

        #A bot can be either Buyer or Seller depending on it's initial holdings.

        if asset_info > 0:
            print("asset")
            self._role  = Role.SELLER

        elif cash_info > 0:
            self._role = Role.BUYER


        self.inform("my bot has a role of" + str(self._role))




    def order_accepted(self, order):

        #resets waiting for server to show order successfully send to server
        self._waiting_for_server= False
        self._wait_buy_server_mm=False
        self._wait_sell_server_mm=False

        print("my order got accepted",order)



    def order_rejected(self, info, order):

        # resets waiting for server to show order successfully send to server
        self._waiting_for_server= False
        self._wait_buy_server_mm = False
        self._wait_sell_server_mm = False
        self.inform("my order " + str(order) + " has rejected")
        print("my order  has  got rejected",order,info)




    def received_order_book(self, order_book, market_id):

        #gets the trade_opportunity every time the order book is received from server.
        self._trade_opportunity = {"buy": {}, "sell": {}}

        #Order is defined as markert id: order object reference, type, Mine, Buy or Sell , Unit with price.

        for order in order_book:
            if not order.mine:

                #creates a list with all the buy orders

                if(order._side == OrderSide.BUY):
                    if( order._id not  in  self._trade_opportunity['buy'].keys()):
                        self._trade_opportunity['buy'][order._id] = copy.deepcopy(order)

                #creates a list with all the sell orders

                if (order._side == OrderSide.SELL):
                    if ( order._id  not in self._trade_opportunity['sell'].keys()):
                        self._trade_opportunity['sell'][order._id] = copy.deepcopy(order)



        if(self._bot_type == BotType.REACTIVE):
            print("I am a Reactive Bot")
            self._reactive(order_book)
        elif(self._bot_type == BotType.MARKET_MAKER):
            print("I am a Market Maker")

            self._marketmaker(order_book)

        self._print_trade_opportunity(order_book)




    def _reactive(self,other_order):

        print("Current cash balance ", self.holdings['cash']['available_cash'])
        print("Current asset balance ", self.holdings['cash']['available_units'])
        min_order_price = self.MAXIMUM
        place_buy_order = False

        #need to check the order placed was of us and make sure it doesn't place another order.
        for order in other_order:
            if(order.mine and order.side == OrderSide.BUY):
                place_buy_order = True


        #if we are buyer and any person who is selling a sell order which is less than DS_UTIL,
        #we will buy from it.
        #calculated the minimum order of sell and placed a buy order if it is less than the given amount.
        #also checked that we placed one order till the server gives the confirmation.

        if( self._role == Role.BUYER):

            #find the lowest ask order price
            for (id,order) in self._trade_opportunity['sell'].items():
                if(order._price < min_order_price):
                    min_order_price = order._price

            #if a valid lowest ask price is present, no outstanding orders, and have enough money for profitable trade
            # then execute trade

            if(min_order_price !=self.MAXIMUM and not self._waiting_for_server
                    and not place_buy_order and
                    self.holdings['cash']['available_cash'] >= min_order_price
                    and DS_REWARD_CHARGE >= min_order_price):

                place_buy_order = Order(min_order_price,
                                1,OrderType.LIMIT,OrderSide.BUY,self._market_id,ref="b1")

                self.send_order(place_buy_order)
                print("[ORDER PLACED] ",place_buy_order)
                self._waiting_for_server = True


        #reset parameters
        max_order_price = self.MINIMUM
        place_sell_order = False

        #check no existing sell orders
        for order in other_order:
            if (order.mine and order.side == OrderSide.SELL):
                place_sell_order = True

        #if seller, than
        if (self._role == Role.SELLER):

            #check all market orders and find highest bid price
            for (id, order) in self._trade_opportunity['buy'].items():
                if (order._price > max_order_price):
                    max_order_price = order._price
            #print("max order price to buy is ", max_order_price)

            #if profitable trade exists, and order able to be sent to server, and we have available assets,
            # then send order
            if ( max_order_price!=self.MINIMUM and    not self._waiting_for_server and not place_sell_order
                    and self.holdings['markets'][self._market_id]['available_units']>0
                    and DS_REWARD_CHARGE <= max_order_price):

                #send sell order
                place_sell_order = Order(max_order_price, 1, OrderType.LIMIT, OrderSide.SELL,
                                        self._market_id, ref="b1")

                self.send_order(place_sell_order)
                print("[ORDER PLACED] ", place_sell_order)
                self._waiting_for_server = True

    #this defines the market maker code
    def _marketmaker(self, other_order):

        #initial parameters and displays cash balance
        place_sell_order = False
        place_buy_order = False
        print("Current cash balance ", self.holdings['cash']['available_cash'])
        print("Current asset balance ", self.holdings['cash']['available_units'])

        #if we already have a sell/buy order, then we do not place another
        for order in other_order:
            if (order.mine and order.side == OrderSide.SELL):
                place_sell_order = True
            if(order.mine and order.side == OrderSide.BUY):
                place_buy_order= True

        #if we have positive asset, and can place a order, we place sell order for 5c more than our DS_REWARD_CHARGE
        if (not self._wait_sell_server_mm and not place_sell_order
                and self.holdings['markets'][self._market_id]['available_units']>0 ): #sell
            place_sell_order = Order(DS_REWARD_CHARGE+5, 1, OrderType.LIMIT, OrderSide.SELL,
                                     self._market_id, ref="b1")

            #Send order
            self.send_order(place_sell_order)
            self._wait_buy_server_mm=True
            print("[ORDER PLACED] ", place_sell_order)

        # if we have positive asset, and can place a order, we place buy order for 5c less than our DS_REWARD_CHARGE
        if (not self._wait_buy_server_mm and not place_buy_order
                and self.holdings['cash']['available_cash'] >= (DS_REWARD_CHARGE-5)): # buy
            place_buy_order = Order(DS_REWARD_CHARGE-5,
                                    1, OrderType.LIMIT, OrderSide.BUY, self._market_id, ref="s1")
            self._wait_sell_server_mm=True
            self.send_order(place_buy_order)
            print("[ORDER PLACED] ", place_buy_order)

    def _print_trade_opportunity(self, other_order):

        #if we are a buyer, or market maker, than we print all profitable orders
        if(self._role == Role.BUYER or self._bot_type == BotType.MARKET_MAKER):
            for (id, ord) in self._trade_opportunity['sell'].items():
                #print("sell orders ")
                #print(ord._price)

                #if sell orders is less than ds_reward_charge, a profitable buy opportunity exists
                if(ord._price <= DS_REWARD_CHARGE and not (self._bot_type ==BotType.MARKET_MAKER)):

                    #print proftiable trade
                    if(self.holdings['cash']['available_cash'] >= ord._price):
                        self.inform("[PROFITABLE TRADE] @ " + str(ord._price) )
                        print("[PROFITABLE TRADE] @ ", str(ord._price) )
                    if(self.holdings['cash']['available_cash'] < ord._price):
                        print("cash with us is ", self.holdings['cash']['available_cash'])

                        self.inform("[PROFITABLE TRADE] @ " + str(ord._price) , " but has insufficient cash")
                        print("[PROFITABLE TRADE] @ ",ord._price , " but has insufficient cash")

                #market maker goes through all the sell orders and buys it which is
                #5cents lowers than utility.
                if(self._bot_type==BotType.MARKET_MAKER and ord._price <= (DS_REWARD_CHARGE-5)):
                    if (self.holdings['cash']['available_cash'] >= ord._price):
                        self.inform("[PROFITABLE TRADE] @ " + str(ord._price))
                        print("[PROFITABLE TRADE] @ ", str(ord._price))

                    if (self.holdings['cash']['available_cash'] < ord._price):

                        #if profitable trade exists, but insufficient cash, then it is printed.
                        self.inform("[PROFITABLE TRADE] @ " + str(ord._price), " but has insufficient cash")
                        print("[PROFITABLE TRADE] @ ", ord._price, " but has insufficient cash")


        #defines profitable trade opportunity for seller or market maker.
        if (self._role == Role.SELLER or self._bot_type == BotType.MARKET_MAKER):

            #for each trade
            for (id, ord) in self._trade_opportunity['buy'].items():

                #if trade is profitable, we check if we have enough units to sell
                if (ord._price >= DS_REWARD_CHARGE and not (self._bot_type ==BotType.MARKET_MAKER)):

                    #if we have enough units, then trade is executed
                    if (self.holdings['markets'][self._market_id]['available_units'] >0):
                        print("[PROFITABLE TRADE] @ ", str(ord._price))
                        self.inform("[PROFITABLE TRADE] @ " + str(ord._price))

                    #if we dont have enough units, then system is notified
                    if (self.holdings['markets'][self._market_id]['available_units'] == 0 ):
                        print("[PROFITABLE TRADE] @ ", str(ord._price), " but has insufficient assets")
                        self.inform("[PROFITABLE TRADE] @ "+ str(ord._price), " but has insufficient assets")

                # market maker goes through all the buy orders and sells it which is
                # 5cents above than utility price.
                if (self._bot_type == BotType.MARKET_MAKER and ord._price >= (DS_REWARD_CHARGE + 5)):

                    #trade is executed if we have enough units
                    if (self.holdings['markets'][self._market_id]['available_units'] >0):
                        self.inform("[PROFITABLE TRADE] @ " + str(ord._price))
                        print("[PROFITABLE TRADE] @ ", str(ord._price))

                    #system is notified is profitable trade exists but insufficient assets
                    if (self.holdings['markets'][self._market_id]['available_units'] == 0 ):
                        self.inform("[PROFITABLE TRADE] @ " + str(ord._price), " but has insufficient cash")
                        print("[PROFITABLE TRADE] @ ", ord._price, " but has insufficient assets")





    def received_completed_orders(self, orders, market_id=None):
        pass

    def received_holdings(self, holdings):


        pass

    def received_marketplace_info(self, marketplace_info):
        self.inform(marketplace_info['session_id'])
        pass

    def run(self):
        self.initialise()
        self.start()


if __name__ == "__main__":
    FM_ACCOUNT = "bullish-delight"
    FM_EMAIL = "k.xu16@student.unimelb.edu.au"
    FM_PASSWORD = "831865"
    MARKETPLACE_ID = 260  # replace this with the marketplace id
    bot_type= BotType.MARKET_MAKER
    ds_bot = DSBot(FM_ACCOUNT, FM_EMAIL, FM_PASSWORD, MARKETPLACE_ID,BotType.MARKET_MAKER)
    ds_bot.run()
