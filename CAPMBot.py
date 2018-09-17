"""
This is a template bot for  the CAPM Task.
"""

import time
from enum import Enum
import numpy as np
from fmclient import Agent
from fmclient import Order, OrderSide, OrderType

# Group details
GROUP_MEMBERS = {"831865": "Kevin Xu", "834063" : "Austen McClernon", "796799": "Rishab Garg"}

class BotType(Enum):
    MARKET_MAKER = 0,
    REACTIVE = 1

class CAPMBot(Agent):

    def __init__(self, account, email, password, marketplace_id, risk_penalty=0.01, session_time=20):
        """
        Constructor for the Bot
        :param account: Account name
        :param email: Email id
        :param password: password
        :param marketplace_id: id of the marketplace
        :param risk_penalty: Penalty for risk
        :param session_time: Total trading time for one session
        """
        super().__init__(account, email, password, marketplace_id, name="CAPM Bot")
        self._payoffs = {}
        self._risk_penalty = risk_penalty
        self._session_time = session_time
        self._market_ids = {}
        self._start_time = 0
        self._role = -1
        self._current_unit_holdings = {}


    def initialised(self):
        print(self.markets.items())
        for market_id, market_info in self.markets.items():

            security = market_info["item"]

            description = market_info["description"]
            # updated the dictionary of payoffs of securtity
            #with key as security and payoffs as value.
            self._payoffs[security] = [int(a) for a in description.split(",")]

        print("[MARKET INFO] ",self._markets_info)
        print("[PAYOFFS] ",self._payoffs)

        #fills the _market_ids variable with a dictionary of keys 'items' and value 'id'
        for x in self._markets_info:
            self._market_ids[self._markets_info[x]["item"]] = self._markets_info[x]["id"]

        self._start_time = time.time()

    def get_potential_performance(self, orders):
        """
        Returns the portfolio performance if the given list of orders is executed.
        The performance as per the following formula:
        Performance = ExpectedPayoff - b * PayoffVariance, where b is the penalty for risk.
        :param orders: list of orders
        :return:
        """
        #Order(self, price, units, type, side, market, date=None, id=None, ref=None):)

        test_portfolio = self._current_unit_holdings
        factor = 0

        #adjusts copy of portfolio for trades. Buys adds unit, sells deducts unit
        for i in orders:
            if i.side == OrderSide.SELL:
                factor = -1
            else:
                factor = 1
            test_portfolio[self.get_item_name(i.market[-3:])] += factor*i.units #adjusts asset item units

        #unhash next line to check the orders have been applied to mock portfolio
        #print(test_portfolio)

        holding_list = [] #all unit holdings as a 1d array
        payoff_matrix = [] #a 2d adday with all payoff values
        cov_matrix = [] #2d array with all payoff values as individual lists

        #format the test portfolios to another variable with its values only
        for key in test_portfolio:
            holding_list.append(test_portfolio[key])
        #print(holding_list)

        verticle_holding_list = np.array([[x] for x in holding_list])  # 1 column, N rows list of holdings
        # print(verticle_holding_list)

        for ids in self._market_ids:
            payoff_matrix.append(self._payoffs[ids])
        #print("PAYOFF MATRIX: ", payoff_matrix)

        portfolio_covariance = np.cov(np.vstack(payoff_matrix),ddof=0)
        #print("PORTFOLIO COV: ", portfolio_covariance)

        payoff_variance = np.dot(verticle_holding_list.T, np.dot(portfolio_covariance, holding_list))
        print("PAYOFF VAR: ",payoff_variance)

        expected_payoff = np.sum((verticle_holding_list*np.average(np.vstack(payoff_matrix))))
        print("EXPECTED PAYOFF :", expected_payoff)

        performance = float(self.holdings["cash"]["available_cash"])+float(expected_payoff)-float(payoff_variance)*float(self._risk_penalty)
        print("PERFORMANCE: ", performance)

        pass

    def is_portfolio_optimal(self):
        """
        Returns true if the current holdings are optimal (as per the performance formula), false otherwise.
        :return:
        """
        pass

    #takes an market id (i.e 713) and returns the asset name of market id 713. Returns false otherwise.
    def get_item_name(self, market_id):
        for items in self._market_ids:
            if int(self._market_ids[items]) == int(market_id):
                return items
        return False

    def order_accepted(self, order):
        print(order.type.name, " was accepted")
        pass

    def order_rejected(self, info, order):
        print(order.type.name, " was rejected")
        pass

    def received_order_book(self, order_book, market_id):

        time_elapsed = int(round(((time.time() - self._start_time)/60))) #rounds seconds to minutes

        # updates current unit holdings according to item name and in order with payoff data
        for items in self._payoffs:
            self._current_unit_holdings[items] = self.holdings["markets"][self._market_ids[items]]["available_units"]

        #print("[CURRENT HOLDING] ", self._current_unit_holdings)

        #changes bot type from mm to reactive once time left <= 5min
        if (self._session_time - time_elapsed) <= 5:
            self._role = BotType.MARKET_MAKER
        else:
            self._role = BotType.REACTIVE

        Orders = [Order(500, 1, OrderType.LIMIT, OrderSide.SELL, 713, ref="b1"), Order(100, 1, OrderType.LIMIT, OrderSide.BUY, 714, ref="b1")]

        self.get_potential_performance(Orders)

        pass

    def received_completed_orders(self, orders, market_id=None):
        pass

    def received_holdings(self, holdings):
        pass

    def received_marketplace_info(self, marketplace_info):
        pass

    def run(self):
        self.initialise()
        self.start()


if __name__ == "__main__":
    FM_ACCOUNT = "bullish-delight"
    FM_EMAIL = "r.garg2@student.unimelb.edu.au"
    FM_PASSWORD = "796799"
    MARKETPLACE_ID = 372  # replace this with the marketplace id

    bot = CAPMBot(FM_ACCOUNT, FM_EMAIL, FM_PASSWORD, MARKETPLACE_ID)
    bot.run()
