"""
This is a base class for the trading agents.
"""

import abc
import asyncio
import concurrent.futures as ft
import copy
import logging
import logging.config
import threading
from urllib.parse import urlparse, parse_qs
from aiohttp import client_exceptions as aiohttp_exceptions
from fmclient.data.orm.order import Order, OrderType
from fmclient.data.orm.session import SessionState, Session
from fmclient.fmio.net.fmapi.rest.auth import Auth as FMAuth
from fmclient.fmio.net.fmapi.rest.request import Request, RequestMethod
from fmclient.fmio.net.hosting.ws.client import AgentWebsocketClient
from fmclient.utils import helper as hlp, constants as cons
from fmclient.utils.logging import FMLogger
from fmclient.data.message import MessageType


class Agent(object, metaclass=abc.ABCMeta):
    """
    This is the main (abstract) class that will provide the required functionality to build custom trading agents.
    The class provides automatic polling of order book, holding, and marketplace updates. In addition, the class also
    provides websocket communication.
    """

    def __init__(self, account, email, password, marketplace_id, name=None):
        """
        Note: Do not include any method that uses asyncio here. This creates a conflict with upload on algohost.
        :param account: FM account
        :param email:  FM email
        :param password: FM password
        :param marketplace_id: Id of marketplace
        :param name: name of the bot
        """
        self._account = account
        self._email = email
        self._password = password
        self._marketplace_id = marketplace_id
        self._name = name if name else email
        self._logger = None
        self._setup_logging()  # This should be init as soon as basic information on an agent is available
        self._loop = None
        self._description = None
        self._current_session = None
        self._is_session_active = False
        self._holdings = {}
        self._markets = {}
        self._markets_info = {}  # this information is sent to the subclass to encapsulate the FM specific urls.
        self._session_url = None
        self._markets_url = None
        self._holdings_url = None
        self._stop = False
        self._user_tasks = []
        self._outgoing_order_queue = None
        self._outgoing_order_count = {}
        self._incoming_message_queue = None
        self._ws = None
        self._monitor_count = 0  # For LAT_TEST ONLY
        self._conn_read_timeout = 5 * 60  # Disable: 0, aiohttp default: 5 * 60
        self._order_book_tasks = {}
        self._enable_user_communication = False
        self._exposed_fields = {}

    def _login(self):
        """
        FMAuth is a singleton and needs to be instantiated before any request is performed.
        #TODO: In case of errors, check if FMAuth was garbage collected.
        :return:
        """
        FMAuth(self._account, self._email, self._password)  # Only need to login once

    def initialise(self):
        self._loop = self._get_event_loop()
        self._login()
        self._set_asyncio_properties()
        self._outgoing_order_queue = asyncio.Queue()
        self._incoming_message_queue = asyncio.Queue()

        # Setup websocket client
        self._ws = AgentWebsocketClient(self._name, self._account, self._email, self._password,
                                        self._marketplace_id, cons.WS_ADDRESS, cons.WS_PORT, cons.WS_PATH,
                                        self._incoming_message_queue)
        self._ws.setup()

        # Fetch the essential marketplace information
        self.inform("Requesting initial marketplace information.")
        try:
            self._get_marketplace_info()
        except TypeError:
            self._logger.error("Unable to get initial marketplace information.")
            exit(1)
        except KeyboardInterrupt:
            self._logger.error("User interrupt received. Closing program.")
            logging.shutdown()
            exit(1)

        # Inform the trading agent that initialization is complete.
        self.inform("Received initial marketplace information.")
        self.initialised()
        self._init_communication()

    def _init_communication(self):
        if self.enable_user_communication:
            self._send_ws_message("activate communication", msg_type=MessageType.CUSTOM)

    def _check_init(self):
        """
        Checks if all the required variables have assigned values.
        :return:
        """
        assert self._name is not None
        assert self._account is not None
        assert self._email is not None
        assert self._password is not None
        assert self._marketplace_id is not None
        assert len(self._markets) > 0
        assert self._current_session is not None

    def start(self):
        """
        Start the algorithm execution by running the essential tasks in a loop.
        Each co-routine is executed in a single asyncio loop.
        :return:
        """
        try:
            asyncio_tasks = [self._get_monitor_session_task(), self._get_monitor_holdings_task()]

            # monitor order book
            for key, task in self._get_monitor_order_books_tasks().items():
                asyncio_tasks.append(task)
                self._order_book_tasks[key] = task

                # schedule and user defined tasks
            for task in self._user_tasks:
                asyncio_tasks.append(task)

                # task for the order processor
            asyncio_tasks.append(self._process_order_queue())

            # tasks for the handling incoming messages
            asyncio_tasks.append(self._process_incoming_messages())

            # run the loop
            try:
                # return exceptions True to prevent error in one task from failing all other tasks
                self._loop.run_until_complete(asyncio.gather(*asyncio_tasks, return_exceptions=True))
            except (aiohttp_exceptions.ServerConnectionError, aiohttp_exceptions.ServerDisconnectedError):
                self.error("Cannot connect to server!")
            except asyncio.TimeoutError:
                self.error("The connection to server timed out and now I have to die!")

        except KeyboardInterrupt:
            self.error("Oops. Keyboard Interrupt received from user.")
        finally:
            logging.shutdown()
            self.stop_after_wait(2)

    def stop_after_wait(self, wait_time):
        """
        Stop the algorithm execution in a graceful manner.
        Each perpetual task should by default check for the stop variable.
        :return:
        """

        # set the stop variable to be true
        async def _wait_stop():
            await asyncio.sleep(wait_time if wait_time > 0 else 2)
            self.stop = True

        task = self._loop.create_task(_wait_stop())
        asyncio.gather(task)

    def get_session(self):
        return self._current_session

    def execute_periodically(self, func, sleep_time):
        """
        Register a co-routine for the given function to be executed perpetually with a delay.
        :param func: The function to be executed.
        :param sleep_time: The time to wait before the next execution (at least 1 second)
        :return:
        """
        assert sleep_time >= 1

        async def _execute():
            while not self.stop:
                func()
                await asyncio.sleep(sleep_time)

        self._user_tasks.append(_execute())

    def _get_marketplace_info(self, call_back=None):
        """
        Get the details of the assigned marketplace.
        The function retrieves the markets under the marketplace, current session, and initial funds for the account.

        :return:
        """
        info = {}  # an empty dictionary to hold the result

        async def get_info():
            await self._request_marketplace_info(info)
            if "marketsUrl" in info.keys():
                self._markets_url = info["marketsUrl"]
                self._session_url = info["sessionUrl"]
                self._holdings_url = info["holdingsUrl"].split("&original")[0]
                await Request(self._markets_url, self._handle_markets_info).perform()
                await Request(self._session_url, self._handle_session_info).perform()
                await Request(self._holdings_url, self._handle_holdings_info).perform()
            else:
                self.error("Oops! Something seriously went wrong.")
                exit(1)

        # check if a loop exists and call the get_info method.
        if not self._loop.is_running():
            self._loop.run_until_complete(get_info())
        else:
            task = self._loop.create_task(get_info())
            if call_back is not None:
                task.add_done_callback(call_back)
            asyncio.gather(task)

    def _get_monitor_session_task(self):
        """
        Create an co-routine to monitor the marketplace session.
        Any change in the status of the session is notified to the listeners.
        :return:
        """

        def error_func(error):
            self.error(error)

        def handle_session_info(content):
            old_state = copy.copy(self._current_session.state)
            self._handle_session_info(content)
            self.debug(
                "The session id is " + str(self._current_session.id) + " is it open?" + str(self._is_session_active))
            if old_state!= self._current_session.state:
                info = {"session_id": self._current_session.id, "status": self._is_session_active}
                self.debug("The session status has changed. Is it open?" + str(self._is_session_active))
                if self._is_session_active:
                    self._get_marketplace_info()
                self.received_marketplace_info(info)

        task = Request(self._session_url, handle_session_info, error_callback_func=error_func)\
            .perform_forever_conditionally(delay=cons.MONITOR_SESSION_DELAY, should_continue=self.should_continue)
        return task

    def _get_monitor_order_books_tasks(self):
        """
        Creates asynchronous tasks for each market in the marketplace.
        Each task is responsible for polling the order book and returning the data to the listeners.
        :return: Array of Asyncio co-routines
        """
        tasks = {}

        # The callback method parses the json content, extracts the order information, and passes this to listener
        def handle_orderbook_info(content):
            _link = content["_links"]["self"]["href"]
            market_id = int(parse_qs(urlparse(_link).query)["marketId"][0])

            orders = self._create_order_list(content)

            if len(orders) > 0:
                self.debug(
                    "Received information on " + str(len(orders)) + " orders for market " + str(market_id) + ".")
            else:
                self.debug("No orders currently in the market " + str(market_id) + ".")
            self.received_order_book(orders, market_id)

        def error_func(error):
            self.error(error)

        params = {"size": 10000}

        for market in self._markets.values():
            task = Request(market["ordersAvailable"], handle_orderbook_info, error_callback_func=error_func,
                           params=params).perform_forever_conditionally(delay=cons.MONITOR_ORDER_BOOK_DELAY,
                                                                        condition=self.is_session_active, should_continue=self.should_continue)
            tasks[market["id"]] = task

        return tasks

    def _get_monitor_holdings_task(self):
        """
        Creates a co-routine to monitor holdings.
        The listeners are notified whenever information is received from the server.
        Currently, we do not check if the holdings were updated.
        :return:
        """

        def error_func(error):
            self.error(error)

        def handle_holdings_info(content):
            self.debug(':'.join(["LAT_TEST:HLD_RECV:MON_ID", str(content["mon_id"])]))
            self._handle_holdings_info(content)
            self.received_holdings(copy.deepcopy(self._holdings))

        params = {"size": 10000}

        task = Request(self._holdings_url, handle_holdings_info, error_callback_func=error_func,
                       params=params).perform_forever_conditionally(delay=cons.MONITOR_HOLDINGS_DELAY,
                                                                    condition=self.is_session_active, should_continue=self.should_continue)

        return task

    def _request_marketplace_info(self, info_holder):
        """
        Creates an asyncio task to request the marketplace info for the set marketplace id.
        :param info_holder: a dictionary to hold the information.
        :return: Asyncio coroutine.
        """
        marketplace_url = cons.MARKETPLACE_URL.replace("$id$", str(self._marketplace_id))

        def _extract_urls(content):
            info_holder["marketsUrl"] = content["_links"]["markets"]["href"]
            info_holder["sessionUrl"] = content["_links"]["currentSession"]["href"]
            info_holder["holdingsUrl"] = content["_links"]["currentHolding"]["href"]

        # return self._request_json(marketplace_url, _extract_urls)
        return Request(marketplace_url, _extract_urls).perform()

    def _handle_markets_info(self, content):
        """
        Extract info for markets and store in respective variables.
        :param content: json returned by the server.
        :return:
        """
        markets = content["_embedded"]["markets"]
        for i in range(0, len(markets)):
            market = markets[i]
            self._markets[market["id"]] = {"id": market["id"], "minimum": market["minimumPrice"],
                                           "maximum": market["maximumPrice"], "tick": market["priceTick"],
                                           "url": market["_links"]["market"]["href"],
                                           "orders": market["_links"]["orders"]["href"],
                                           "ordersAvailable": market["_links"]["ordersAvailable"]["href"],
                                           "ordersCompleted": market["_links"]["ordersCompleted"]["href"],
                                           "ordersAvailableNS":
                                               market["_links"]["ordersAvailable"]["href"].split("&original")[0]
                                           }
            self._markets_info[market["id"]] = {"id": market["id"], "minimum": market["minimumPrice"],
                                                "maximum": market["maximumPrice"], "tick": market["priceTick"],
                                                "name": market["name"], "item": market["item"],
                                                "description": market["description"]
                                                }
            # TODO: Remove from here. REASON?
            self._outgoing_order_count[int(market["id"])] = 0

    # ----- REST Methods -----
    async def _process_order_queue(self):
        """
        A task to send orders from the queue to the exchange server. On acceptance and rejection of orders the subclass
        is notified. The subclass must override order_accepted and order_rejected abstract methods.

        :return: Asyncio co-routine
        """

        def order_accepted(info):
            accepted_order = hlp.json_to_order(info)
            accepted_order.ref = order.ref
            self.order_accepted(accepted_order)

        def order_rejected(info):
            self.order_rejected(info, order)

        # TODO: Candidate for modularisation and code extraction
        while not self.stop:
            if self.is_session_active():
                while not self._outgoing_order_queue.empty():
                    order = self._outgoing_order_queue.get_nowait()
                    order_dict = {"type": order.type.name, "side": order.side.name, "price": order.price,
                                  "units": order.units, "market": order.market, "marketId": order.market_id}

                    if order.type == OrderType.CANCEL:
                        order_dict["supplier"] = order.id
                        order_dict["original"] = order.id

                    self.debug("Order Queued: {}".format(self._outgoing_order_count))
                    await Request("/orders", order_accepted, error_callback_func=order_rejected,
                                  request_method=RequestMethod.POST, data=order_dict).perform()
                    self.debug("  Order Sent: {}".format(self._outgoing_order_count))

                    self._outgoing_order_count[order.market_id] -= 1
                    # task = self._loop.create_task(self._rest_post_data(cons.API_ROOT + "/orders/", order_dict, order_accepted, order_rejected))
                    # asyncio.gather(task)
            else:
                if self._outgoing_order_queue.qsize() > 0:
                    self.warning("I cannot send orders to an inactive session.")
            await asyncio.sleep(cons.MONITOR_ORDER_BOOK_DELAY)

    async def _process_incoming_messages(self):
        """
        A task to process incoming messages to the trading agent.
        :return: Asyncio co-routine
        """
        while not self.stop:
            while not self._incoming_message_queue.empty():
                message = self._incoming_message_queue.get_nowait()
                if "source" in message.keys() and message["source"] == "bot":
                    pass
                elif "command" in message.keys() and message["command"] == "stop":
                    self.stop = True
                elif self.enable_user_communication:
                    self.handle_user_message(message["message"])
                else:
                    pass
                # Work on this and clean the output. It duplicates messages on web and here, via websockets
                self.debug("Received message: " + str(message["message"]), ws=False)
                # TODO Post custom order to the server, define domain language
            await asyncio.sleep(cons.WS_MESSAGE_DELAY)

    def my_info(self):
        """
        Send the algorithm name and associated marketplace id.
        :return: Algorithm name:marketplace id
        """
        return self._name + ":" + str(self._marketplace_id)

    def thread_info(self):
        """
        Print the number of thread cout and names of active threads.
        :return:
        """
        print(threading.active_count())
        for t in threading.enumerate():
            print(t.getName())

    def _set_asyncio_properties(self):
        """
        Sets the asyncio properties, e.g., the thread pool executor.
        :return:
        """
        executor = ft.ThreadPoolExecutor(max_workers=cons.ASYNCIO_MAX_THREADS)
        self._loop.set_default_executor(executor)

    def _send_ws_message(self, message, msg_type=MessageType.INFO):
        """
        Just a wrapper around the websocket message sending functionality.
        :param message: message string
        :param msg_type: type of message
        :return:
        """
        self._ws.send_message(message, str(msg_type.name))

    def _handle_session_info(self, content):  # Perhaps update this method to use Session class.
        """
        Extract relevant information from the session response.
        If the session is open, we set the active session variable to True.
        :param content:
        :return:
        """
        session = content
        self._current_session = Session(session["id"], session["state"])
        # self.debug("Session Info: " + str(self._current_session))
        if self._current_session.state is SessionState.OPEN:
            self._is_session_active = True
        else:
            self._is_session_active = False

    def _handle_holdings_info(self, content):
        """
        Extract relevant information from the account holdings response.
        The holdings data structure is updated based on the information.
        :param content:
        :return:
        """
        self._holdings["cash"] = {"cash": content["cash"], "available_cash": content["availableCash"]}
        self._holdings["markets"] = {}
        for asset in content["assets"]:
            market = asset["market"]
            self._holdings["markets"][market["id"]] = {"units": asset["units"],
                                                       "available_units": asset["availableUnits"]}

    def get_pending_orders(self, market_id):
        """
        Return the number of pending orders in the order queue for a given market.
        :param market_id: Id of the market
        :return: Number of orders waiting to be sent
        """
        return self._outgoing_order_count[market_id]

    def send_order(self, order):
        """
        Send an order to the exchange.
        :param order:
        """
        assert isinstance(order, Order)
        self._outgoing_order_queue.put_nowait(order)
        self._outgoing_order_count[order.market_id] += 1

    def get_completed_orders(self, market_id):
        """
        Request completed orders for a market from the exchange and calls the callback with the list of such orders.
        :param market_id: Id of the market.
        :return:
        """

        def handle_trades_info(info):
            trades = self._create_order_list(info)
            self.received_completed_orders(trades, market_id)

        if market_id in self._markets.keys():
            url = self._markets[market_id]["ordersCompleted"]
            params = {"size": 10000}
            # task = self._loop.create_task(self._request_json(url, handle_trades_info, params=params))
            task = self._loop.create_task(Request(url, callback_func=handle_trades_info, params=params).perform())
            asyncio.gather(task)

    def _create_order_list(self, orders_json: dict) -> list:
        if "_embedded" in orders_json.keys():
            json_orders = orders_json["_embedded"]["orders"]
        else:
            json_orders = []

        orders = []
        for json_order in json_orders:
            order = hlp.json_to_order(json_order)
            orders.append(order)

        return orders

    # ----- Logging Methods -----
    def inform(self, msg, ws=True):
        """
        Send the given message to relevant targets. For example, logging and websocket.
        :param msg:
        :param ws: True will send message on websockets as well
        :return:
        """
        self._logger.info(msg)
        if ws and self._logger.isEnabledFor(logging.INFO): self._send_ws_message(msg, MessageType.INFO)

    def error(self, msg, ws=True):
        self._logger.error(msg)
        if ws and self._logger.isEnabledFor(logging.ERROR): self._send_ws_message(msg, MessageType.ERROR)

    def debug(self, msg, ws=True):
        self._logger.debug(msg)
        if ws and self._logger.isEnabledFor(logging.DEBUG): self._send_ws_message(msg, MessageType.DEBUG)

    def warning(self, msg, ws=True):
        self._logger.warning(msg)
        if ws and self._logger.isEnabledFor(logging.WARNING): self._send_ws_message(msg, MessageType.WARN)

    def _setup_logging(self):
        """
        Setup the logger with specific format.
        :return:
        """
        # conf_file_location = os.path.normpath(os.path.join(os.path.dirname(__file__), "../logging.ini"))
        #
        # if os.path.exists(conf_file_location):
        #     default_config = {
        #         'agent_name': self._email,
        #         'marketplace_id': str(self._marketplace_id)
        #     }
        #     logging.config.fileConfig(conf_file_location, defaults=default_config)
        default_config = {
            'agent_name': self._email,
            'marketplace_id': str(self._marketplace_id)
        }

        # Name should be agent.<agent.name> format
        self._logger = FMLogger(default_config=default_config).get_logger(hlp.str_shorten(self.name, 12), "agent")
        try:
            self._log_file = FMLogger().get_logger("agent").handlers[0].baseFilename
        except IndexError:
            self._log_file = ""

    def _get_event_loop(self):
        """
        If the agent is not running in the main thread, create a new loop and associate it with asyncio.
        :return: Asyncio loop
        """
        try:
            loop = asyncio.get_event_loop()
        except Exception as e:
            loop = None
        if loop is None:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        return loop

    def handle_user_message(self, message):
        try:
            self.respond_to_user(message)
        except AttributeError as e:
            self.error("User communication is enabled but respond_to_user method is not defined.")

    def send_message_to_user(self, message):
        self._send_ws_message(message, MessageType.COMM)

    @property
    def description(self):
        return self._description

    @description.setter
    def description(self, value):
        self._description = value

    @property
    def name(self):
        return str(self._name)

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def stop(self):
        return self._stop

    @stop.setter
    def stop(self, value):
        self.inform("I have been asked to stop :-(")
        self._stop = bool(value)
        self._ws.stop = bool(value)

    @property
    def markets(self):
        return self._markets_info

    @property
    def holdings(self):
        return self._holdings

    @property
    def log_file(self):
        return self._log_file

    @property
    def enable_user_communication(self):
        return self._enable_user_communication

    @enable_user_communication.setter
    def enable_user_communication(self, value):
        self._enable_user_communication = value

    @property
    def exposed_fields(self):
        return self._exposed_fields

    @exposed_fields.setter
    def exposed_fields(self, value):
        self._exposed_fields = value

    def is_session_active(self):
        return self._is_session_active

    def should_continue(self):
        return not self.stop

    def is_session_active_and_can_continue(self):
        return self.is_session_active() and self.should_continue()

    # ---- Abstract Methods for Subclasses to make concrete ---

    @abc.abstractmethod
    def initialised(self):
        """
        Called after initialization of the robot is complete.
        :return:
        """
        raise NotImplementedError

    @abc.abstractmethod
    def order_accepted(self, order):
        """
        Called when an submitted order is accepted.
        :param order: Order that was submitted to the server
        :return:
        """
        raise NotImplementedError

    @abc.abstractmethod
    def order_rejected(self, info, order):
        """
        Called when a submitted order is rejected.
        :param info: Error information returned by the server
        :param order: Order that was submitted to the server
        :return:
        """
        raise NotImplementedError

    @abc.abstractmethod
    def received_order_book(self, order_book, market_id):
        """
        Called when order book details are available from the exchange.
        This method should be overridden by the implementing sub-class.
        :param order_book: List of orders
        :param market_id: Id of the market
        :return:
        """
        raise NotImplementedError

    def received_completed_orders(self, orders, market_id=None):
        """
        Called when completed orders are sent by the exchange.
        :param market_id:
        :param orders:
        :return:
        """
        raise NotImplementedError

    @abc.abstractmethod
    def received_holdings(self, holdings):
        """
        Called when holdings information is received from the exchange.
        This method should be overridden by the implementing sub-class.
        :param holdings:
        :return:
        """
        raise NotImplementedError

    @abc.abstractmethod
    def received_marketplace_info(self, marketplace_info):
        """
        Called when marketplace information is received from the exchange.
        :param marketplace_info:
        :return:
        """
        raise NotImplementedError

    @abc.abstractmethod
    def run(self):
        """
        The trading robot should override this method to start trading!
        :return:
        """
        raise NotImplementedError
