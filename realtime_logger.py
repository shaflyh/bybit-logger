"""
PyBit WebSocket Real-Time Trading Logger for Bybit

Connects to Bybit's private WebSocket streams to provide real-time updates
to a Google Sheet. It manages four separate sheets:
1.  Real-Time Log: A detailed, append-only log of every trade execution.
2.  Live Open Orders: A snapshot of all pending/unfilled orders.
3.  Live Open Positions: A snapshot of all current open futures positions.
4.  Live Wallet Balance: A snapshot of current asset balances.
"""

import time
from datetime import datetime
from typing import Dict, List, Optional
from pybit.unified_trading import WebSocket

from config import Config
from google_sheets_service import GoogleSheetsService


class WebSocketService:
    """Handles PyBit WebSocket connection and stream subscriptions."""

    def __init__(self, callback_handler):
        self.callback_handler = callback_handler
        self.ws = WebSocket(
            testnet=Config.BYBIT_USE_TESTNET,
            channel_type="private",
            api_key=Config.BYBIT_API_KEY,
            api_secret=Config.BYBIT_API_SECRET
        )
        env = "TESTNET" if Config.BYBIT_USE_TESTNET else "MAINNET"
        print(f"PyBit WebSocket initialized for {env}")

    def start_streams(self):
        """Starts all required WebSocket streams."""
        try:
            print("📡 Subscribing to WebSocket streams...")
            self.ws.execution_stream(
                callback=self.callback_handler.handle_execution)
            print("   - Subscribed to 'execution' stream.")
            self.ws.position_stream(
                callback=self.callback_handler.handle_position)
            print("   - Subscribed to 'position' stream.")
            self.ws.wallet_stream(callback=self.callback_handler.handle_wallet)
            print("   - Subscribed to 'wallet' stream.")
            # # Subscribe to the order stream
            # self.ws.order_stream(callback=self.callback_handler.handle_order)
            # print("   - Subscribed to 'order' stream.")

            print("\n✅ All streams started successfully.")
            return True
        except Exception as e:
            print(f"❌ Error starting WebSocket streams: {e}")
            return False

    def stop_streams(self):
        if self.ws:
            self.ws.exit()
            print("WebSocket streams stopped.")


class DataProcessor:
    """Processes and formats raw WebSocket data for Google Sheets."""

    EXECUTION_HEADERS = ["Timestamp", "Category", "Symbol", "Side", "PnL", "Fee", "Exec Price",
                         "Exec Qty", "Exec Value", "Create Type", "Order Type", "Maker/Taker", "OrderID", "ExecutionID"]
    # # Headers for the new Live Orders sheet
    # ORDER_HEADERS = ["Updated Time", "Symbol", "Side", "Order Type", "Status", "Price", "Qty",
    #                  "Avg Fill Price", "Filled Qty", "Remaining Qty", "Take Profit", "Stop Loss", "Order ID"]
    POSITION_HEADERS = ["Symbol", "Side", "Size", "Entry Price", "Mark Price",
                        "Value (USD)", "Unrealized PnL", "Leverage", "Liq. Price", "Updated"]
    WALLET_HEADERS = ["Coin", "Balance", "Available", "Value (USD)"]

    @staticmethod
    def format_execution(execution: Dict) -> Optional[Dict]:
        """Formats an execution message for the append-only log."""
        try:
            if execution.get("execType") != "Trade":
                return None
            pnl = float(execution.get("execPnl", 0))
            fee = float(execution.get("execFee", 0))
            return {
                "Symbol": execution.get("symbol", ""),
                "Side": execution.get("side", ""),
                "PnL": f"{pnl:.8f}",
                "Fee": f"{fee:.8f}",
                "Exec Price": execution.get("execPrice", "0"),
                "Exec Qty": execution.get("execQty", "0"),
                "Exec Value": execution.get("execValue", "0"),
                "Category": execution.get("category", "").capitalize(),
                "Timestamp": datetime.fromtimestamp(int(execution.get("execTime", 0)) / 1000).strftime('%Y-%m-%d %H:%M:%S'),
                "Create Type": execution.get("createType", "N/A"),
                "Order Type": execution.get("orderType", ""),
                "Maker/Taker": "Maker" if execution.get("isMaker") else "Taker",
                "OrderID": execution.get("orderId", ""),
                "ExecutionID": execution.get("execId", ""),
            }
        except (ValueError, TypeError, KeyError) as e:
            print(f"⚠️  Could not format execution: {e}")
            return None

    # # Function to process order stream data
    # @staticmethod
    # def format_order(order: Dict) -> Optional[Dict]:
    #     """Formats an order message for the 'Live Orders' snapshot."""
    #     try:
    #         # We only want to see orders that are still active
    #         active_statuses = ["New", "PartiallyFilled", "Untriggered"]
    #         if order.get("orderStatus") not in active_statuses:
    #             return None

    #         return {
    #             "Updated Time": datetime.fromtimestamp(int(order.get("updatedTime", 0)) / 1000).strftime('%Y-%m-%d %H:%M:%S'),
    #             "Symbol": order.get("symbol", ""),
    #             "Side": order.get("side", ""),
    #             "Order Type": order.get("orderType", ""),
    #             "Status": order.get("orderStatus", ""),
    #             "Price": order.get("price", "0"),
    #             "Qty": order.get("qty", "0"),
    #             "Avg Fill Price": order.get("avgPrice", "0"),
    #             "Filled Qty": order.get("cumExecQty", "0"),
    #             "Remaining Qty": order.get("leavesQty", "0"),
    #             "Take Profit": order.get("takeProfit", "0"),
    #             "Stop Loss": order.get("stopLoss", "0"),
    #             "Order ID": order.get("orderId", ""),
    #         }
    #     except (ValueError, TypeError, KeyError) as e:
    #         print(f"⚠️  Could not format order: {e} | Data: {order}")
    #         return None

    @staticmethod
    def format_position(position: Dict) -> Optional[Dict]:
        try:
            if float(position.get("size", "0")) == 0:
                return None
            return {"Symbol": position.get("symbol", ""), "Side": position.get("side", ""), "Size": position.get("size", "0"), "Entry Price": position.get("entryPrice", "0"), "Mark Price": position.get("markPrice", "0"), "Value (USD)": position.get("positionValue", "0"), "Unrealized PnL": position.get("unrealisedPnl", "0"), "Leverage": position.get("leverage", ""), "Liq. Price": position.get("liqPrice", "0"), "Updated": datetime.fromtimestamp(int(position.get("updatedTime", 0)) / 1000).strftime('%H:%M:%S'), }
        except (ValueError, TypeError, KeyError) as e:
            print(f"⚠️  Could not format position: {e} | Data: {position}")
            return None

    @staticmethod
    def format_wallet(wallet_data: Dict) -> List[Dict]:
        formatted_balances = []
        try:
            for coin in wallet_data.get('coin', []):
                if float(coin.get("walletBalance", 0)) > 0:
                    formatted_balances.append({"Coin": coin.get("coin", ""), "Balance": coin.get(
                        "walletBalance", "0"), "Available": coin.get("availableToWithdraw", "0"), "Value (USD)": coin.get("usdValue", "0"), })
            return formatted_balances
        except (ValueError, TypeError, KeyError) as e:
            print(
                f"⚠️  Could not format wallet data: {e} | Data: {wallet_data}")
            return []


class CallbackHandler:
    """Receives WebSocket messages, processes them, and updates Google Sheets."""

    def __init__(self, sheets_service: GoogleSheetsService):
        self.sheets = sheets_service
        self.logged_exec_ids = set()
        # self.open_orders = {}  # NEW: State tracking for open orders
        self.open_positions = {}

    def handle_execution(self, message: Dict):
        try:
            for execution in message.get("data", []):
                exec_id = execution.get("execId")
                if exec_id in self.logged_exec_ids:
                    continue
                formatted_exec = DataProcessor.format_execution(execution)
                if formatted_exec:
                    self.sheets.append_data(
                        "Real-Time Log", [formatted_exec], headers=DataProcessor.EXECUTION_HEADERS)
                    self.logged_exec_ids.add(exec_id)
                    print(
                        f"✅ Logged execution: {formatted_exec['Symbol']} {formatted_exec['Side']}")
        except Exception as e:
            print(f"Error handling execution: {e}")

    # # Handler for the order stream
    # def handle_order(self, message: Dict):
    #     updated = False
    #     try:
    #         for order_data in message.get("data", []):
    #             order_id = order_data.get("orderId")
    #             if not order_id:
    #                 continue

    #             formatted_order = DataProcessor.format_order(order_data)

    #             # If the order is active, add/update it in our state
    #             if formatted_order:
    #                 self.open_orders[order_id] = formatted_order
    #                 updated = True
    #             # If it's not active, remove it from our state if it exists
    #             elif order_id in self.open_orders:
    #                 del self.open_orders[order_id]
    #                 updated = True

    #         if updated:
    #             self.sheets.overwrite_data("Live Orders", list(
    #                 self.open_orders.values()), headers=DataProcessor.ORDER_HEADERS)
    #             print(f"🔄 Synced {len(self.open_orders)} open orders.")
    #     except Exception as e:
    #         print(f"Error handling order update: {e}")

    def handle_position(self, message: Dict):
        updated = False
        try:
            for position in message.get("data", []):
                symbol = position.get("symbol")
                if not symbol:
                    continue
                formatted_pos = DataProcessor.format_position(position)
                if formatted_pos:
                    self.open_positions[symbol] = formatted_pos
                    updated = True
                elif symbol in self.open_positions:
                    del self.open_positions[symbol]
                    updated = True
            if updated:
                self.sheets.overwrite_data("Live Open Positions", list(
                    self.open_positions.values()), headers=DataProcessor.POSITION_HEADERS)
                print(f"🔄 Synced {len(self.open_positions)} open positions.")
        except Exception as e:
            print(f"Error handling position update: {e}")

    def handle_wallet(self, message: Dict):
        try:
            wallet_data = message.get("data", [{}])[0]
            if not wallet_data:
                return
            formatted_balances = DataProcessor.format_wallet(wallet_data)
            self.sheets.overwrite_data(
                "Live Wallet Balance", formatted_balances, headers=DataProcessor.WALLET_HEADERS)
            print(f"💰 Synced {len(formatted_balances)} asset balances.")
        except Exception as e:
            print(f"Error handling wallet update: {e}")


class PyBitRealTimeLogger:
    """Main application class to orchestrate the WebSocket logger."""

    def __init__(self):
        print("🚀 Initializing PyBit Real-Time Logger...")
        self.sheets_service = GoogleSheetsService()
        self.callback_handler = CallbackHandler(self.sheets_service)
        self.websocket_service = WebSocketService(self.callback_handler)

    def start(self):
        print("=" * 50)
        try:
            Config.validate()
            if not self.sheets_service.test_connection():
                return
            if self.websocket_service.start_streams():
                print("\n🎉 Real-time logging is now ACTIVE!")
                print("   - Executions will be appended to 'Real-Time Log'")
                # print("   - Pending orders will be synced to 'Live Orders'")  # NEW
                print("   - Open positions will be synced to 'Live Open Positions'")
                print("   - Wallet balances will be synced to 'Live Wallet Balance'")
                print(
                    f"\n🔗 View your sheet at: {self.sheets_service.get_spreadsheet_url()}")
                print("\nPress Ctrl-C to stop the logger.")
                while True:
                    time.sleep(1)
        except KeyboardInterrupt:
            print("\n⏹️ User requested shutdown. Stopping logger...")
        finally:
            self.websocket_service.stop_streams()
            print("Logger has been stopped.")


if __name__ == "__main__":
    logger = PyBitRealTimeLogger()
    logger.start()
