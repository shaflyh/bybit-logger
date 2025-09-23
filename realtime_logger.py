#!/usr/bin/env python3
"""
PyBit WebSocket Real-Time Trading Logger for Bybit

Connects to Bybit's private WebSocket streams to provide real-time updates
to a Google Sheet. It manages three separate sheets:
1.  Real-Time Log: A detailed, append-only log of every trade execution.
2.  Live Open Positions: A snapshot of all current open futures positions.
3.  Live Wallet Balance: A snapshot of current asset balances.
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
            self.ws.execution_stream(callback=self.callback_handler.handle_execution)
            print("   - Subscribed to 'execution' stream.")
            self.ws.position_stream(callback=self.callback_handler.handle_position)
            print("   - Subscribed to 'position' stream.")
            self.ws.wallet_stream(callback=self.callback_handler.handle_wallet)
            print("   - Subscribed to 'wallet' stream.")
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

    EXECUTION_HEADERS = [
        "Timestamp", "Category", "Symbol", "Side", 
        "PnL", "Fee",
        "Exec Price", "Exec Qty", "Exec Value", 
        "Create Type", "Order Type", "Maker/Taker", 
        "OrderID", "ExecutionID"
    ]
    POSITION_HEADERS = [ "Symbol", "Side", "Size", "Entry Price", "Mark Price", "Value (USD)", "Unrealized PnL", "Leverage", "Liq. Price", "Updated" ]
    WALLET_HEADERS = [ "Coin", "Balance", "Available", "Value (USD)" ]

    @staticmethod
    def format_execution(execution: Dict) -> Optional[Dict]:
        """Formats an execution message with a supervisor-focused column order."""
        try:
            if execution.get("execType") != "Trade": return None
            
            exec_price = float(execution.get("execPrice", 0))
            exec_qty = float(execution.get("execQty", 0))
            exec_value = float(execution.get("execValue", 0))
            fee = float(execution.get("execFee", 0))
            pnl = float(execution.get("execPnl", 0))

            return {
                "Timestamp": datetime.fromtimestamp(int(execution.get("execTime", 0)) / 1000).strftime('%Y-%m-%d %H:%M:%S'),
                "Category": execution.get("category", "").capitalize(),
                "Symbol": execution.get("symbol", ""),
                "Side": execution.get("side", ""),
                "PnL": f"{pnl:.8f}",
                "Fee": f"{fee:.8f}",
                "Exec Price": f"{exec_price:.4f}",
                "Exec Qty": f"{exec_qty:.6f}".rstrip('0').rstrip('.'),
                "Exec Value": f"{exec_value:.4f}",
                "Create Type": execution.get("createType", "N/A"),
                "Order Type": execution.get("orderType", ""),
                "Maker/Taker": "Maker" if execution.get("isMaker") else "Taker",
                "OrderID": execution.get("orderId", ""),
                "ExecutionID": execution.get("execId", ""),
            }
        except (ValueError, TypeError, KeyError) as e:
            print(f"⚠️  Could not format execution: {e} | Data: {execution}")
            return None

    @staticmethod
    def format_position(position: Dict) -> Optional[Dict]:
        try:
            if float(position.get("size", "0")) == 0: return None
            return { "Symbol": position.get("symbol", ""), "Side": position.get("side", ""), "Size": position.get("size", "0"), "Entry Price": position.get("entryPrice", "0"), "Mark Price": position.get("markPrice", "0"), "Value (USD)": position.get("positionValue", "0"), "Unrealized PnL": position.get("unrealisedPnl", "0"), "Leverage": position.get("leverage", ""), "Liq. Price": position.get("liqPrice", "0"), "Updated": datetime.fromtimestamp(int(position.get("updatedTime", 0)) / 1000).strftime('%H:%M:%S'), }
        except (ValueError, TypeError, KeyError) as e:
            print(f"⚠️  Could not format position: {e} | Data: {position}")
            return None

    @staticmethod
    def format_wallet(wallet_data: Dict) -> List[Dict]:
        formatted_balances = []
        try:
            for coin in wallet_data.get('coin', []):
                if float(coin.get("walletBalance", 0)) > 0:
                    formatted_balances.append({ "Coin": coin.get("coin", ""), "Balance": coin.get("walletBalance", "0"), "Available": coin.get("availableToWithdraw", "0"), "Value (USD)": coin.get("usdValue", "0"), })
            return formatted_balances
        except (ValueError, TypeError, KeyError) as e:
            print(f"⚠️  Could not format wallet data: {e} | Data: {wallet_data}")
            return []


class CallbackHandler:
    """Receives WebSocket messages, processes them, and updates Google Sheets."""

    def __init__(self, sheets_service: GoogleSheetsService):
        self.sheets = sheets_service
        self.logged_exec_ids = set()
        self.open_positions = {}

    def handle_execution(self, message: Dict):
        try:
            for execution in message.get("data", []):
                # print(execution)  # Debug print to see the execution data
                exec_id = execution.get("execId")
                if exec_id in self.logged_exec_ids: continue
                formatted_exec = DataProcessor.format_execution(execution)
                if formatted_exec:
                    self.sheets.append_data("Real-Time Log", [formatted_exec], headers=DataProcessor.EXECUTION_HEADERS)
                    self.logged_exec_ids.add(exec_id)
                    print(f"✅ Logged execution: {formatted_exec['Symbol']} {formatted_exec['Side']}")
        except Exception as e:
            print(f"Error handling execution: {e}")

    def handle_position(self, message: Dict):
        updated = False
        try:
            for position in message.get("data", []):
                symbol = position.get("symbol")
                if not symbol: continue
                formatted_pos = DataProcessor.format_position(position)
                if formatted_pos:
                    self.open_positions[symbol] = formatted_pos
                    updated = True
                elif symbol in self.open_positions:
                    del self.open_positions[symbol]
                    updated = True
            if updated:
                self.sheets.overwrite_data("Live Open Positions", list(self.open_positions.values()), headers=DataProcessor.POSITION_HEADERS)
                print(f"🔄 Synced {len(self.open_positions)} open positions.")
        except Exception as e:
            print(f"Error handling position update: {e}")

    def handle_wallet(self, message: Dict):
        try:
            wallet_data = message.get("data", [{}])[0]
            if not wallet_data: return
            formatted_balances = DataProcessor.format_wallet(wallet_data)
            self.sheets.overwrite_data("Live Wallet Balance", formatted_balances, headers=DataProcessor.WALLET_HEADERS)
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
                print("❌ Aborting due to Google Sheets connection failure.")
                return
            if self.websocket_service.start_streams():
                print("\n🎉 Real-time logging is now ACTIVE!")
                print("   - Executions will be appended to 'Real-Time Log'")
                print("   - Open positions will be synced to 'Live Open Positions'")
                print("   - Wallet balances will be synced to 'Live Wallet Balance'")
                print(f"\n🔗 View your sheet at: {self.sheets_service.get_spreadsheet_url()}")
                print("\nPress Ctrl-C to stop the logger.")
                while True: time.sleep(1)
        except KeyboardInterrupt:
            print("\n⏹️ User requested shutdown. Stopping logger...")
        finally:
            self.websocket_service.stop_streams()
            print("Logger has been stopped.")


if __name__ == "__main__":
    logger = PyBitRealTimeLogger()
    logger.start()