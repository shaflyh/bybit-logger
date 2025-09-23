#!/usr/bin/env python3
"""
PyBit WebSocket Real-Time Trading Logger for Bybit
Focuses on real-time execution logging for trading analysis
"""

import json
import time
from datetime import datetime
from typing import Dict, List, Optional
from pybit.unified_trading import WebSocket

from config import Config
from google_sheets_service import GoogleSheetsService


class WebSocketService:
    """Handle PyBit WebSocket connections and subscriptions"""

    def __init__(self, callback_handler):
        self.callback_handler = callback_handler
        self.ws = None
        self.is_connected = False

        # Initialize PyBit WebSocket
        self.ws = WebSocket(
            testnet=Config.BYBIT_USE_TESTNET,
            channel_type="private",
            api_key=Config.BYBIT_API_KEY,
            api_secret=Config.BYBIT_API_SECRET
        )

        env_name = "TESTNET" if Config.BYBIT_USE_TESTNET else "MAINNET"
        print(f"PyBit WebSocket initialized - {env_name}")

    def start_streams(self):
        """Start execution stream only"""
        try:
            # Subscribe to execution stream only
            print("Starting execution stream...")
            self.ws.execution_stream(
                callback=self.callback_handler.handle_execution)

            self.is_connected = True
            print("WebSocket execution stream started successfully")

        except Exception as e:
            print(f"Error starting WebSocket stream: {e}")
            return False

        return True

    def stop_streams(self):
        """Stop WebSocket streams"""
        if self.ws:
            try:
                self.ws.exit()
                print("WebSocket stream stopped")
            except Exception as e:
                print(f"Error stopping stream: {e}")


class DataProcessor:
    """Process WebSocket data into spreadsheet format"""

    @staticmethod
    def format_execution_for_sheets(execution: Dict) -> Optional[Dict]:
        """Format execution data for real-time futures log"""
        try:
            # Extract key fields
            symbol = execution.get("symbol", "")
            side = execution.get("side", "")
            exec_qty = execution.get("execQty", "0")
            exec_price = execution.get("execPrice", "0")
            exec_fee = execution.get("execFee", "0")
            exec_time = execution.get("execTime", "0")
            exec_type = execution.get("execType", "")
            exec_pnl = execution.get("execPnl", "0")
            order_type = execution.get("orderType", "")
            is_maker = execution.get("isMaker", False)
            category = execution.get("category", "")

            # Skip non-trade executions (funding, etc.)
            if exec_type not in ["Trade"]:
                return None

            # Calculate position value for this execution
            try:
                qty = float(exec_qty)
                price = float(exec_price)
                position_value = qty * price
            except (ValueError, TypeError):
                position_value = 0.0

            # Format timestamp
            try:
                exec_datetime = datetime.fromtimestamp(int(exec_time)/1000)
                formatted_time = exec_datetime.strftime(
                    '%Y-%m-%d %H:%M:%S.%f')[:-3]
            except (ValueError, TypeError):
                formatted_time = datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S.%f')[:-3]

            # Determine if this is opening or closing trade
            trade_type = "Close" if float(exec_pnl) != 0 else "Open"

            return {
                "Timestamp": formatted_time,
                "Trade Type": trade_type,
                "Symbol": symbol,
                "Side": side,
                "Quantity": exec_qty,
                "Price": exec_price,
                "Position Value": f"{position_value:.2f}",
                "Fee Cost": exec_fee,
                "Profit/Loss": exec_pnl if trade_type == "Close" else "0",
                "Order Type": order_type,
                "Maker/Taker": "Maker" if is_maker else "Taker",
                "Category": category,
                "Execution ID": execution.get("execId", ""),
                "Logged At": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

        except Exception as e:
            print(f"Error formatting execution: {e}")
            return None

    @staticmethod
    def format_position_for_sheets(position: Dict) -> Optional[Dict]:
        """Format position data for Google Sheets"""
        try:
            symbol = position.get("symbol", "")
            side = position.get("side", "")
            size = position.get("size", "0")
            entry_price = position.get("entryPrice", "0")
            mark_price = position.get("markPrice", "0")
            unrealized_pnl = position.get("unrealisedPnl", "0")
            leverage = position.get("leverage", "")
            position_value = position.get("positionValue", "0")
            updated_time = position.get("updatedTime", "0")
            category = position.get("category", "")

            # Skip empty positions
            if float(size) == 0:
                return None

            # Format timestamp
            try:
                update_datetime = datetime.fromtimestamp(
                    int(updated_time)/1000)
                formatted_time = update_datetime.strftime(
                    '%Y-%m-%d %H:%M:%S.%f')[:-3]
            except (ValueError, TypeError):
                formatted_time = datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S.%f')[:-3]

            return {
                "Timestamp": formatted_time,
                "Category": category,
                "Symbol": symbol,
                "Side": side,
                "Size": size,
                "Entry Price": entry_price,
                "Mark Price": mark_price,
                "Position Value": position_value,
                "Unrealized PnL": unrealized_pnl,
                "Leverage": leverage,
                "Updated At": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

        except Exception as e:
            print(f"Error formatting position: {e}")
            return None


class CallbackHandler:
    """Handle WebSocket callbacks and log to Google Sheets"""

    def __init__(self, sheets_service: GoogleSheetsService):
        self.sheets_service = sheets_service
        self.logged_executions = set()

    def handle_execution(self, message):
        """Handle execution stream messages"""
        try:
            # PyBit wraps the message, extract the data
            if isinstance(message, dict) and 'data' in message:
                executions = message['data']
            else:
                print(f"Unexpected execution message format: {message}")
                return

            for execution in executions:
                exec_id = execution.get("execId")

                # Skip if already logged
                if exec_id in self.logged_executions:
                    continue

                # Process the execution
                processed_execution = DataProcessor.format_execution_for_sheets(
                    execution)

                if processed_execution:
                    # Log to Google Sheets
                    success = self.log_execution_to_sheets(processed_execution)

                    if success:
                        self.logged_executions.add(exec_id)
                        symbol = execution.get('symbol', 'Unknown')
                        side = execution.get('side', 'Unknown')
                        qty = execution.get('execQty', '0')
                        print(f"✅ Logged execution: {symbol} {side} {qty}")
                    else:
                        print(f"❌ Failed to log execution: {exec_id}")

        except Exception as e:
            print(f"Error handling execution: {e}")

    def log_execution_to_sheets(self, execution_data: Dict) -> bool:
        """Log execution to Real-Time Futures Log"""
        try:
            sheet_name = "Real-Time Futures Log"
            return self._log_to_sheet(sheet_name, execution_data)
        except Exception as e:
            print(f"Error logging execution to sheets: {e}")
            return False

    def _log_to_sheet(self, sheet_name: str, data: Dict) -> bool:
        """Generic method to log data to specified sheet"""
        try:
            # Connect to spreadsheet if not already connected
            if not self.sheets_service.spreadsheet:
                if not self.sheets_service.connect_to_spreadsheet():
                    print("Failed to connect to Google Sheets")
                    return False

            # Get or create worksheet
            worksheet = self.sheets_service.get_or_create_worksheet(sheet_name)
            if not worksheet:
                return False

            # Check if sheet is empty (needs headers)
            existing_data = worksheet.get_all_values()
            if not existing_data:
                # Add headers
                headers = list(data.keys())
                worksheet.append_row(headers)

                # Format headers
                if Config.ENABLE_FORMATTING:
                    worksheet.format('1:1', {
                        'textFormat': {'bold': True},
                        'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9}
                    })

            # Add the data
            row_data = list(data.values())
            worksheet.append_row(row_data)

            return True

        except Exception as e:
            print(f"Error updating worksheet {sheet_name}: {e}")
            return False


class PyBitRealTimeLogger:
    """Main real-time logger using PyBit WebSocket"""

    def __init__(self):
        # Initialize services
        self.sheets_service = GoogleSheetsService()
        self.callback_handler = CallbackHandler(self.sheets_service)
        self.websocket_service = WebSocketService(self.callback_handler)

        print("PyBit Real-Time Logger initialized")

    def load_historical_data(self):
        """Load historical data using API before starting real-time streams"""
        print("\n📚 Loading historical data...")

        try:
            # Get recent data to populate initial context
            futures_positions = self.bybit_service.get_futures_positions(
                days_back=1)
            open_positions = self.bybit_service.get_open_positions()

            print(f"Loaded {len(futures_positions)} recent closed positions")
            print(f"Loaded {len(open_positions)} current open positions")

            return True

        except Exception as e:
            print(f"Error loading historical data: {e}")
            return False

    def start(self):
        """Start the real-time logger"""
        print("🔴 Starting PyBit Real-Time Trading Logger")
        print("=" * 50)

        try:
            # Validate configuration
            Config.validate()

            # Test Google Sheets connection
            if not self.sheets_service.test_connection():
                print("Failed to connect to Google Sheets. Check your credentials.")
                return

            # Load historical data first (optional)
            self.load_historical_data()

            # Start WebSocket streams
            print("\n📡 Starting real-time WebSocket stream...")
            if not self.websocket_service.start_streams():
                print("Failed to start WebSocket stream")
                return

            print("\n✅ Real-time logging active!")
            print("📊 Execution updates → 'Real-Time Executions' sheet")
            print("Press Ctrl+C to stop")

            # Keep the main thread alive
            while True:
                time.sleep(1)

        except KeyboardInterrupt:
            print("\n⏹️ Stopping real-time logger...")
            self.websocket_service.stop_streams()
            print("Real-time logger stopped")

        except Exception as e:
            print(f"Error: {e}")
            self.websocket_service.stop_streams()


def main():
    """Main entry point"""
    try:
        logger = PyBitRealTimeLogger()
        logger.start()

    except ValueError as e:
        print(f"Configuration error: {e}")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
