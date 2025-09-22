import time
import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from pybit.unified_trading import HTTP
from config import Config


class BybitService:
    """Service for interacting with Bybit API using PyBit library"""

    def __init__(self):
        self.session = HTTP(
            testnet=Config.BYBIT_USE_TESTNET,
            api_key=Config.BYBIT_API_KEY,
            api_secret=Config.BYBIT_API_SECRET
        )

        env_name = "TESTNET" if Config.BYBIT_USE_TESTNET else "MAINNET"
        print(f"🧪 Bybit Service initialized - {env_name}")

    def log_response(self, response: Dict, filename: str) -> None:
        """Log API response to JSON file in log directory"""
        log_dir = "log"
        os.makedirs(log_dir, exist_ok=True)

        filepath = f'{log_dir}/{filename}.json'
        with open(filepath, 'w') as f:
            json.dump(response, f, indent=2)
        print(f"📝 Response logged to {filepath}")

    def get_account_info(self) -> Optional[Dict]:
        """Get basic account information"""
        print("📊 Fetching account info...")
        try:
            response = self.session.get_account_info()
            if response.get('retCode') == 0:
                return response.get('result')
            else:
                print(f"❌ API Error: {response.get('retMsg')}")
                return None
        except Exception as e:
            print(f"❌ Error: {e}")
            return None

    def get_wallet_balance(self, account_type: str = "UNIFIED") -> Optional[Dict]:
        """Get wallet balance for specified account type"""
        print(f"💰 Fetching {account_type} wallet balance...")
        try:
            response = self.session.get_wallet_balance(
                accountType=account_type)
            if response.get('retCode') == 0:
                return response.get('result')
            else:
                print(f"❌ API Error: {response.get('retMsg')}")
                return None
        except Exception as e:
            print(f"❌ Error: {e}")
            return None

    def get_futures_executions_with_positions(self, days_back: Optional[int] = None) -> Tuple[List[Dict], List[Dict]]:
        """Get futures executions and closed positions with proper timing"""
        days_back = days_back or Config.FUTURES_DAYS_BACK
        print(f"⚡ Fetching futures data (last {days_back} days)...")

        start_time = int(
            (datetime.now() - timedelta(days=days_back)).timestamp() * 1000)

        # Get execution history for timing data
        executions = []
        try:
            response = self.session.get_executions(
                category="linear",
                startTime=str(start_time),
                limit=100
            )
            self.log_response(response, "executions")
            if response.get('retCode') == 0:
                executions = response.get('result', {}).get('list', [])
                print(f"✅ Found {len(executions)} futures executions")
        except Exception as e:
            print(f"⚠️  Error fetching executions: {e}")

        # Get closed positions for PnL data
        positions = []
        try:
            response = self.session.get_closed_pnl(
                category="linear",
                startTime=str(start_time),
                limit=100
            )
            self.log_response(response, "positions")
            if response.get('retCode') == 0:
                positions = response.get('result', {}).get('list', [])
                print(f"✅ Found {len(positions)} closed positions")
        except Exception as e:
            print(f"⚠️  Error fetching positions: {e}")

        return executions, positions

    def get_futures_positions(self, days_back: Optional[int] = None) -> List[Dict]:
        """Get enhanced futures positions with proper timing"""
        executions, positions = self.get_futures_executions_with_positions(
            days_back)
        enhanced_positions = self.match_executions_to_positions(
            executions, positions)
        return enhanced_positions

    def get_open_positions(self) -> List[Dict]:
        """Get current open positions"""
        print("📊 Fetching open positions...")
        try:
            response = self.session.get_positions(
                category="linear",
                settleCoin="USDT"
            )
            self.log_response(response, "open_positions")

            if response.get('retCode') == 0:
                all_positions = response.get('result', {}).get('list', [])
                # Filter only positions with actual size (not zero)
                open_positions = [
                    pos for pos in all_positions
                    if float(pos.get('size', 0)) != 0
                ]
                print(f"✅ Found {len(open_positions)} open positions")
                return open_positions
            else:
                print(f"❌ API Error: {response.get('retMsg')}")
                return []

        except Exception as e:
            print(f"❌ Error: {e}")
            return []

    def get_spot_trades(self, days_back: Optional[int] = None) -> List[Dict]:
        """Get spot trading history"""
        days_back = days_back or Config.SPOT_DAYS_BACK
        print(f"📈 Fetching spot trades (last {days_back} days)...")

        start_time = int(
            (datetime.now() - timedelta(days=days_back)).timestamp() * 1000)

        try:
            response = self.session.get_executions(
                category="spot",
                startTime=str(start_time),
                limit=100
            )

            if response.get('retCode') == 0:
                trades = response.get('result', {}).get('list', [])
                print(f"✅ Found {len(trades)} spot trades")
                return trades
            else:
                print(f"❌ API Error: {response.get('retMsg')}")
                return []

        except Exception as e:
            print(f"❌ Error: {e}")
            return []

    def get_deposit_withdraw_history(self, days_back: Optional[int] = None) -> Dict[str, List[Dict]]:
        """Get deposit and withdrawal history"""
        days_back = days_back or Config.WALLET_DAYS_BACK
        print(
            f"💸 Fetching deposit/withdrawal history (last {days_back} days)...")

        start_time = int(
            (datetime.now() - timedelta(days=days_back)).timestamp() * 1000)

        deposits = []
        withdrawals = []

        # Get deposits
        try:
            response = self.session.get_deposit_records(
                startTime=str(start_time),
                limit=50
            )
            if response.get('retCode') == 0:
                deposits = response.get('result', {}).get('rows', [])
        except Exception as e:
            print(f"⚠️  Error fetching deposits: {e}")

        # Get withdrawals
        try:
            response = self.session.get_withdrawal_records(
                startTime=str(start_time),
                limit=50
            )
            if response.get('retCode') == 0:
                withdrawals = response.get('result', {}).get('rows', [])
        except Exception as e:
            print(f"⚠️  Error fetching withdrawals: {e}")

        print(
            f"✅ Found {len(deposits)} deposits, {len(withdrawals)} withdrawals")

        return {
            "deposits": deposits,
            "withdrawals": withdrawals
        }

    def match_executions_to_positions(self, executions: List[Dict], positions: List[Dict]) -> List[Dict]:
        """Match executions to positions to calculate proper hold times"""
        print("🔄 Matching executions to positions for accurate timing...")

        enhanced_positions = []

        for position in positions:
            order_id = position.get('orderId')
            symbol = position.get('symbol')

            # Find all executions for this position
            # Try to match by order ID first, then by symbol and timing
            position_executions = []

            # Method 1: Match by order ID (most accurate)
            for exec in executions:
                if exec.get('orderId') == order_id:
                    position_executions.append(exec)

            # Method 2: If no exact order match, try symbol and time proximity
            if not position_executions and symbol:
                position_created_time = int(position.get('createdTime', 0))
                position_updated_time = int(position.get('updatedTime', 0))

                for exec in executions:
                    if exec.get('symbol') == symbol:
                        exec_time = int(exec.get('execTime', 0))
                        # Check if execution time is within reasonable range of position
                        if position_created_time - 60000 <= exec_time <= position_updated_time + 60000:
                            position_executions.append(exec)

            if position_executions:
                # Sort executions by time
                position_executions.sort(
                    key=lambda x: int(x.get('execTime', 0)))

                # Get first execution (position open) and last execution (position close)
                first_exec = position_executions[0]
                last_exec = position_executions[-1]

                # Calculate actual hold time
                open_time = datetime.fromtimestamp(
                    int(first_exec['execTime'])/1000)
                close_time = datetime.fromtimestamp(
                    int(last_exec['execTime'])/1000)
                hold_duration = close_time - open_time

                # Enhanced position data
                enhanced_position = position.copy()
                enhanced_position.update({
                    'actualOpenTime': first_exec['execTime'],
                    'actualCloseTime': last_exec['execTime'],
                    'actualHoldDuration': str(hold_duration),
                    'executionCount': len(position_executions),
                    'totalExecQty': sum(float(exec.get('execQty', 0)) for exec in position_executions),
                    'hasExecutionMatch': True
                })

                enhanced_positions.append(enhanced_position)
            else:
                # Fallback to original timing if no executions found
                fallback_position = position.copy()
                fallback_position.update({
                    'actualOpenTime': position.get('createdTime'),
                    'actualCloseTime': position.get('updatedTime'),
                    'actualHoldDuration': 'Unknown (no execution match)',
                    'executionCount': 0,
                    'totalExecQty': position.get('qty', 0),
                    'hasExecutionMatch': False
                })
                enhanced_positions.append(fallback_position)

        matched_count = sum(
            1 for pos in enhanced_positions if pos.get('hasExecutionMatch'))
        print(
            f"✅ Enhanced {len(enhanced_positions)} positions ({matched_count} with execution timing)")
        return enhanced_positions

    def test_connection(self) -> bool:
        """Test API connection and authentication"""
        print("🔧 Testing Bybit API connection...")

        try:
            # Test with server time (no auth needed)
            response = self.session.get_server_time()
            if response.get('retCode') == 0:
                print("✅ Server connection OK")
            else:
                print(f"❌ Server connection failed: {response}")
                return False
        except Exception as e:
            print(f"❌ Connection error: {e}")
            return False

        # Test authenticated endpoint
        account_info = self.get_account_info()
        if account_info:
            print("✅ API authentication successful")
            return True
        else:
            print("❌ API authentication failed")
            return False
