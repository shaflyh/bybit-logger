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
        log_dir = "logs"
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
        """
        Get wallet balance for specified account type.

        Args:
            account_type: Account type - "UNIFIED" (default), "FUND", "CONTRACT", or "SPOT"

        Returns:
            Wallet balance data or None if error
        """
        print(f"💰 Fetching {account_type} wallet balance...")
        try:
            response = self.session.get_wallet_balance(
                accountType=account_type)

            # Log the response to JSON
            self.log_response(
                response, f'wallet_balance_{account_type.lower()}')

            if response.get('retCode') == 0:
                return response.get('result')
            else:
                print(f"❌ API Error: {response.get('retMsg')}")
                return None
        except Exception as e:
            print(f"❌ Error: {e}")
            return None

    def get_funding_wallet_balance(self) -> Optional[Dict]:
        """Get funding wallet balance (convenience method)"""
        return self.get_wallet_balance(account_type="FUND")

    def get_futures_executions_with_positions(self, days_back: Optional[int] = None) -> Tuple[List[Dict], List[Dict]]:
        """Get futures executions and closed positions with proper timing"""
        days_back = days_back or Config.DAYS_BACK
        print(f"⚡ Fetching futures data (last {days_back} days)...")

        # Calculate date ranges in 7-day chunks
        end_time = datetime.now()
        start_time = end_time - timedelta(days=days_back)

        all_executions = []
        all_positions = []

        # Process in 7-day chunks
        current_start = start_time
        chunk_count = 0

        while current_start < end_time:
            chunk_count += 1
            current_end = min(current_start + timedelta(days=7), end_time)

            start_timestamp = int(current_start.timestamp() * 1000)
            end_timestamp = int(current_end.timestamp() * 1000)

            print(
                f"  📅 Chunk {chunk_count}: {current_start.strftime('%Y-%m-%d')} to {current_end.strftime('%Y-%m-%d')}")

            # Get execution history for this chunk
            try:
                response = self.session.get_executions(
                    category="linear",
                    startTime=str(start_timestamp),
                    endTime=str(end_timestamp),
                    limit=100
                )

                if response.get('retCode') == 0:
                    chunk_executions = response.get(
                        'result', {}).get('list', [])
                    all_executions.extend(chunk_executions)
                    if chunk_executions:
                        print(
                            f"    ✅ Found {len(chunk_executions)} executions in this chunk")
                else:
                    print(
                        f"    ⚠️  API Error for executions: {response.get('retMsg')}")
            except Exception as e:
                print(
                    f"    ⚠️  Error fetching executions for chunk {chunk_count}: {e}")

            # Get closed positions for this chunk
            try:
                response = self.session.get_closed_pnl(
                    category="linear",
                    startTime=str(start_timestamp),
                    endTime=str(end_timestamp),
                    limit=200
                )

                if response.get('retCode') == 0:
                    chunk_positions = response.get(
                        'result', {}).get('list', [])
                    all_positions.extend(chunk_positions)
                    if chunk_positions:
                        print(
                            f"    ✅ Found {len(chunk_positions)} positions in this chunk")
                else:
                    print(
                        f"    ⚠️  API Error for positions: {response.get('retMsg')}")
            except Exception as e:
                print(
                    f"    ⚠️  Error fetching positions for chunk {chunk_count}: {e}")

            # Move to next chunk
            current_start = current_end

            # Small delay to avoid rate limiting
            time.sleep(0.2)

        # Log final response
        self.log_response(all_executions, "executions")
        self.log_response(all_positions, "positions")

        print(
            f"✅ Total found: {len(all_executions)} futures executions, {len(all_positions)} closed positions")
        return all_executions, all_positions

    def get_futures_positions(self, days_back: Optional[int] = None) -> List[Dict]:
        """Get enhanced futures positions with proper timing"""
        executions, positions = self.get_futures_executions_with_positions(
            days_back)
        enhanced_positions = self.match_executions_to_positions(
            executions, positions)
        return enhanced_positions

    def get_spot_trades(self, days_back: Optional[int] = None) -> List[Dict]:
        """Get spot trading history starting from configured start date"""
        end_time = datetime.now()

        # Parse start date from config
        try:
            start_time = datetime.strptime(
                Config.SPOT_HISTORY_START_DATE, "%Y-%m-%d")
        except ValueError:
            print(
                f"⚠️  Invalid SPOT_HISTORY_START_DATE format. Using default (7 days back)")
            start_time = end_time - timedelta(days=7)

        # Calculate date range
        date_range_days = (end_time - start_time).days

        # Warn if exceeding API limit (2 years = 730 days)
        if date_range_days > 730:
            print(
                f"⚠️  WARNING: Date range ({date_range_days} days) exceeds Bybit API limit of 2 years (730 days)")
            print(f"⚠️  Limiting query to last 730 days from today")
            start_time = end_time - timedelta(days=730)
            date_range_days = 730

        print(
            f"📈 Fetching spot trades from {start_time.strftime('%Y-%m-%d')} to {end_time.strftime('%Y-%m-%d')} ({date_range_days} days)...")

        all_trades = []

        # Process in 7-day chunks
        current_start = start_time
        chunk_count = 0

        while current_start < end_time:
            chunk_count += 1
            current_end = min(current_start + timedelta(days=7), end_time)

            start_timestamp = int(current_start.timestamp() * 1000)
            end_timestamp = int(current_end.timestamp() * 1000)

            print(
                f"  📅 Chunk {chunk_count}: {current_start.strftime('%Y-%m-%d')} to {current_end.strftime('%Y-%m-%d')}")

            try:
                response = self.session.get_executions(
                    category="spot",
                    startTime=str(start_timestamp),
                    endTime=str(end_timestamp),
                    limit=100
                )

                if response.get('retCode') == 0:
                    chunk_trades = response.get('result', {}).get('list', [])
                    all_trades.extend(chunk_trades)
                    if chunk_trades:
                        print(
                            f"    ✅ Found {len(chunk_trades)} trades in this chunk")
                else:
                    print(f"    ⚠️  API Error: {response.get('retMsg')}")
            except Exception as e:
                print(
                    f"    ⚠️  Error fetching spot trades for chunk {chunk_count}: {e}")

            # Move to next chunk
            current_start = current_end

            # Small delay to avoid rate limiting
            time.sleep(0.2)

        print(f"✅ Total found: {len(all_trades)} spot trades")
        return all_trades

    def get_deposit_withdraw_history(self, days_back: Optional[int] = None) -> Dict[str, List[Dict]]:
        """Get deposit and withdrawal history"""
        days_back = 365
        print(
            f"💸 Fetching deposit/withdrawal history (last {days_back} days)...")

        # Calculate date ranges in 7-day chunks
        end_time = datetime.now()
        start_time = end_time - timedelta(days=days_back)

        all_deposits = []
        all_withdrawals = []

        # Process in 7-day chunks
        current_start = start_time
        chunk_count = 0

        while current_start < end_time:
            chunk_count += 1
            current_end = min(current_start + timedelta(days=30), end_time)

            start_timestamp = int(current_start.timestamp() * 1000)
            end_timestamp = int(current_end.timestamp() * 1000)

            print(
                f"  📅 Chunk {chunk_count}: {current_start.strftime('%Y-%m-%d')} to {current_end.strftime('%Y-%m-%d')}")

            # Get deposits for this chunk
            try:
                response = self.session.get_deposit_records(
                    startTime=str(start_timestamp),
                    endTime=str(end_timestamp),
                    limit=50
                )
                if response.get('retCode') == 0:
                    chunk_deposits = response.get('result', {}).get('rows', [])
                    all_deposits.extend(chunk_deposits)
                    if chunk_deposits:
                        print(
                            f"    ✅ Found {len(chunk_deposits)} deposits in this chunk")
                else:
                    print(
                        f"    ⚠️  API Error for deposits: {response.get('retMsg')}")
            except Exception as e:
                print(
                    f"    ⚠️  Error fetching deposits for chunk {chunk_count}: {e}")

            # Get withdrawals for this chunk
            try:
                response = self.session.get_withdrawal_records(
                    startTime=str(start_timestamp),
                    endTime=str(end_timestamp),
                    withdrawType=2,
                    limit=50
                )
                if response.get('retCode') == 0:
                    chunk_withdrawals = response.get(
                        'result', {}).get('rows', [])
                    all_withdrawals.extend(chunk_withdrawals)
                    if chunk_withdrawals:
                        print(
                            f"    ✅ Found {len(chunk_withdrawals)} withdrawals in this chunk")
                else:
                    print(
                        f"    ⚠️  API Error for withdrawals: {response.get('retMsg')}")
            except Exception as e:
                print(
                    f"    ⚠️  Error fetching withdrawals for chunk {chunk_count}: {e}")

            # Move to next chunk
            current_start = current_end

            # Small delay to avoid rate limiting
            time.sleep(0.2)

        print(
            f"✅ Total found: {len(all_deposits)} deposits, {len(all_withdrawals)} withdrawals")

        deposits_withdrawals = {
            "deposits": all_deposits,
            "withdrawals": all_withdrawals
        }

        self.log_response(all_deposits, "deposits")
        self.log_response(all_withdrawals, "withdrawals")

        return deposits_withdrawals

    def get_internal_deposit_records(self, days_back: Optional[int] = None) -> List[Dict]:
        """Get internal deposit records (off-chain deposits within Bybit platform)"""
        days_back = 365  # Use same timeframe as deposits/withdrawals
        print(
            f"💸 Fetching internal deposit records (last {days_back} days)...")

        # Calculate date ranges in 30-day chunks (API limit)
        end_time = datetime.now()
        start_time = end_time - timedelta(days=days_back)

        all_deposits = []

        # Process in 30-day chunks
        current_start = start_time
        chunk_count = 0

        while current_start < end_time:
            chunk_count += 1
            current_end = min(current_start + timedelta(days=30), end_time)

            start_timestamp = int(current_start.timestamp() * 1000)
            end_timestamp = int(current_end.timestamp() * 1000)

            print(
                f"  📅 Chunk {chunk_count}: {current_start.strftime('%Y-%m-%d')} to {current_end.strftime('%Y-%m-%d')}")

            try:
                response = self.session.get_internal_deposit_records(
                    startTime=str(start_timestamp),
                    endTime=str(end_timestamp),
                    limit=50
                )

                if response.get('retCode') == 0:
                    chunk_deposits = response.get('result', {}).get('rows', [])
                    all_deposits.extend(chunk_deposits)
                    if chunk_deposits:
                        print(
                            f"    ✅ Found {len(chunk_deposits)} internal deposits in this chunk")
                else:
                    print(f"    ⚠️  API Error: {response.get('retMsg')}")
            except Exception as e:
                print(
                    f"    ⚠️  Error fetching internal deposits for chunk {chunk_count}: {e}")

            # Move to next chunk
            current_start = current_end

            # Small delay to avoid rate limiting
            time.sleep(0.2)

        print(f"✅ Total found: {len(all_deposits)} internal deposits")

        # Log the response
        self.log_response({'result': {'rows': all_deposits}},
                          "internal_deposits")

        return all_deposits

    def get_internal_transfer_records(self, days_back: Optional[int] = None) -> List[Dict]:
        """Get internal transfer records between different account types"""
        days_back = days_back or Config.DAYS_BACK
        print(
            f"💸 Fetching internal transfer records (last {days_back} days)...")

        # Calculate date ranges in 7-day chunks
        end_time = datetime.now()
        start_time = end_time - timedelta(days=days_back)

        all_transfers = []

        # Process in 7-day chunks
        current_start = start_time
        chunk_count = 0

        while current_start < end_time:
            chunk_count += 1
            current_end = min(current_start + timedelta(days=7), end_time)

            start_timestamp = int(current_start.timestamp() * 1000)
            end_timestamp = int(current_end.timestamp() * 1000)

            print(
                f"  📅 Chunk {chunk_count}: {current_start.strftime('%Y-%m-%d')} to {current_end.strftime('%Y-%m-%d')}")

            try:
                response = self.session.get_internal_transfer_records(
                    startTime=str(start_timestamp),
                    endTime=str(end_timestamp),
                    limit=50
                )

                if response.get('retCode') == 0:
                    chunk_transfers = response.get(
                        'result', {}).get('list', [])
                    all_transfers.extend(chunk_transfers)
                    if chunk_transfers:
                        print(
                            f"    ✅ Found {len(chunk_transfers)} transfers in this chunk")
                else:
                    print(f"    ⚠️  API Error: {response.get('retMsg')}")
            except Exception as e:
                print(
                    f"    ⚠️  Error fetching internal transfers for chunk {chunk_count}: {e}")

            # Move to next chunk
            current_start = current_end

            # Small delay to avoid rate limiting
            time.sleep(0.2)

        print(f"✅ Total found: {len(all_transfers)} internal transfers")

        # Log the response
        self.log_response({'result': {'list': all_transfers}},
                          "internal_transfers")

        return all_transfers

    def get_universal_transfer_records(self, days_back: Optional[int] = None) -> List[Dict]:
        """Get universal transfer records between different UIDs"""
        days_back = days_back or Config.DAYS_BACK
        print(
            f"🌐 Fetching universal transfer records (last {days_back} days)...")

        # Calculate date ranges in 7-day chunks
        end_time = datetime.now()
        start_time = end_time - timedelta(days=days_back)

        all_transfers = []

        # Process in 7-day chunks
        current_start = start_time
        chunk_count = 0

        while current_start < end_time:
            chunk_count += 1
            current_end = min(current_start + timedelta(days=7), end_time)

            start_timestamp = int(current_start.timestamp() * 1000)
            end_timestamp = int(current_end.timestamp() * 1000)

            print(
                f"  📅 Chunk {chunk_count}: {current_start.strftime('%Y-%m-%d')} to {current_end.strftime('%Y-%m-%d')}")

            try:
                response = self.session.get_universal_transfer_records(
                    startTime=str(start_timestamp),
                    endTime=str(end_timestamp),
                    limit=50
                )

                if response.get('retCode') == 0:
                    chunk_transfers = response.get(
                        'result', {}).get('list', [])
                    all_transfers.extend(chunk_transfers)
                    if chunk_transfers:
                        print(
                            f"    ✅ Found {len(chunk_transfers)} universal transfers in this chunk")
                else:
                    print(f"    ⚠️  API Error: {response.get('retMsg')}")
            except Exception as e:
                print(
                    f"    ⚠️  Error fetching universal transfers for chunk {chunk_count}: {e}")

            # Move to next chunk
            current_start = current_end

            # Small delay to avoid rate limiting
            time.sleep(0.2)

        print(f"✅ Total found: {len(all_transfers)} universal transfers")

        # Log the response
        self.log_response({'result': {'list': all_transfers}},
                          "universal_transfers")

        return all_transfers

    def get_convert_history(self) -> List[Dict]:
        """Get convert history using pagination"""
        print("🔄 Fetching convert history...")

        all_conversions = []
        page = 1
        limit = 100  # Maximum allowed per API documentation

        while True:
            print(f"  📄 Fetching page {page}...")

            try:
                response = self.session.get_convert_history(
                    index=page,
                    limit=limit
                )

                if response.get('retCode') == 0:
                    result = response.get('result', {})
                    conversions = result.get('list', [])

                    if not conversions:
                        print(
                            f"    ✅ No more conversions found. Finished at page {page}")
                        break

                    all_conversions.extend(conversions)
                    print(
                        f"    ✅ Found {len(conversions)} conversions on page {page}")

                    # If we got less than the limit, we've reached the end
                    if len(conversions) < limit:
                        print(
                            f"    ✅ Reached end of data (got {len(conversions)} < {limit})")
                        break

                    page += 1
                else:
                    print(f"    ⚠️  API Error: {response.get('retMsg')}")
                    break
            except Exception as e:
                print(
                    f"    ⚠️  Error fetching convert history page {page}: {e}")
                break

            # Small delay to avoid rate limiting
            time.sleep(0.2)

        print(
            f"✅ Total found: {len(all_conversions)} conversions across {page} pages")

        # Log the response
        self.log_response(
            {'result': {'list': all_conversions}}, "convert_history")

        return all_conversions

    def match_executions_to_positions(self, executions: List[Dict], positions: List[Dict]) -> List[Dict]:
        """Match executions to positions to calculate proper hold times"""
        print("🔄 Matching executions to positions for accurate timing...")

        enhanced_positions = []

        for position in positions:
            position_order_id = position.get('orderId')
            symbol = position.get('symbol')
            side = position.get('side')
            qty = float(position.get('qty', 0))
            avg_entry_price = float(position.get('avgEntryPrice', 0))
            avg_exit_price = float(position.get('avgExitPrice', 0))

            # Find all executions for this symbol around the position time
            position_created_time = int(position.get('createdTime', 0))
            position_updated_time = int(position.get('updatedTime', 0))

            # Get all executions for this symbol within a reasonable time window
            # Extend the window to capture opening positions that might be hours/days earlier
            time_window = 7 * 24 * 60 * 60 * 1000  # 7 days in milliseconds
            symbol_executions = []

            for exec in executions:
                if exec.get('symbol') == symbol:
                    exec_time = int(exec.get('execTime', 0))
                    # Include executions within the time window
                    if position_created_time - time_window <= exec_time <= position_updated_time + 60000:
                        symbol_executions.append(exec)

            if symbol_executions:
                # Sort executions by time
                symbol_executions.sort(key=lambda x: int(x.get('execTime', 0)))

                # For futures positions, we need to find the opening and closing executions
                # The position side tells us what the CLOSING action was
                # So if position side is "Sell", the opening was "Buy" and vice versa
                opening_side = "Buy" if side == "Sell" else "Sell"
                closing_side = side

                # Find opening executions (opposite side)
                opening_executions = [
                    exec for exec in symbol_executions
                    if exec.get('side') == opening_side and exec.get('execType') == 'Trade'
                ]

                # Find closing executions (same side as position)
                closing_executions = [
                    exec for exec in symbol_executions
                    if exec.get('side') == closing_side and exec.get('execType') == 'Trade'
                ]

                # Try to match by order ID first for closing execution
                closing_exec = None
                for exec in closing_executions:
                    if exec.get('orderId') == position_order_id:
                        closing_exec = exec
                        break

                # If no exact order ID match, use the closest execution by price or time
                if not closing_exec and closing_executions:
                    # Find execution closest to avg exit price
                    closest_exec = min(
                        closing_executions,
                        key=lambda x: abs(
                            float(x.get('execPrice', 0)) - avg_exit_price)
                    )
                    closing_exec = closest_exec

                # For opening execution, find the one closest to avg entry price
                opening_exec = None
                if opening_executions:
                    closest_exec = min(
                        opening_executions,
                        key=lambda x: abs(
                            float(x.get('execPrice', 0)) - avg_entry_price)
                    )
                    opening_exec = closest_exec

                # Calculate hold time if we have both opening and closing
                if opening_exec and closing_exec:
                    open_time = datetime.fromtimestamp(
                        int(opening_exec['execTime'])/1000)
                    close_time = datetime.fromtimestamp(
                        int(closing_exec['execTime'])/1000)
                    hold_duration = close_time - open_time

                    # Enhanced position data
                    enhanced_position = position.copy()
                    enhanced_position.update({
                        'actualOpenTime': opening_exec['execTime'],
                        'actualCloseTime': closing_exec['execTime'],
                        'actualHoldDuration': str(hold_duration),
                        'executionCount': len(symbol_executions),
                        'totalExecQty': sum(float(exec.get('execQty', 0)) for exec in symbol_executions),
                        'hasExecutionMatch': True,
                        'openingOrderId': opening_exec.get('orderId'),
                        'closingOrderId': closing_exec.get('orderId'),
                        'matchMethod': 'Price+Time Match'
                    })

                    enhanced_positions.append(enhanced_position)
                    continue

            # Fallback to original timing if no good match found
            fallback_position = position.copy()
            created_time = datetime.fromtimestamp(
                int(position.get('createdTime', 0))/1000)
            updated_time = datetime.fromtimestamp(
                int(position.get('updatedTime', 0))/1000)
            hold_duration = updated_time - created_time

            fallback_position.update({
                'actualOpenTime': position.get('createdTime'),
                'actualCloseTime': position.get('updatedTime'),
                'actualHoldDuration': str(hold_duration) if hold_duration.total_seconds() > 1 else 'Very Short Trade',
                'executionCount': len(symbol_executions),
                'totalExecQty': position.get('qty', 0),
                'hasExecutionMatch': False,
                'matchMethod': 'Position Times (Fallback)'
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
