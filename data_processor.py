from datetime import datetime, timedelta
from typing import Dict, List, Optional


class DataProcessor:
    """
    Service for processing HISTORICAL trading data from the Bybit API
    into final, supervisor-approved spreadsheet formats.
    """

    @staticmethod
    def process_futures_data(positions: List[Dict], wallet_balance: Optional[Dict]) -> List[Dict]:
        """
        Processes closed futures positions to create the 'Futures History' which
        fulfills the supervisor's requirements for a simple, clean report.
        """
        if not positions:
            return []

        print(
            f"   - Processing {len(positions)} futures positions for 'Futures History'...")

        # Use the most recent total account equity as the 'Initial Capital'
        total_balance = DataProcessor._extract_total_balance(wallet_balance)

        futures_log = []
        for position in positions:
            try:
                # Requirement: Trade opened and closed time & How long it was held
                # Note: Uses 'actual' times from the execution matching logic for accuracy
                open_time_ms = int(position.get('actualOpenTime', 0))
                close_time_ms = int(position.get('actualCloseTime', 0))
                open_time = datetime.fromtimestamp(open_time_ms / 1000)
                close_time = datetime.fromtimestamp(close_time_ms / 1000)
                hold_duration = close_time - open_time

                # Requirement: Profit / loss & Fee cost
                pnl = float(position.get('closedPnl', 0))
                # Bybit API provides a single 'orderFee' field that sums up open and close fees for a position.
                total_fee_cost = float(position.get('orderFee', 0))

                # Requirement: Initial Capital & Risk% to Wallet
                entry_value = float(position.get('maxTradeValue', position.get(
                    'execValue', 0)))  # Use max value if available
                risk_percent = (entry_value / total_balance *
                                100) if total_balance > 0 else 0

                futures_log.append({
                    "Open Time": open_time.strftime('%Y-%m-%d %H:%M:%S'),
                    "Close Time": close_time.strftime('%Y-%m-%d %H:%M:%S'),
                    # Cleanly formatted duration
                    "Hold Duration": str(timedelta(seconds=round(hold_duration.total_seconds()))),
                    "Symbol": position.get('symbol', ''),
                    # The 'side' in a closed PnL record is the side of the closing trade.
                    # So, the opening trade was the opposite.
                    "Side": "Buy" if position.get('side') == "Sell" else "Sell",
                    "Initial Capital (Wallet)": f"{total_balance:.2f}",
                    "Position Value": f"{entry_value:.2f}",
                    "Risk % to Wallet": f"{risk_percent:.2f}%",
                    "Profit / Loss": f"{pnl:.4f}",
                    "Fee Cost": f"{total_fee_cost:.4f}",
                })
            except Exception as e:
                print(
                    f"⚠️  Error processing futures position {position.get('symbol', 'Unknown')}: {e}")
                continue

        # Sort by most recently closed positions first
        return sorted(futures_log, key=lambda x: x['Close Time'], reverse=True)

    @staticmethod
    def process_spot_data(trades: List[Dict]) -> List[Dict]:
        """
        Processes spot executions to create the 'Spot History' (Basic trade log requirement).
        """
        if not trades:
            return []

        print(f"   - Processing {len(trades)} trades for 'Spot History'...")

        spot_log = []
        for trade in trades:
            try:
                exec_time = datetime.fromtimestamp(int(trade['execTime'])/1000)
                qty = float(trade.get('execQty', 0))
                price = float(trade.get('execPrice', 0))
                value = qty * price

                spot_log.append({
                    "Time": exec_time.strftime('%Y-%m-%d %H:%M:%S'),
                    "Symbol": trade.get('symbol', ''),
                    "Side": trade.get('side', ''),
                    "Quantity": f"{qty:.6f}".rstrip('0').rstrip('.'),
                    "Price": f"{price:.4f}",
                    "Total Value": f"{value:.4f}",
                    "Fee": trade.get('execFee', ''),
                    "Fee Currency": trade.get('feeCurrency', ''),
                })
            except Exception as e:
                print(
                    f"⚠️  Error processing spot trade {trade.get('symbol', 'Unknown')}: {e}")
                continue

        return sorted(spot_log, key=lambda x: x['Time'], reverse=True)

    @staticmethod
    def process_wallet_flows(deposit_withdraw_data: Dict[str, List[Dict]]) -> List[Dict]:
        """
        Processes deposits/withdrawals for the 'Wallet Flows' log (Inflow/Outflow requirement).
        """
        all_flows = deposit_withdraw_data.get(
            'deposits', []) + deposit_withdraw_data.get('withdrawals', [])
        if not all_flows:
            return []

        print(
            f"   - Processing {len(all_flows)} transactions for 'Wallet Flows'...")

        flow_data = []
        for flow in all_flows:
            # Determine if it's a deposit or withdrawal by checking for a unique key.
            is_deposit = 'depositId' in flow

            try:
                # The final timestamp for a transaction is 'successAt' for deposits and 'updatedTime' for withdrawals
                timestamp_ms = int(flow.get('successAt')
                                   or flow.get('updatedTime', 0))
                timestamp = datetime.fromtimestamp(
                    timestamp_ms/1000).strftime('%Y-%m-%d %H:%M:%S')

                flow_data.append({
                    "Time": timestamp,
                    "Type": "Deposit (Inflow)" if is_deposit else "Withdrawal (Outflow)",
                    "Coin": flow.get('coin', ''),
                    "Amount": flow.get('amount', ''),
                    "Status": flow.get('status', ''),
                    "Chain": flow.get('chain', ''),
                    "TX ID": flow.get('txID', ''),
                })
            except Exception as e:
                print(f"⚠️  Error processing wallet flow: {e}")
                continue

        return sorted(flow_data, key=lambda x: x['Time'], reverse=True)

    @staticmethod
    def _extract_total_balance(wallet_balance: Optional[Dict]) -> float:
        """
        Helper to extract the total account equity in USD. This is the most
        reliable measure of total wallet balance for risk calculations.
        """
        if not wallet_balance or 'list' not in wallet_balance:
            return 0.0
        try:
            # The 'totalEquity' field from the UNIFIED account type is the best measure.
            return float(wallet_balance['list'][0].get('totalEquity', 0.0))
        except (ValueError, TypeError, IndexError) as e:
            print(f"⚠️  Could not extract total balance from wallet data: {e}")
            return 0.0
