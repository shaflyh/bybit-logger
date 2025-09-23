from datetime import datetime, timedelta
from typing import Dict, List, Optional


class DataProcessor:
    """
    Service for processing HISTORICAL trading data from the Bybit API
    into final, supervisor-approved spreadsheet formats.
    """

    @staticmethod
    def process_futures_data(positions: List[Dict], wallet_balance: Optional[Dict] = None) -> List[Dict]:
        """
        Processes closed futures positions to create a clean and ACCURATE 'Futures History' log
        based on reliably available API data.
        """
        if not positions:
            return []

        print(
            f"   - Processing {len(positions)} futures positions for 'Futures History'...")

        futures_log = []
        for position in positions:
            try:
                # Open/Close Time & Hold Duration from execution matching
                open_time_ms = int(position.get('actualOpenTime', 0))
                close_time_ms = int(position.get('actualCloseTime', 0))
                open_time = datetime.fromtimestamp(open_time_ms / 1000)
                close_time = datetime.fromtimestamp(close_time_ms / 1000)
                hold_duration = close_time - open_time

                # Final PnL from the API summary
                pnl = float(position.get('closedPnl', 0))

                # Total fee cost from the API summary (Note: often 0 on testnet)
                total_fee_cost = float(position.get('orderFee', 0))

                futures_log.append({
                    "Open Time": open_time.strftime('%Y-%m-%d %H:%M:%S'),
                    "Close Time": close_time.strftime('%Y-%m-%d %H:%M:%S'),
                    "Hold Duration": str(timedelta(seconds=round(hold_duration.total_seconds()))),
                    "Symbol": position.get('symbol', ''),
                    "Side": "Buy" if position.get('side') == "Sell" else "Sell",
                    "Profit / Loss": f"{pnl:.4f}",
                    "Fee Cost": f"{total_fee_cost:.4f}",
                })
            except Exception as e:
                print(
                    f"⚠️  Error processing futures position {position.get('symbol', 'Unknown')}: {e}")
                continue

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
            is_deposit = 'depositId' in flow
            try:
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
