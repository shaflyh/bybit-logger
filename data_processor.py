from datetime import datetime, timedelta
from typing import Dict, List, Optional


class DataProcessor:
    """
    Service for processing HISTORICAL trading data from the Bybit API
    """

    @staticmethod
    def process_portfolio_overview(
        futures_history: List[Dict],
        wallet_balance_unified: Optional[Dict],
        wallet_balance_fund: Optional[Dict],
        portfolio_start_date: str
    ) -> Optional[Dict]:
        """
        Calculates high-level portfolio overview metrics.
        Filters futures_history to only include trades closed within the portfolio date range.

        Args:
            wallet_balance_unified: UNIFIED account wallet balance
            wallet_balance_fund: FUND (Funding) wallet balance
        """
        if not wallet_balance_unified:
            return None

        # 1. Get Current Balance (Unified) and Total Balance (Unified + Fund)
        unified_balance = float(
            wallet_balance_unified['list'][0].get('totalEquity', 0))

        # Calculate Funding wallet balance (sum of all coin USD values)
        fund_balance = 0.0
        if wallet_balance_fund and wallet_balance_fund.get('list'):
            fund_coins = wallet_balance_fund['list'][0].get('coin', [])
            fund_balance = sum(float(coin.get('usdValue', 0))
                               for coin in fund_coins)

        # 2. Total Balance = UNIFIED + FUND
        total_balance = unified_balance + fund_balance

        # 3. Parse portfolio start date and filter futures history
        try:
            start_date = datetime.strptime(portfolio_start_date, "%Y-%m-%d")
        except ValueError:
            print(f"⚠️  Invalid PORTFOLIO_START_DATE format. Using default (7 days back)")
            start_date = datetime.now() - timedelta(days=7)

        end_date = datetime.now()

        # Filter futures_history to only include trades closed within portfolio date range
        filtered_futures = []
        for trade in futures_history:
            try:
                # Parse close time from the trade (format: "Sep-15 08:44")
                close_time_str = trade.get('Close Time', '')
                if close_time_str:
                    # Add current year to the close time for proper parsing
                    close_time = datetime.strptime(
                        f"{close_time_str} {end_date.year}", "%b-%d %H:%M %Y")

                    # Handle year boundary (if close time is in future, it's from previous year)
                    if close_time > end_date:
                        close_time = close_time.replace(year=end_date.year - 1)

                    # Only include trades closed within portfolio date range
                    if start_date <= close_time <= end_date:
                        filtered_futures.append(trade)
            except (ValueError, KeyError) as e:
                # If parsing fails, skip this trade
                print(
                    f"⚠️  Warning: Could not parse close time for trade: {e}")
                continue

        # 4. Calculate Net PnL and Win Rate from filtered trades
        net_pnl = 0
        winning_trades = 0
        total_trades = len(filtered_futures)

        if total_trades > 0:
            for trade in filtered_futures:
                pnl = float(trade.get('PnL', 0))
                if pnl > 0:
                    winning_trades += 1
                net_pnl += pnl

            win_rate = (winning_trades /
                        total_trades) if total_trades > 0 else 0
        else:
            win_rate = 0

        # Calculate the date range display using portfolio start date
        date_range_str = f"{start_date.strftime('%b %d, %Y')} - {end_date.strftime('%b %d, %Y')}"

        overview_data = {
            "Total Balance": f"{total_balance:.2f}",
            "Current Balance": f"{unified_balance:.2f}",
            "Net PnL": f"{net_pnl:+.2f}",
            "Win Rate": win_rate,
            "Date Range": date_range_str,  # Use the newly calculated range
            "Notes": {
                "Total Balance": f"Unified (${unified_balance:.2f}) + Fund (${fund_balance:.2f})",
                "Current Balance": "Unified trading account",
                "Net PnL": f"Across {total_trades} trades",
                "Win Rate": f"{winning_trades} wins / {total_trades} trades"
            }
        }

        return overview_data

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

                pnl = float(position.get('closedPnl', 0))
                total_fee_cost = float(position.get(
                    'openFee', 0)) + float(position.get('closeFee', 0))
                leverage = position.get('leverage', '1')
                total_hours = hold_duration.total_seconds() / 3600
                hours_held_simplified = f"{total_hours:.1f}"

                # Simplified time format: "Sep-15 08:44"
                open_time_simplified = open_time.strftime('%b-%d %H:%M')
                close_time_simplified = close_time.strftime('%b-%d %H:%M')

                futures_log.append({
                    "Symbol": position.get('symbol', ''),
                    "Side": "Buy" if position.get('side') == "Sell" else "Sell",
                    "Lev": leverage,
                    "PnL": f"{pnl:.4f}",
                    "Fee": f"{total_fee_cost:.4f}",
                    "Hours Held": hours_held_simplified,
                    "Open Time": open_time_simplified,
                    "Close Time": close_time_simplified,
                    "_close_time_sort": close_time,
                })
            except Exception as e:
                print(
                    f"⚠️  Error processing futures position {position.get('symbol', 'Unknown')}: {e}")
                continue

        sorted_log = sorted(
            futures_log, key=lambda x: x['_close_time_sort'], reverse=True)

        for entry in sorted_log:
            entry.pop('_close_time_sort', None)

        return sorted_log

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
                    "Symbol": trade.get('symbol', ''),
                    "Side": trade.get('side', ''),
                    "Quantity": f"{qty:.6f}".rstrip('0').rstrip('.'),
                    "Price": f"{price:.4f}",
                    "Total Value": f"{value:.4f}",
                    "Fee": trade.get('execFee', ''),
                    "Fee Currency": trade.get('feeCurrency', ''),
                    "Time": exec_time.strftime('%Y-%m-%d %H:%M:%S'),
                })
            except Exception as e:
                print(
                    f"⚠️  Error processing spot trade {trade.get('symbol', 'Unknown')}: {e}")
                continue

        return sorted(spot_log, key=lambda x: x['Time'], reverse=True)

    @staticmethod
    def process_wallet_flows(deposit_withdraw_data: Dict[str, List[Dict]], internal_deposits: List[Dict] = None) -> List[Dict]:
        """
        Processes deposits/withdrawals for the 'Wallet Flows' log (Inflow/Outflow requirement).
        Combines on-chain deposits/withdrawals with internal (off-chain) deposits.
        """
        all_flows = deposit_withdraw_data.get(
            'deposits', []) + deposit_withdraw_data.get('withdrawals', [])

        # Add internal deposits if provided
        if internal_deposits:
            all_flows.extend(internal_deposits)

        if not all_flows:
            return []

        print(
            f"   - Processing {len(all_flows)} transactions for 'Wallet Flows'...")

        flow_data = []
        for flow in all_flows:
            # Check if it's an internal deposit (has 'type' field with value 1)
            is_internal_deposit = flow.get('type') == 1

            # 'successAt' is present for on-chain deposits, but not for withdrawals or internal deposits
            is_onchain_deposit = 'successAt' in flow

            # Determine direction and network
            if is_internal_deposit:
                direction = "Deposit"
                network = "Off-chain"
            elif is_onchain_deposit:
                direction = "Deposit"
                network = "On-chain"
            else:
                direction = "Withdrawal"
                network = "On-chain"

            try:
                # Get timestamp based on transaction type
                if is_internal_deposit:
                    # Internal deposits use 'createdTime' (in seconds, not milliseconds)
                    timestamp_ms = int(flow.get('createdTime', 0)) * 1000
                else:
                    # On-chain: use 'successAt' for deposits or 'updateTime' for withdrawals
                    timestamp_ms = int(flow.get('successAt')
                                       or flow.get('updateTime', 0))

                # Handle cases where the timestamp might still be zero
                if timestamp_ms == 0:
                    print(
                        f"⚠️  Warning: Found a wallet transaction with a zero timestamp. Skipping.")
                    continue

                timestamp = datetime.fromtimestamp(
                    timestamp_ms/1000).strftime('%Y-%m-%d %H:%M:%S')

                # Get status and normalize it
                status = flow.get('status', '')

                # Internal deposits use numeric status codes
                if is_internal_deposit and isinstance(status, int):
                    internal_status_map = {
                        1: 'Processing',
                        2: 'Success',
                        3: 'Failed'
                    }
                    status = internal_status_map.get(status, str(status))

                # On-chain deposits use numeric status codes
                elif is_onchain_deposit and isinstance(status, int):
                    onchain_deposit_status_map = {
                        0: 'Unknown',
                        1: 'To Be Confirmed',
                        2: 'Processing',
                        3: 'Success',
                        4: 'Failed',
                        10011: 'Pending Credit to Funding Pool',
                        10012: 'Credited to Funding Pool'
                    }
                    status = onchain_deposit_status_map.get(
                        status, str(status))

                # Withdrawals use string status - normalize capitalization
                elif isinstance(status, str):
                    status = status.capitalize()

                # For internal deposits, address is email/phone, otherwise it's blockchain chain
                if is_internal_deposit:
                    chain_or_address = flow.get('address', '')
                    tx_id = flow.get('txID', '')
                else:
                    chain_or_address = flow.get('chain', '')
                    tx_id = flow.get('txID', '')

                flow_data.append({
                    "Coin": flow.get('coin', ''),
                    "Direction": direction,
                    "Network": network,
                    "Amount": flow.get('amount', ''),
                    "Chain/Address": chain_or_address,
                    "Status": status,
                    "Time": timestamp,
                    "TX ID": tx_id,
                })
            except Exception as e:
                print(f"⚠️  Error processing wallet flow: {e}")
                continue

        return sorted(flow_data, key=lambda x: x['Time'], reverse=True)

    @staticmethod
    def process_internal_transfer_data(transfers: List[Dict]) -> List[Dict]:
        """
        Processes internal transfer records to create the 'Internal Transfers' log.
        """
        if not transfers:
            return []

        print(
            f"   - Processing {len(transfers)} internal transfers for 'Internal Transfers'...")

        transfer_log = []
        for transfer in transfers:
            try:
                timestamp_ms = int(transfer.get('timestamp', 0))
                if timestamp_ms == 0:
                    print(
                        "⚠️  Warning: Found a transfer with a zero timestamp. Skipping.")
                    continue

                timestamp = datetime.fromtimestamp(
                    timestamp_ms/1000).strftime('%Y-%m-%d %H:%M:%S')

                transfer_log.append({
                    "Time": timestamp,
                    "Coin": transfer.get('coin', ''),
                    "Amount": transfer.get('amount', ''),
                    "From Account": transfer.get('fromAccountType', ''),
                    "To Account": transfer.get('toAccountType', ''),
                    "Status": transfer.get('status', ''),
                })
            except Exception as e:
                print(f"⚠️  Error processing internal transfer: {e}")
                continue

        return sorted(transfer_log, key=lambda x: x['Time'], reverse=True)

    @staticmethod
    def process_universal_transfer_data(transfers: List[Dict]) -> List[Dict]:
        """
        Processes universal transfer records to create the 'Universal Transfers' log.
        """
        if not transfers:
            return []

        print(
            f"   - Processing {len(transfers)} universal transfers for 'Universal Transfers'...")

        transfer_log = []
        for transfer in transfers:
            try:
                timestamp_ms = int(transfer.get('timestamp', 0))
                if timestamp_ms == 0:
                    print(
                        "⚠️  Warning: Found a universal transfer with a zero timestamp. Skipping.")
                    continue

                timestamp = datetime.fromtimestamp(
                    timestamp_ms/1000).strftime('%Y-%m-%d %H:%M:%S')

                transfer_log.append({
                    "Time": timestamp,
                    "Transfer ID": transfer.get('transferId', ''),
                    "Coin": transfer.get('coin', ''),
                    "Amount": transfer.get('amount', ''),
                    "From Member ID": transfer.get('fromMemberId', ''),
                    "To Member ID": transfer.get('toMemberId', ''),
                    "From Account": transfer.get('fromAccountType', ''),
                    "To Account": transfer.get('toAccountType', ''),
                    "Status": transfer.get('status', ''),
                })
            except Exception as e:
                print(f"⚠️  Error processing universal transfer: {e}")
                continue

        return sorted(transfer_log, key=lambda x: x['Time'], reverse=True)

    @staticmethod
    def process_convert_history_data(conversions: List[Dict]) -> List[Dict]:
        """
        Processes convert history records to create the 'Convert History' log.
        """
        if not conversions:
            return []

        print(
            f"   - Processing {len(conversions)} conversions for 'Convert History'...")

        conversion_log = []
        for conversion in conversions:
            try:
                timestamp_ms = int(conversion.get('createdAt', 0))
                if timestamp_ms == 0:
                    print(
                        "⚠️  Warning: Found a conversion with a zero timestamp. Skipping.")
                    continue

                timestamp = datetime.fromtimestamp(
                    timestamp_ms/1000).strftime('%Y-%m-%d %H:%M:%S')

                # Format the conversion rate for better readability
                convert_rate = conversion.get('convertRate', '')
                if convert_rate:
                    try:
                        rate_float = float(convert_rate)
                        convert_rate = f"{rate_float:.8f}".rstrip(
                            '0').rstrip('.')
                    except ValueError:
                        pass

                conversion_log.append({
                    "Time": timestamp,
                    "Exchange TX ID": conversion.get('exchangeTxId', ''),
                    "From Coin": conversion.get('fromCoin', ''),
                    "To Coin": conversion.get('toCoin', ''),
                    "From Amount": conversion.get('fromAmount', ''),
                    "To Amount": conversion.get('toAmount', ''),
                    "Convert Rate": convert_rate,
                    "Account Type": conversion.get('accountType', ''),
                    "Status": conversion.get('exchangeStatus', ''),
                })
            except Exception as e:
                print(f"⚠️  Error processing conversion: {e}")
                continue

        return sorted(conversion_log, key=lambda x: x['Time'], reverse=True)

    @staticmethod
    def process_asset_allocation(wallet_balance_unified: Optional[Dict], wallet_balance_fund: Optional[Dict]) -> List[Dict]:
        """
        Processes wallet balance data from both UNIFIED and FUND wallets to create asset allocation data.
        Extracts coin information from both wallet balance responses.
        Returns list sorted by USD value (largest first) with percentage calculations based on combined total USD value.

        Args:
            wallet_balance_unified: UNIFIED account wallet balance
            wallet_balance_fund: FUND (Funding) wallet balance
        """
        assets_with_balance = []
        total_usd_value = 0.0

        # Process UNIFIED wallet coins
        if wallet_balance_unified:
            account_list = wallet_balance_unified.get('list', [])
            if account_list:
                unified_coins = account_list[0].get('coin', [])
                if unified_coins:
                    print(
                        f"   - Processing {len(unified_coins)} UNIFIED wallet coins...")
                    for coin in unified_coins:
                        try:
                            wallet_balance_value = float(
                                coin.get('walletBalance', 0))
                            usd_value = float(coin.get('usdValue', 0))

                            # Only include coins with positive balance
                            if wallet_balance_value > 0:
                                assets_with_balance.append({
                                    'coin': coin.get('coin', ''),
                                    'wallet': 'UNIFIED',
                                    'walletBalance': wallet_balance_value,
                                    'usdValue': usd_value
                                })
                                total_usd_value += usd_value
                        except (ValueError, TypeError) as e:
                            print(
                                f"⚠️  Error processing UNIFIED balance for {coin.get('coin', 'Unknown')}: {e}")
                            continue

        # Process FUND wallet coins
        if wallet_balance_fund:
            account_list = wallet_balance_fund.get('list', [])
            if account_list:
                fund_coins = account_list[0].get('coin', [])
                if fund_coins:
                    print(
                        f"   - Processing {len(fund_coins)} FUND wallet coins...")
                    for coin in fund_coins:
                        try:
                            wallet_balance_value = float(
                                coin.get('walletBalance', 0))
                            usd_value = float(coin.get('usdValue', 0))

                            # Only include coins with positive balance
                            if wallet_balance_value > 0:
                                assets_with_balance.append({
                                    'coin': coin.get('coin', ''),
                                    'wallet': 'FUND',
                                    'walletBalance': wallet_balance_value,
                                    'usdValue': usd_value
                                })
                                total_usd_value += usd_value
                        except (ValueError, TypeError) as e:
                            print(
                                f"⚠️  Error processing FUND balance for {coin.get('coin', 'Unknown')}: {e}")
                            continue

        # Calculate percentages based on combined USD value and format data
        asset_allocation = []
        for asset in assets_with_balance:
            percentage = (asset['usdValue'] /
                          total_usd_value * 100) if total_usd_value > 0 else 0
            asset_allocation.append({
                'Coin': asset['coin'],
                'Balance': f"{asset['walletBalance']:.8f}".rstrip('0').rstrip('.'),
                'USD Value': f"${asset['usdValue']:.2f}",
                'Percentage': f"{percentage:.2f}%",
                'Wallet': asset['wallet'],
                '_usd_value': asset['usdValue'],  # For sorting
                '_percentage_value': percentage  # For chart
            })

        # Sort by USD value (largest first)
        sorted_allocation = sorted(
            asset_allocation,
            key=lambda x: x['_usd_value'],
            reverse=True
        )

        print(
            f"   - Found {len(sorted_allocation)} coins with non-zero balance (Total: ${total_usd_value:.2f})")
        return sorted_allocation
