from datetime import datetime
from typing import Dict, List, Optional


class DataProcessor:
    """Service for processing trading data into spreadsheet format"""

    @staticmethod
    def process_futures_data(positions: List[Dict], wallet_balance: Optional[Dict] = None) -> List[Dict]:
        """Process futures positions into spreadsheet format with enhanced timing"""
        if not positions:
            return []

        print(f"🔄 Processing {len(positions)} futures positions...")

        # Get total wallet balance for risk calculation
        total_balance = DataProcessor._extract_total_balance(wallet_balance)

        futures_data = []

        for position in positions:
            try:
                # Use enhanced timing data if available
                if position.get('hasExecutionMatch'):
                    # Use actual execution times
                    created_time = datetime.fromtimestamp(
                        int(position['actualOpenTime'])/1000)
                    updated_time = datetime.fromtimestamp(
                        int(position['actualCloseTime'])/1000)
                    hold_duration_str = position.get(
                        'actualHoldDuration', 'Unknown')
                    timing_source = "Execution Match"
                else:
                    # Fallback to position creation/update times
                    created_time = datetime.fromtimestamp(
                        int(position['createdTime'])/1000)
                    updated_time = datetime.fromtimestamp(
                        int(position['updatedTime'])/1000)
                    hold_duration = updated_time - created_time
                    hold_duration_str = str(hold_duration)
                    timing_source = "Position Times"

                # Calculate position value and risk percentage
                qty = float(position.get('qty', 0))
                entry_price = float(position.get('avgEntryPrice', 0))
                entry_value = qty * entry_price

                # Risk calculation using supervisor's formula: stop_loss / wallet_balance * 100
                # Since we don't have stop_loss data yet, we'll approximate with potential loss
                # TODO: Update this when we get stop_loss data from orders
                risk_percentage = (entry_value / total_balance *
                                   100) if total_balance > 0 else 0

                # Calculate PnL percentage
                exit_price = float(position.get('avgExitPrice', 0))
                pnl = float(position.get('closedPnl', 0))
                pnl_percentage = (pnl / entry_value *
                                  100) if entry_value > 0 else 0

                # Calculate total fees
                open_fee = float(position.get('openFee', 0))
                close_fee = float(position.get('closeFee', 0))
                total_fee = open_fee + close_fee

                futures_data.append({
                    "Symbol": position.get('symbol', ''),
                    "Open Time": created_time.strftime('%Y-%m-%d %H:%M:%S'),
                    "Close Time": updated_time.strftime('%Y-%m-%d %H:%M:%S'),
                    "Hold Duration": hold_duration_str,
                    "Initial Capital": f"{total_balance:.2f}" if total_balance > 0 else "Unknown",
                    "Risk% to Wallet": f"{risk_percentage:.2f}%",
                    "Profit/Loss": position.get('closedPnl', ''),
                    "Fee Cost": f"{total_fee:.6f}",
                    "Updated": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })

            except Exception as e:
                print(
                    f"⚠️  Error processing position {position.get('symbol', 'Unknown')}: {e}")
                continue

        print(f"✅ Processed {len(futures_data)} futures positions")
        return futures_data

    @staticmethod
    def process_spot_data(trades: List[Dict]) -> List[Dict]:
        """Process spot trades into spreadsheet format"""
        if not trades:
            return []

        print(f"🔄 Processing {len(trades)} spot trades...")

        spot_data = []

        for trade in trades:
            try:
                exec_time = datetime.fromtimestamp(int(trade['execTime'])/1000)
                qty = float(trade.get('execQty', 0))
                price = float(trade.get('execPrice', 0))
                total_value = qty * price

                spot_data.append({
                    "Time": exec_time.strftime('%Y-%m-%d %H:%M:%S'),
                    "Symbol": trade.get('symbol', ''),
                    "Side": trade.get('side', ''),
                    "Quantity": trade.get('execQty', ''),
                    "Price": trade.get('execPrice', ''),
                    "Total Value": f"{total_value:.4f}",
                    "Fee": trade.get('execFee', ''),
                    "Fee Currency": trade.get('feeCurrency', ''),
                    "Order ID": trade.get('orderId', ''),
                    "Execution ID": trade.get('execId', ''),
                    "Updated": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })

            except Exception as e:
                print(
                    f"⚠️  Error processing trade {trade.get('symbol', 'Unknown')}: {e}")
                continue

        print(f"✅ Processed {len(spot_data)} spot trades")
        return spot_data

    @staticmethod
    def process_wallet_flows(deposit_withdraw_data: Dict[str, List[Dict]]) -> List[Dict]:
        """Process deposits/withdrawals into spreadsheet format"""
        deposits = deposit_withdraw_data.get('deposits', [])
        withdrawals = deposit_withdraw_data.get('withdrawals', [])

        total_transactions = len(deposits) + len(withdrawals)
        if total_transactions == 0:
            return []

        print(
            f"🔄 Processing {len(deposits)} deposits and {len(withdrawals)} withdrawals...")

        flow_data = []

        # Process deposits (inflow)
        for deposit in deposits:
            try:
                # Handle different timestamp fields
                timestamp = deposit.get(
                    'successAt') or deposit.get('createTime')
                if timestamp:
                    deposit_time = datetime.fromtimestamp(int(timestamp)/1000)
                    time_str = deposit_time.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    time_str = 'Pending'

                flow_data.append({
                    "Time": time_str,
                    "Type": "Deposit (Inflow)",
                    "Coin": deposit.get('coin', ''),
                    "Amount": deposit.get('amount', ''),
                    "Status": deposit.get('status', ''),
                    "Method": "Deposit",
                    "Chain": deposit.get('chain', ''),
                    "TX ID": deposit.get('txID', ''),
                    "Updated": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })

            except Exception as e:
                print(f"⚠️  Error processing deposit: {e}")
                continue

        # Process withdrawals (outflow)
        for withdrawal in withdrawals:
            try:
                withdraw_time = datetime.fromtimestamp(
                    int(withdrawal['createTime'])/1000)

                flow_data.append({
                    "Time": withdraw_time.strftime('%Y-%m-%d %H:%M:%S'),
                    "Type": "Withdrawal (Outflow)",
                    "Coin": withdrawal.get('coin', ''),
                    "Amount": withdrawal.get('amount', ''),
                    "Status": withdrawal.get('status', ''),
                    "Method": "Withdrawal",
                    "Chain": withdrawal.get('chain', ''),
                    "TX ID": withdrawal.get('txID', ''),
                    "Updated": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })

            except Exception as e:
                print(f"⚠️  Error processing withdrawal: {e}")
                continue

        # Sort by time (most recent first)
        flow_data.sort(key=lambda x: x['Time'], reverse=True)

        print(f"✅ Processed {len(flow_data)} wallet transactions")
        return flow_data

    @staticmethod
    def process_wallet_balance(wallet_balance: Optional[Dict]) -> List[Dict]:
        """Process current wallet balance into spreadsheet format"""
        if not wallet_balance:
            return []

        print("🔄 Processing wallet balance...")

        balance_data = []

        try:
            if 'list' in wallet_balance:
                for account in wallet_balance['list']:
                    account_type = account.get('accountType', 'Unknown')

                    if 'coin' in account:
                        for coin in account['coin']:
                            # Safely convert balance values
                            try:
                                balance = float(coin.get('walletBalance', 0))
                            except (ValueError, TypeError):
                                balance = 0.0

                            if balance > 0:  # Only include coins with balance
                                try:
                                    available = float(
                                        coin.get('availableToWithdraw', 0))
                                except (ValueError, TypeError):
                                    available = 0.0

                                try:
                                    locked = float(coin.get('locked', 0))
                                except (ValueError, TypeError):
                                    locked = 0.0

                                usd_value = coin.get('usdValue', '0')
                                # Handle empty or invalid USD value
                                try:
                                    usd_val_float = float(
                                        usd_value) if usd_value else 0.0
                                    usd_val_str = f"{usd_val_float:.4f}"
                                except (ValueError, TypeError):
                                    usd_val_str = "0.0000"

                                balance_data.append({
                                    "Account Type": account_type,
                                    "Coin": coin.get('coin', ''),
                                    "Wallet Balance": f"{balance:.8f}",
                                    "Available Balance": f"{available:.8f}",
                                    "Locked/In Orders": f"{locked:.8f}",
                                    "USD Value": usd_val_str,
                                    "Updated": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                })

            print(f"✅ Processed {len(balance_data)} coin balances")

        except Exception as e:
            print(f"⚠️  Error processing wallet balance: {e}")

        return balance_data

    @staticmethod
    def _extract_total_balance(wallet_balance: Optional[Dict]) -> float:
        """Extract total USDT balance for risk calculations"""
        if not wallet_balance or 'list' not in wallet_balance:
            return 0.0

        try:
            for account in wallet_balance['list']:
                if 'coin' in account:
                    for coin in account['coin']:
                        if coin.get('coin') == 'USDT':
                            try:
                                return float(coin.get('walletBalance', 0))
                            except (ValueError, TypeError):
                                continue

            # If no USDT found, try to get total USD value
            total_usd = 0.0
            for account in wallet_balance['list']:
                if 'coin' in account:
                    for coin in account['coin']:
                        usd_value = coin.get('usdValue', '0')
                        try:
                            if usd_value and usd_value != '':
                                total_usd += float(usd_value)
                        except (ValueError, TypeError):
                            continue

            return total_usd

        except Exception as e:
            print(f"⚠️  Error extracting total balance: {e}")
            return 0.0

    @staticmethod
    def calculate_trading_stats(futures_data: List[Dict], spot_data: List[Dict], wallet_flows: List[Dict]) -> Dict:
        """Calculate summary statistics for the trading data"""
        stats = {
            'futures_count': len(futures_data),
            'spot_count': len(spot_data),
            'wallet_count': len(wallet_flows),
            'total_futures_pnl': 0.0,
            'winning_trades': 0,
            'losing_trades': 0,
            'total_fees': 0.0
        }

        # Calculate futures statistics
        for position in futures_data:
            try:
                pnl = float(position.get('Profit/Loss', 0))
                fee = float(position.get('Fee Cost', 0))

                stats['total_futures_pnl'] += pnl
                stats['total_fees'] += fee

                if pnl > 0:
                    stats['winning_trades'] += 1
                elif pnl < 0:
                    stats['losing_trades'] += 1

            except (ValueError, TypeError):
                continue

        # Calculate win rate
        total_trades = stats['winning_trades'] + stats['losing_trades']
        stats['win_rate'] = (stats['winning_trades'] /
                             total_trades * 100) if total_trades > 0 else 0

        return stats
