#!/usr/bin/env python3
"""
Bybit Trading Logger - Simple Main Application
"""

from config import Config
from bybit_service import BybitService
from google_sheets_service import GoogleSheetsService
from data_processor import DataProcessor


def main():
    """Simple main function to sync Bybit data to Google Sheets"""

    print("🚀 Bybit Trading Logger")
    print("=" * 50)

    try:
        # Validate configuration
        Config.validate()
        Config.print_config()

        # Initialize services
        print("\n🔧 Initializing services...")
        bybit = BybitService()
        sheets = GoogleSheetsService()

        # Connect to spreadsheet
        if not sheets.connect_to_spreadsheet():
            print("❌ Failed to connect to Google Sheets")
            return

        # Fetch data from Bybit
        print("\n📊 Fetching data from Bybit...")
        wallet_balance = bybit.get_wallet_balance()
        spot_trades = bybit.get_spot_trades()
        futures_positions = bybit.get_futures_positions()
        deposit_withdraw = bybit.get_deposit_withdraw_history()
        # print(wallet_balance)
        # print(spot_trades)
        # print(futures_positions)
        # print(deposit_withdraw)

        # Process data
        print("\n🔄 Processing data...")
        futures_data = DataProcessor.process_futures_data(
            futures_positions, wallet_balance)
        spot_data = DataProcessor.process_spot_data(spot_trades)
        wallet_flows = DataProcessor.process_wallet_flows(deposit_withdraw)
        wallet_balance_data = DataProcessor.process_wallet_balance(
            wallet_balance)

        # Update Google Sheets
        print("\n📤 Updating Google Sheets...")

        success_count = 0

        if futures_data:
            if sheets.update_worksheet_data("Futures Log", futures_data):
                success_count += 1

        if spot_data:
            if sheets.update_worksheet_data("Spot Log", spot_data):
                success_count += 1

        if wallet_flows:
            if sheets.update_worksheet_data("Wallet Flows", wallet_flows):
                success_count += 1

        if wallet_balance_data:
            if sheets.update_worksheet_data("Wallet Balance", wallet_balance_data):
                success_count += 1

        # Create summary
        stats = DataProcessor.calculate_trading_stats(
            futures_data, spot_data, wallet_flows)
        summary_data = sheets.create_summary_data(stats)

        if summary_data:
            if sheets.update_worksheet_data("Summary", summary_data):
                success_count += 1

        # Results
        print(f"\n🎉 Sync completed! Updated {success_count} sheets")

        spreadsheet_url = sheets.get_spreadsheet_url()
        if spreadsheet_url:
            print(f"📄 Spreadsheet: {spreadsheet_url}")

        # Print summary
        print(f"\n📈 Summary:")
        print(f"   Futures: {len(futures_data)} positions")
        print(f"   Spot: {len(spot_data)} trades")
        print(f"   Wallet: {len(wallet_flows)} transactions")

    except ValueError as e:
        print(f"❌ Configuration error: {e}")
        print("\n📝 Please create a .env file with:")
        print("BYBIT_API_KEY=your_key")
        print("BYBIT_API_SECRET=your_secret")
        print("GOOGLE_SPREADSHEET_ID=your_spreadsheet_id")

    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    main()
