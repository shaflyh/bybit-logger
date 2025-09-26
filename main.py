# main.py

from config import Config
from bybit_service import BybitService
from google_sheets_service import GoogleSheetsService
from data_processor import DataProcessor


def main():
    """Main function to fetch, process, and sync Bybit data."""

    print("🚀 Bybit Historical Trading Logger")
    print("=" * 50)

    try:
        # 1. Initialization
        Config.validate()
        Config.print_config()

        print("\n🔧 Initializing services...")
        bybit = BybitService()
        sheets = GoogleSheetsService()

        if not sheets.connect_to_spreadsheet():
            print("❌ Aborting due to Google Sheets connection failure.")
            return

        # 2. Fetch Data from Bybit
        print("\n📊 Fetching data from Bybit...")
        wallet_balance = bybit.get_wallet_balance()
        spot_trades = bybit.get_spot_trades()
        futures_positions = bybit.get_futures_positions()
        deposit_withdraw = bybit.get_deposit_withdraw_history()

        # 3. Process Data
        print("\n🔄 Processing data for final logs...")
        futures_log_data = DataProcessor.process_futures_data(
            futures_positions, wallet_balance)
        spot_log_data = DataProcessor.process_spot_data(spot_trades)
        wallet_flows_data = DataProcessor.process_wallet_flows(
            deposit_withdraw)

        # Process Portfolio Overview Data
        portfolio_overview_data = DataProcessor.process_portfolio_overview(
            futures_log_data, wallet_balance, wallet_flows_data, Config.DAYS_BACK
        )

        # 4. Update Google Sheets
        print("\n📤 Syncing data to Google Sheets...")

        # Overwrite the Portfolio Overview sheet
        if portfolio_overview_data:
            sheets.overwrite_portfolio_overview(portfolio_overview_data)

        if futures_log_data:
            headers = list(futures_log_data[0].keys())
            sheets.overwrite_data(
                "Futures History", futures_log_data, headers=headers)

        if spot_log_data:
            headers = list(spot_log_data[0].keys())
            sheets.overwrite_data(
                "Spot History", spot_log_data, headers=headers)

        if wallet_flows_data:
            headers = list(wallet_flows_data[0].keys())
            sheets.overwrite_data(
                "Wallet Flows", wallet_flows_data, headers=headers)

        # 5. Final Summary
        print("\n🎉 Sync completed successfully!")
        spreadsheet_url = sheets.get_spreadsheet_url()
        if spreadsheet_url:
            print(f"📄 View your spreadsheet at: {spreadsheet_url}")

        print("\n📈 Summary of data synced:")
        if portfolio_overview_data:
            print("   - Portfolio Overview: Updated with latest stats.")
        print(
            f"   - Futures History: {len(futures_log_data)} closed positions")
        print(f"   - Spot History: {len(spot_log_data)} trades")
        print(f"   - Wallet Flows: {len(wallet_flows_data)} transactions")

    except ValueError as e:
        print(f"❌ Configuration Error: {e}")
    except Exception as e:
        print(f"❌ An unexpected application error occurred: {e}")


if __name__ == "__main__":
    main()
