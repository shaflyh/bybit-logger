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
        internal_deposits = bybit.get_internal_deposit_records()
        internal_transfers = bybit.get_internal_transfer_records()
        # universal_transfers = bybit.get_universal_transfer_records()
        # convert_history = bybit.get_convert_history()

        # 3. Process Data
        print("\n🔄 Processing data for final logs...")
        futures_log_data = DataProcessor.process_futures_data(
            futures_positions, wallet_balance)
        spot_log_data = DataProcessor.process_spot_data(spot_trades)
        wallet_flows_data = DataProcessor.process_wallet_flows(
            deposit_withdraw, internal_deposits)
        internal_transfer_data = DataProcessor.process_internal_transfer_data(
            internal_transfers)
        # universal_transfer_data = DataProcessor.process_universal_transfer_data(
        #     universal_transfers)
        # convert_history_data = DataProcessor.process_convert_history_data(
        #     convert_history)

        # Process Portfolio Overview Data
        portfolio_overview_data = DataProcessor.process_portfolio_overview(
            futures_log_data, wallet_balance, wallet_flows_data, Config.PORTFOLIO_START_DATE
        )

        # Process Asset Allocation Data
        asset_allocation_data = DataProcessor.process_asset_allocation(
            wallet_balance)

        # 4. Update Google Sheets
        print("\n📤 Syncing data to Google Sheets...")

        # Overwrite the Portfolio Overview sheet
        if portfolio_overview_data:
            sheets.overwrite_portfolio_overview(
                portfolio_overview_data, asset_allocation_data)

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

        if internal_transfer_data:
            headers = list(internal_transfer_data[0].keys())
            sheets.overwrite_data(
                "Internal Transfers", internal_transfer_data, headers=headers)

        # if universal_transfer_data:
        #     headers = list(universal_transfer_data[0].keys())
        #     sheets.overwrite_data(
        #         "Universal Transfers", universal_transfer_data, headers=headers)

        # if convert_history_data:
        #     headers = list(convert_history_data[0].keys())
        #     sheets.overwrite_data(
        #         "Convert History", convert_history_data, headers=headers)

        # 5. Final Summary
        print("\n🎉 Sync completed successfully!")
        spreadsheet_url = sheets.get_spreadsheet_url()
        if spreadsheet_url:
            print(f"📄 View your spreadsheet at: {spreadsheet_url}")

        print("\n📈 Summary of data synced:")
        if portfolio_overview_data:
            print("   - Portfolio Overview: Updated with latest stats")
        if asset_allocation_data:
            print(
                f"   - Asset Allocation: {len(asset_allocation_data)} coins with balance")
        print(
            f"   - Futures History: {len(futures_log_data)} closed positions")
        print(f"   - Spot History: {len(spot_log_data)} trades")
        print(f"   - Wallet Flows: {len(wallet_flows_data)} transactions")
        print(
            f"   - Internal Transfers: {len(internal_transfer_data)} transfers")
        # print(
        #     f"   - Universal Transfers: {len(universal_transfer_data)} transfers")
        # print(
        #     f"   - Convert History: {len(convert_history_data)} conversions")

    except ValueError as e:
        print(f"❌ Configuration Error: {e}")
    except Exception as e:
        print(f"❌ An unexpected application error occurred: {e}")


if __name__ == "__main__":
    main()
