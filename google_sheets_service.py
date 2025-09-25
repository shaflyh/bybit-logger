import os
import time
import gspread
from gspread.utils import ValueInputOption
from google.oauth2.service_account import Credentials
from typing import Dict, List, Optional
from config import Config


class GoogleSheetsService:
    """Service for interacting with Google Sheets API"""

    def __init__(self):
        self.gc = None
        self.spreadsheet = None
        self.setup_connection()

    def setup_connection(self) -> bool:
        try:
            if not os.path.exists(Config.GOOGLE_CREDENTIALS_FILE):
                print(
                    f"❌ Google credentials file not found: {Config.GOOGLE_CREDENTIALS_FILE}")
                return False
            scope = ["https://www.googleapis.com/auth/spreadsheets",
                     "https://www.googleapis.com/auth/drive"]
            creds = Credentials.from_service_account_file(
                Config.GOOGLE_CREDENTIALS_FILE, scopes=scope)
            self.gc = gspread.authorize(creds)
            print("✅ Google Sheets API connected")
            return True
        except Exception as e:
            print(f"❌ Google Sheets setup error: {e}")
            return False

    def connect_to_spreadsheet(self) -> bool:
        if not self.gc:
            return False
        try:
            if Config.GOOGLE_SPREADSHEET_ID:
                self.spreadsheet = self.gc.open_by_key(
                    Config.GOOGLE_SPREADSHEET_ID)
                print(
                    f"✅ Connected to spreadsheet by ID: {self.spreadsheet.title}")
                return True
            # ... [rest of the function is the same]
        except Exception as e:
            print(f"❌ Error connecting to spreadsheet: {e}")
            return False
        return False

    def format_headers(self, worksheet: gspread.Worksheet):
        if not Config.ENABLE_FORMATTING:
            return
        try:
            worksheet.format('1:1', {'textFormat': {'bold': True}, 'backgroundColor': {
                             'red': 0.9, 'green': 0.9, 'blue': 0.9}})
        except Exception as e:
            print(f"⚠️  Could not apply header formatting: {e}")

    def apply_conditional_formatting(self, worksheet: gspread.Worksheet, headers: List[str]):
        """Apply conditional formatting for Side and PnL columns using efficient batch operations"""
        if not Config.ENABLE_FORMATTING:
            return

        try:
            # Get column indices for Side and PnL
            side_col_idx = None
            pnl_col_idx = None

            for i, header in enumerate(headers):
                if header == "Side":
                    side_col_idx = i + 1  # gspread uses 1-based indexing
                elif header == "PnL":
                    pnl_col_idx = i + 1

            # Get all data at once to minimize API calls
            all_data = worksheet.get_all_values()
            if len(all_data) < 2:  # No data rows to format
                return

            # Group cells by color to create ranges
            buy_ranges = []
            sell_ranges = []
            profit_ranges = []
            loss_ranges = []

            # Process Side column
            if side_col_idx:
                side_col_letter = chr(64 + side_col_idx)
                current_buy_start = None
                current_sell_start = None

                # Skip header
                for row_idx, row_data in enumerate(all_data[1:], start=2):
                    side_value = row_data[side_col_idx -
                                          1] if len(row_data) > side_col_idx - 1 else ""

                    if side_value == "Buy":
                        if current_buy_start is None:
                            current_buy_start = row_idx
                        # Close any ongoing sell range
                        if current_sell_start is not None:
                            sell_ranges.append(
                                f"{side_col_letter}{current_sell_start}:{side_col_letter}{row_idx - 1}")
                            current_sell_start = None
                    elif side_value == "Sell":
                        if current_sell_start is None:
                            current_sell_start = row_idx
                        # Close any ongoing buy range
                        if current_buy_start is not None:
                            buy_ranges.append(
                                f"{side_col_letter}{current_buy_start}:{side_col_letter}{row_idx - 1}")
                            current_buy_start = None
                    else:
                        # Close both ranges if we hit a different value
                        if current_buy_start is not None:
                            buy_ranges.append(
                                f"{side_col_letter}{current_buy_start}:{side_col_letter}{row_idx - 1}")
                            current_buy_start = None
                        if current_sell_start is not None:
                            sell_ranges.append(
                                f"{side_col_letter}{current_sell_start}:{side_col_letter}{row_idx - 1}")
                            current_sell_start = None

                # Close any remaining ranges
                last_row = len(all_data)
                if current_buy_start is not None:
                    buy_ranges.append(
                        f"{side_col_letter}{current_buy_start}:{side_col_letter}{last_row}")
                if current_sell_start is not None:
                    sell_ranges.append(
                        f"{side_col_letter}{current_sell_start}:{side_col_letter}{last_row}")

            # Process PnL column
            if pnl_col_idx:
                pnl_col_letter = chr(64 + pnl_col_idx)
                current_profit_start = None
                current_loss_start = None

                # Skip header
                for row_idx, row_data in enumerate(all_data[1:], start=2):
                    pnl_value = row_data[pnl_col_idx -
                                         1] if len(row_data) > pnl_col_idx - 1 else ""

                    try:
                        pnl_float = float(pnl_value)
                        if pnl_float > 0:
                            if current_profit_start is None:
                                current_profit_start = row_idx
                            # Close any ongoing loss range
                            if current_loss_start is not None:
                                loss_ranges.append(
                                    f"{pnl_col_letter}{current_loss_start}:{pnl_col_letter}{row_idx - 1}")
                                current_loss_start = None
                        elif pnl_float < 0:
                            if current_loss_start is None:
                                current_loss_start = row_idx
                            # Close any ongoing profit range
                            if current_profit_start is not None:
                                profit_ranges.append(
                                    f"{pnl_col_letter}{current_profit_start}:{pnl_col_letter}{row_idx - 1}")
                                current_profit_start = None
                        else:
                            # Zero value, close both ranges
                            if current_profit_start is not None:
                                profit_ranges.append(
                                    f"{pnl_col_letter}{current_profit_start}:{pnl_col_letter}{row_idx - 1}")
                                current_profit_start = None
                            if current_loss_start is not None:
                                loss_ranges.append(
                                    f"{pnl_col_letter}{current_loss_start}:{pnl_col_letter}{row_idx - 1}")
                                current_loss_start = None
                    except (ValueError, TypeError):
                        # Close both ranges on invalid value
                        if current_profit_start is not None:
                            profit_ranges.append(
                                f"{pnl_col_letter}{current_profit_start}:{pnl_col_letter}{row_idx - 1}")
                            current_profit_start = None
                        if current_loss_start is not None:
                            loss_ranges.append(
                                f"{pnl_col_letter}{current_loss_start}:{pnl_col_letter}{row_idx - 1}")
                            current_loss_start = None

                # Close any remaining ranges
                last_row = len(all_data)
                if current_profit_start is not None:
                    profit_ranges.append(
                        f"{pnl_col_letter}{current_profit_start}:{pnl_col_letter}{last_row}")
                if current_loss_start is not None:
                    loss_ranges.append(
                        f"{pnl_col_letter}{current_loss_start}:{pnl_col_letter}{last_row}")

            # Apply formatting to ranges (much fewer API calls)
            green_bg = {'backgroundColor': {
                'red': 0.7, 'green': 1.0, 'blue': 0.7}}
            red_bg = {'backgroundColor': {
                'red': 1.0, 'green': 0.7, 'blue': 0.7}}

            # Text colors for Side column
            green_text = {'textFormat': {'foregroundColor': {
                'red': 0.0, 'green': 0.6, 'blue': 0.0}}}  # Dark green text
            red_text = {'textFormat': {'foregroundColor': {
                'red': 0.8, 'green': 0.0, 'blue': 0.0}}}   # Dark red text

            # Format Buy ranges (green text) - with longer delays to avoid rate limits
            for range_str in buy_ranges:
                try:
                    worksheet.format(range_str, green_text)
                    time.sleep(1.0)  # Increased delay between range operations
                except Exception as e:
                    print(f"⚠️  Could not format Buy range {range_str}: {e}")

            # Format Sell ranges (red text)
            for range_str in sell_ranges:
                try:
                    worksheet.format(range_str, red_text)
                    time.sleep(1.0)  # Increased delay
                except Exception as e:
                    print(f"⚠️  Could not format Sell range {range_str}: {e}")

            # Format Profit ranges (green)
            for range_str in profit_ranges:
                try:
                    worksheet.format(range_str, green_bg)
                    time.sleep(1.0)  # Increased delay
                except Exception as e:
                    print(
                        f"⚠️  Could not format Profit range {range_str}: {e}")

            # Format Loss ranges (red)
            for range_str in loss_ranges:
                try:
                    worksheet.format(range_str, red_bg)
                    time.sleep(1.0)  # Increased delay
                except Exception as e:
                    print(f"⚠️  Could not format Loss range {range_str}: {e}")

            print(
                f"✅ Applied formatting to {len(buy_ranges + sell_ranges + profit_ranges + loss_ranges)} ranges instead of individual cells")

        except Exception as e:
            print(f"⚠️  Could not apply conditional formatting: {e}")

    def get_or_create_worksheet(self, sheet_name: str, headers: Optional[List[str]] = None) -> Optional[gspread.Worksheet]:
        """
        Gets an existing worksheet. If it does not exist, creates it and initializes it with headers.
        """
        if not self.spreadsheet:
            return None
        try:
            # Try to get the worksheet
            worksheet = self.spreadsheet.worksheet(sheet_name)
            return worksheet
        except gspread.WorksheetNotFound:
            # This block runs ONLY if the sheet does not exist
            try:
                print(
                    f"Worksheet '{sheet_name}' not found. Creating and initializing...")
                worksheet = self.spreadsheet.add_worksheet(
                    title=sheet_name, rows="1000", cols="30")
                if headers:
                    # Initialize with headers immediately upon creation
                    worksheet.append_row(
                        headers, value_input_option=ValueInputOption.user_entered)
                    self.format_headers(worksheet)
                print(
                    f"✅ Created and initialized new worksheet: '{sheet_name}'")
                return worksheet
            except Exception as e:
                print(f"❌ Error creating worksheet '{sheet_name}': {e}")
                return None

    def append_data(self, sheet_name: str, data: List[Dict], headers: List[str]) -> bool:
        """Appends rows of data. Handles sheet creation and initialization."""
        if not data:
            return True
        try:
            # This function now guarantees the sheet will exist and have headers
            worksheet = self.get_or_create_worksheet(
                sheet_name, headers=headers)
            if not worksheet:
                return False

            rows_to_add = []
            for row_dict in data:
                rows_to_add.append([row_dict.get(h, "") for h in headers])

            worksheet.append_rows(
                rows_to_add, value_input_option=ValueInputOption.user_entered)
            return True
        except Exception as e:
            print(f"❌ Error appending data to '{sheet_name}': {e}")
            return False

    def overwrite_data(self, sheet_name: str, data: List[Dict], headers: List[str]) -> bool:
        """Clears a sheet and replaces its content with headers and new data."""
        try:
            # This guarantees the sheet exists and is initialized if new
            worksheet = self.get_or_create_worksheet(
                sheet_name, headers=headers)
            if not worksheet:
                return False

            worksheet.clear()

            rows_to_write = [headers]
            for row_dict in data:
                rows_to_write.append([row_dict.get(h, "") for h in headers])

            worksheet.update(
                rows_to_write, value_input_option=ValueInputOption.user_entered)

            # Add delay to avoid rate limits
            time.sleep(1)

            self.format_headers(worksheet)

            # Apply conditional formatting for futures data with additional delay
            if sheet_name == "Futures History" and Config.ENABLE_FORMATTING:
                print(
                    "⏳ Applying conditional formatting (this may take a while due to rate limits)...")
                time.sleep(3)  # Extra delay before formatting
                try:
                    self.apply_conditional_formatting(worksheet, headers)
                except Exception as e:
                    print(
                        f"⚠️  Conditional formatting failed due to rate limits: {e}")
                    print(
                        "💡 Tip: You can disable formatting in config to speed up syncing")

            return True
        except Exception as e:
            print(f"❌ Error overwriting data in '{sheet_name}': {e}")
            return False

    def get_spreadsheet_url(self) -> Optional[str]:
        return self.spreadsheet.url if self.spreadsheet else None

    def test_connection(self) -> bool:
        print("🔧 Testing Google Sheets connection...")
        if not self.gc:
            print("❌ Google Sheets API not connected")
            return False
        if self.connect_to_spreadsheet():
            print("✅ Google Sheets connection successful")
            return True
        else:
            print("❌ Google Sheets connection failed")
            return False
