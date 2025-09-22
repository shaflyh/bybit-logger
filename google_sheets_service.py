import os
import json
import gspread
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
        """Setup Google Sheets API connection"""
        try:
            if not os.path.exists(Config.GOOGLE_CREDENTIALS_FILE):
                print(
                    f"❌ Google credentials file not found: {Config.GOOGLE_CREDENTIALS_FILE}")
                return False

            # Define the scope for Google Sheets API
            scope = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ]

            # Load credentials from service account file
            creds = Credentials.from_service_account_file(
                Config.GOOGLE_CREDENTIALS_FILE, scopes=scope)
            self.gc = gspread.authorize(creds)

            print("✅ Google Sheets API connected")
            return True

        except Exception as e:
            print(f"❌ Google Sheets setup error: {e}")
            return False

    def get_service_account_email(self) -> Optional[str]:
        """Get service account email from credentials file"""
        try:
            with open(Config.GOOGLE_CREDENTIALS_FILE, 'r') as f:
                creds_data = json.load(f)
                return creds_data.get('client_email')
        except Exception:
            return None

    def connect_to_spreadsheet(self) -> bool:
        """Connect to the configured spreadsheet"""
        if not self.gc:
            print("❌ Google Sheets not connected")
            return False

        try:
            # Try to open by ID first (more reliable)
            if Config.GOOGLE_SPREADSHEET_ID:
                self.spreadsheet = self.gc.open_by_key(
                    Config.GOOGLE_SPREADSHEET_ID)
                print(
                    f"✅ Connected to spreadsheet by ID: {Config.GOOGLE_SPREADSHEET_ID}")
                return True

            # Fallback to opening by name
            elif Config.GOOGLE_SPREADSHEET_NAME:
                self.spreadsheet = self.gc.open(Config.GOOGLE_SPREADSHEET_NAME)
                print(
                    f"✅ Connected to spreadsheet by name: {Config.GOOGLE_SPREADSHEET_NAME}")
                return True

            else:
                print("❌ No spreadsheet ID or name configured")
                return False

        except gspread.SpreadsheetNotFound:
            print(f"❌ Spreadsheet not found!")
            print("\n📋 To fix this:")
            print("1. Go to https://sheets.google.com")
            print(
                f"2. Create a new spreadsheet named: '{Config.GOOGLE_SPREADSHEET_NAME}'")
            print("3. Click 'Share' and add this email with Editor permission:")

            service_email = self.get_service_account_email()
            if service_email:
                print(f"   📧 {service_email}")

            print("4. Update your .env file with the spreadsheet ID or name")
            return False

        except Exception as e:
            print(f"❌ Error connecting to spreadsheet: {e}")
            return False

    def get_or_create_worksheet(self, sheet_name: str) -> Optional[gspread.Worksheet]:
        """Get existing worksheet or create new one"""
        if not self.spreadsheet:
            print("❌ No spreadsheet connected")
            return None

        try:
            # Try to get existing worksheet
            worksheet = self.spreadsheet.worksheet(sheet_name)
            return worksheet

        except gspread.WorksheetNotFound:
            # Create new worksheet
            try:
                worksheet = self.spreadsheet.add_worksheet(
                    title=sheet_name,
                    rows="1000",
                    cols="20"
                )
                print(f"✅ Created new worksheet: {sheet_name}")
                return worksheet

            except Exception as e:
                print(f"❌ Error creating worksheet {sheet_name}: {e}")
                return None

    def update_worksheet_data(self, sheet_name: str, data: List[Dict], clear_first: bool = None) -> bool:
        """Update worksheet with data"""
        if not data:
            print(f"⚠️  No data to update for {sheet_name}")
            return False

        clear_first = clear_first if clear_first is not None else Config.AUTO_CLEAR_SHEETS

        try:
            worksheet = self.get_or_create_worksheet(sheet_name)
            if not worksheet:
                return False

            # Clear existing data if requested
            if clear_first:
                worksheet.clear()

            # Convert data to list of lists for Google Sheets
            headers = list(data[0].keys())
            values = [headers]  # Header row

            # Data rows
            for row in data:
                values.append([str(row.get(header, '')) for header in headers])

            # Update the sheet
            worksheet.update(values)

            # Apply formatting if enabled
            if Config.ENABLE_FORMATTING:
                self._apply_formatting(worksheet, len(data))

            print(f"✅ Updated {sheet_name} with {len(data)} rows")
            return True

        except Exception as e:
            print(f"❌ Error updating {sheet_name}: {e}")
            return False

    def _apply_formatting(self, worksheet: gspread.Worksheet, data_rows: int):
        """Apply basic formatting to worksheet"""
        try:
            # Format headers (make them bold with background)
            worksheet.format('1:1', {
                'textFormat': {'bold': True},
                'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9},
                'horizontalAlignment': 'CENTER'
            })

            # Set number format for cells to prevent apostrophe issues
            # Format entire sheet as text to prevent automatic conversions
            worksheet.format(f'A1:Z{data_rows + 1}', {
                'numberFormat': {'type': 'TEXT'}
            })

            # Auto-resize columns to fit content
            if hasattr(worksheet, 'columns_auto_resize'):
                worksheet.columns_auto_resize(0, len(worksheet.row_values(1)))

            # Set minimum column width
            header_row = worksheet.row_values(1)
            for i, header in enumerate(header_row):
                # Calculate minimum width based on header length
                # 20 pixels per character, minimum 100
                min_width = max(len(header) * 20, 100)
                try:
                    worksheet.update_dimension_properties(
                        dimension='COLUMNS',
                        start_index=i,
                        end_index=i + 1,
                        fields='pixelSize',
                        properties={'pixelSize': min_width}
                    )
                except Exception:
                    # Skip if dimension API not available
                    pass

        except Exception as e:
            print(f"⚠️  Formatting warning: {e}")

    def get_spreadsheet_url(self) -> Optional[str]:
        """Get the URL of the current spreadsheet"""
        if self.spreadsheet:
            return self.spreadsheet.url
        return None

    def create_summary_data(self, stats: Dict) -> List[Dict]:
        """Create summary data for the summary sheet"""
        from datetime import datetime

        summary_data = [{
            "Last Updated": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "Futures Positions": stats.get('futures_count', 0),
            "Spot Trades": stats.get('spot_count', 0),
            "Wallet Transactions": stats.get('wallet_count', 0),
            "Data Period (Days)": f"Futures: {Config.FUTURES_DAYS_BACK}, Spot: {Config.SPOT_DAYS_BACK}, Wallet: {Config.WALLET_DAYS_BACK}",
            "Environment": "TESTNET" if Config.BYBIT_USE_TESTNET else "MAINNET",
            "Spreadsheet URL": self.get_spreadsheet_url() or "Unknown"
        }]

        return summary_data

    def test_connection(self) -> bool:
        """Test Google Sheets connection"""
        print("🔧 Testing Google Sheets connection...")

        if not self.gc:
            print("❌ Google Sheets API not connected")
            return False

        success = self.connect_to_spreadsheet()
        if success:
            print("✅ Google Sheets connection successful")
            return True
        else:
            print("❌ Google Sheets connection failed")
            return False
