import os
import json
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
            self.format_headers(worksheet)
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
