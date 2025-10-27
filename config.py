import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    """Configuration management for the application"""
    
    # Bybit API Configuration
    BYBIT_API_KEY = os.getenv("BYBIT_API_KEY")
    BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET")
    BYBIT_USE_TESTNET = os.getenv("BYBIT_USE_TESTNET", "false").lower() == "true"
    
    # Google Sheets Configuration
    GOOGLE_CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE", "google_credentials.json")
    GOOGLE_SPREADSHEET_ID = os.getenv("GOOGLE_SPREADSHEET_ID")
    GOOGLE_SPREADSHEET_NAME = os.getenv("GOOGLE_SPREADSHEET_NAME", "Bybit Trading Log")
    
    # Data Collection Settings
    SPOT_HISTORY_START_DATE = os.getenv("SPOT_HISTORY_START_DATE", "2025-09-28")  # Format: YYYY-MM-DD
    FUTURES_HISTORY_START_DATE = os.getenv("SPOT_HISTORY_START_DATE", "2025-09-28")  # Format: YYYY-MM-DD
    PORTFOLIO_START_DATE = os.getenv("PORTFOLIO_START_DATE", "2025-09-28")  # Format: YYYY-MM-DD - For portfolio overview calculations
    TRANSFER_START_DATE = os.getenv("SPOT_HISTORY_START_DATE", "2025-09-28")  # Format: YYYY-MM-DD
    
    # User Settings
    USER_EMAIL = os.getenv("USER_EMAIL")
    
    # Application Settings
    AUTO_CLEAR_SHEETS = os.getenv("AUTO_CLEAR_SHEETS", "true").lower() == "true"
    ENABLE_FORMATTING = os.getenv("ENABLE_FORMATTING", "true").lower() == "true"
    
    @classmethod
    def validate(cls):
        """Validate required configuration"""
        errors = []
        
        if not cls.BYBIT_API_KEY:
            errors.append("BYBIT_API_KEY is required")
        
        if not cls.BYBIT_API_SECRET:
            errors.append("BYBIT_API_SECRET is required")
        
        if not os.path.exists(cls.GOOGLE_CREDENTIALS_FILE):
            errors.append(f"Google credentials file not found: {cls.GOOGLE_CREDENTIALS_FILE}")
        
        if not cls.GOOGLE_SPREADSHEET_ID and not cls.GOOGLE_SPREADSHEET_NAME:
            errors.append("Either GOOGLE_SPREADSHEET_ID or GOOGLE_SPREADSHEET_NAME is required")
        
        if errors:
            raise ValueError("Configuration errors:\n" + "\n".join(f"- {error}" for error in errors))
        
        return True
    
    @classmethod
    def get_bybit_base_url(cls):
        """Get Bybit API base URL based on environment"""
        if cls.BYBIT_USE_TESTNET:
            return "https://api-testnet.bybit.com"
        else:
            return "https://api.bybit.com"
    
    @classmethod
    def print_config(cls):
        """Print configuration (for debugging)"""
        print("🔧 Configuration:")
        print(f"   Bybit Environment: {'TESTNET' if cls.BYBIT_USE_TESTNET else 'MAINNET'}")
        print(f"   Google Credentials: {cls.GOOGLE_CREDENTIALS_FILE}")
        print(f"   Spreadsheet ID: {cls.GOOGLE_SPREADSHEET_ID or 'Using name instead'}")
        print(f"   Spreadsheet Name: {cls.GOOGLE_SPREADSHEET_NAME}")
        print(f"   Auto Clear: {cls.AUTO_CLEAR_SHEETS}")
        print(f"   Formatting: {cls.ENABLE_FORMATTING}")