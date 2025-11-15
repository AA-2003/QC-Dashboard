"""
Google Sheets connection utilities for Streamlit applications.

This module provides functions to authenticate with Google Sheets API
and load data into pandas DataFrames.
"""

import logging
from typing import Optional

import gspread
import pandas as pd
import streamlit as st
from google.oauth2.service_account import Credentials

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Google Sheets API scopes
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive.file'
]

# Required credentials keys
REQUIRED_CREDENTIAL_KEYS = [
    "type", "project_id", "private_key_id", "private_key",
    "client_email", "client_id", "auth_uri", "token_uri"
]


def _validate_credentials(creds_dict: dict) -> tuple[bool, Optional[str]]:
    """
    Validate Google credentials dictionary.
    
    Args:
        creds_dict: Dictionary containing Google service account credentials
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    # Check for missing keys
    missing_keys = [key for key in REQUIRED_CREDENTIAL_KEYS if key not in creds_dict]
    if missing_keys:
        return False, f"Missing required keys: {', '.join(missing_keys)}"
    
    # Validate and fix private_key format
    if not isinstance(creds_dict["private_key"], str):
        return False, f"'private_key' must be a string, got {type(creds_dict['private_key']).__name__}"
    
    # Replace escaped newlines with actual newlines
    creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")
    
    return True, None


def authenticate_google_sheets() -> Optional[gspread.Client]:
    """
    Authenticate with Google Sheets API using Streamlit secrets.
    
    Returns:
        Authenticated gspread client or None if authentication fails
    """
    try:
        google_creds_object = st.secrets.get("GOOGLE_CREDENTIALS_JSON")
        
        if not google_creds_object:
            logger.error("'GOOGLE_CREDENTIALS_JSON' not found in Streamlit secrets")
            st.error("Google credentials not configured. Please contact administrator.")
            st.stop()
            return None
        
        # Convert to dictionary
        try:
            creds_dict = dict(google_creds_object)
        except (TypeError, ValueError) as e:
            logger.error(f"Failed to convert credentials to dict: {e}")
            st.error("Invalid credentials format. Please contact administrator.")
            st.stop()
            return None
        
        # Validate credentials
        is_valid, error_msg = _validate_credentials(creds_dict)
        if not is_valid:
            logger.error(f"Credential validation failed: {error_msg}")
            st.error("Invalid credentials configuration. Please contact administrator.")
            st.stop()
            return None
        
        # Create credentials and authorize client
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        client = gspread.authorize(creds)
        logger.info("Successfully authenticated with Google Sheets API")
        return client
        
    except Exception as e:
        logger.error(f"Authentication error: {e}", exc_info=True)
        st.error("Failed to authenticate with Google Sheets. Please contact administrator.")
        st.stop()
        return None


def _get_spreadsheet_id(key: str ) -> Optional[str]:
    """
    Get spreadsheet ID from Streamlit secrets.
    
    Args:
        use_eval_sheet: If True, return EVAL_SPREADSHEET_ID, else MAIN_SPREADSHEET_ID
        
    Returns:
        Spreadsheet ID or None if not found
    """
    try:
        spreadsheet_ids = st.secrets.get("SPREADSHEET_IDS")
        if not spreadsheet_ids:
            logger.error("'SPREADSHEET_ID' not found in Streamlit secrets")
            return None
        
        spreadsheet_id = spreadsheet_ids.get(key)
        
        if not spreadsheet_id:
            logger.error(f"'{key}' not found in SPREADSHEET_ID configuration")
            return None
        
        return spreadsheet_id
        
    except Exception as e:
        logger.error(f"Error retrieving spreadsheet ID: {e}")
        return None


def load_data_from_sheet(
    client: gspread.Client,
    spreadsheet_id: str,
    sheet_name: str
) -> Optional[pd.DataFrame]:
    """
    Load data from a Google Sheet into a pandas DataFrame.
    
    Args:
        client: Authenticated gspread client
        spreadsheet_id: Google Spreadsheet ID
        sheet_name: Name of the worksheet to load
        
    Returns:
        DataFrame containing sheet data, or None if loading fails
    """
    if not client:
        logger.error("No authenticated client provided")
        return None
    
    try:
        spreadsheet = client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet(sheet_name)
        data = worksheet.get_all_records()
        
        if not data:
            logger.warning(f"Sheet '{sheet_name}' is empty or contains only headers")
            return pd.DataFrame()
        
        df = pd.DataFrame(data)
        logger.info(f"Loaded {len(df)} rows and {len(df.columns)} columns from '{sheet_name}'")
        return df
        
    except gspread.exceptions.SpreadsheetNotFound:
        logger.error(f"Spreadsheet with ID '{spreadsheet_id}' not found")
        st.error("Spreadsheet not found. Please check configuration.")
    except gspread.exceptions.WorksheetNotFound:
        logger.error(f"Worksheet '{sheet_name}' not found")
        st.error(f"Sheet '{sheet_name}' not found in spreadsheet.")
    except gspread.exceptions.APIError as e:
        logger.error(f"Google Sheets API error: {e}")
        st.error("API error occurred. Please try again later.")
    except Exception as e:
        logger.error(f"Unexpected error loading sheet data: {e}", exc_info=True)
        st.error("Failed to load data from sheet.")
    
    return None


@st.cache_data(ttl=600, show_spinner=False)
def load_sheet(
    key: str,
    sheet_name: str = 'Data',
) -> Optional[pd.DataFrame]:
    """
    Load data from a Google Sheet with caching.
    
    Args:
        sheet_name: Name of the worksheet to load (default: 'Data')
        use_eval_spreadsheet: If True, use EVAL_SPREADSHEET_ID, else MAIN_SPREADSHEET_ID
        
    Returns:
        DataFrame containing sheet data, or None if loading fails
    """
    # Authenticate
    client = authenticate_google_sheets()
    if not client:
        return None
    
    # Get spreadsheet ID
    spreadsheet_id = _get_spreadsheet_id(key)
    if not spreadsheet_id:
        st.error("Spreadsheet ID not configured.")
        return None
    
    # Load data with spinner
    logger.info(f"Loading sheet '{sheet_name}' from spreadsheet '{spreadsheet_id}'")
    with st.spinner("بارگذاری داده ها ..."):
        df = load_data_from_sheet(client, spreadsheet_id, sheet_name)
    
    if df is not None and df.empty:
        st.warning("Sheet loaded successfully but contains no data.")
    
    return df


@st.cache_data(ttl=600, show_spinner=False)
def load_sheet_uncached(
    sheet_name: str = 'Data',
    use_eval_spreadsheet: bool = False
) -> Optional[pd.DataFrame]:
    """
    Load sheet without using cache (wrapper that clears cache before loading).
    
    Args:
        sheet_name: Name of the worksheet to load
        use_eval_spreadsheet: If True, use EVAL_SPREADSHEET_ID
        
    Returns:
        DataFrame containing sheet data
    """
    load_sheet.clear()
    return load_sheet(sheet_name, use_eval_spreadsheet)


def append_to_sheet(
    client: gspread.Client,
    spreadsheet_id: str,
    sheet_name: str,
    row_data: list
) -> bool:
    """
    Append a row of data to a Google Sheet.
    
    Args:
        client: Authenticated gspread client
        spreadsheet_id: Google Spreadsheet ID
        sheet_name: Name of the worksheet to append to
        row_data: List of values representing the row to append
        
    Returns:
        True if append is successful, False otherwise
    """
    if not client:
        logger.error("No authenticated client provided")
        return False
    
    try:
        # Flatten row_data if it's a nested list
        if row_data and isinstance(row_data, list):
            # If row_data is a list of lists, flatten it
            if isinstance(row_data[0], (list, tuple)):
                row_data = row_data[0]
            # If row_data contains dicts, convert to list of values
            elif isinstance(row_data[0], dict):
                row_data = list(row_data[0].values())
        
        spreadsheet = client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet(sheet_name)
        worksheet.append_row(row_data, value_input_option='USER_ENTERED')
        logger.info(f"Appended row to '{sheet_name}'")
        return True
        
    except gspread.exceptions.SpreadsheetNotFound:
        logger.error(f"Spreadsheet with ID '{spreadsheet_id}' not found")
    except gspread.exceptions.WorksheetNotFound:
        logger.error(f"Worksheet '{sheet_name}' not found")
    except gspread.exceptions.APIError as e:
        logger.error(f"Google Sheets API error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error appending to sheet: {e}", exc_info=True)
    
    return False

if __name__ == "__main__":
    logger.info("Google Sheets connector module loaded successfully.")



