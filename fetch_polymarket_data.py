#!/usr/bin/env python3
import os
import json
import logging
import sys
from typing import Dict, List, Any
from dotenv import load_dotenv
from py_clob_client.client import ClobClient

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def validate_pk(pk: str) -> bool:
    """
    Validates that the provided Ethereum private key is correctly formatted.
    
    Args:
        pk (str): The Ethereum private key to validate.
        
    Returns:
        bool: True if the private key is valid, False otherwise.
    """
    # Private key should be a 64-character hexadecimal string (without '0x' prefix)
    if not pk or not isinstance(pk, str):
        return False
    
    if len(pk) != 64:
        return False
    
    try:
        # Check if the key is a valid hexadecimal
        int(pk, 16)
        return True
    except ValueError:
        return False

def initialize_client() -> ClobClient:
    """
    Initializes and returns a Polymarket CLOB API client.
    
    Returns:
        ClobClient: An initialized Polymarket CLOB API client.
        
    Raises:
        ValueError: If required environment variables are missing or invalid.
    """
    # Load environment variables
    load_dotenv()
    
    # Get required environment variables
    api_url = os.getenv("CLOB_API_URL")
    private_key = os.getenv("PK")
    
    # Validate environment variables
    if not api_url:
        raise ValueError("CLOB_API_URL environment variable is missing.")
    
    if not private_key:
        raise ValueError("PK environment variable is missing.")
    
    # Validate private key format
    if not validate_pk(private_key):
        raise ValueError("PK must be a 64-character Ethereum private key (without '0x' prefix).")
    
    # Initialize the client
    logger.info(f"Initializing Polymarket CLOB client with API URL: {api_url}")
    return ClobClient(api_url, key=private_key, chain_id=137)  # 137 is Polygon Mainnet

def fetch_market_data(client: ClobClient) -> List[Dict[str, Any]]:
    """
    Fetches market data from Polymarket's CLOB API.
    
    Args:
        client (ClobClient): The initialized Polymarket CLOB API client.
        
    Returns:
        List[Dict[str, Any]]: A list of market data dictionaries.
        
    Raises:
        Exception: If the API request fails.
    """
    try:
        logger.info("Fetching markets from Polymarket CLOB API...")
        markets = client.get_markets()
        logger.info(f"Successfully fetched {len(markets)} markets.")
        return markets
    except Exception as e:
        logger.error(f"Failed to fetch market data: {str(e)}")
        raise Exception(f"Failed to fetch market data: {str(e)}")

def save_to_json(data: List[Dict[str, Any]], filename: str = "market_data.json") -> None:
    """
    Saves the market data to a JSON file.
    
    Args:
        data (List[Dict[str, Any]]): The market data to save.
        filename (str, optional): The name of the output file. Defaults to "market_data.json".
        
    Raises:
        Exception: If writing to the file fails.
    """
    try:
        with open(filename, 'w') as f:
            json.dump(data, f, indent=4)
        logger.info(f"Successfully saved market data to {filename}")
    except Exception as e:
        logger.error(f"Failed to save data to {filename}: {str(e)}")
        raise Exception(f"Failed to save data to {filename}: {str(e)}")

def main() -> None:
    """
    Main function to orchestrate the data fetching and saving process.
    """
    try:
        # Initialize the client
        client = initialize_client()
        
        # Fetch market data
        market_data = fetch_market_data(client)
        
        # Print the number of markets fetched
        print(f"Fetched {len(market_data)} markets from Polymarket.")
        
        # Save the data to a JSON file
        save_to_json(market_data)
        
        print("Market data successfully saved to market_data.json")
    except ValueError as e:
        logger.error(f"Validation error: {str(e)}")
        print(f"Error: {str(e)}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
