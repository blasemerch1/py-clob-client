#!/usr/bin/env python3
import os
import json
import logging
import sys
import time
from typing import Dict, List, Any, Optional
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

def fetch_price_history(client: ClobClient, market_hash: str, start_time: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Fetches price history for a specific market from Polymarket's CLOB API.
    
    Args:
        client (ClobClient): The initialized Polymarket CLOB API client.
        market_hash (str): The FPMM contract address (hash) of the market.
        start_time (Optional[int]): The timestamp to start fetching prices from (optional).
        
    Returns:
        List[Dict[str, Any]]: A list of price history data points.
        
    Raises:
        Exception: If the API request fails.
    """
    max_retries = 3
    retry_delay = 2  # seconds
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Fetching price history for market {market_hash}...")
            
            # Prepare query parameters
            params = {"hash": market_hash}
            if start_time:
                params["startTime"] = start_time
            
            # Make API call to get price history
            response = client.session.get(f"{client.api_url}/pricesForMarket", params=params)
            response.raise_for_status()
            
            price_data = response.json()
            if not price_data:
                logger.warning(f"No price history found for market {market_hash}")
                return []
                
            logger.info(f"Successfully fetched {len(price_data)} price history entries for market {market_hash}")
            
            # Format the price history data
            formatted_price_history = []
            for entry in price_data:
                formatted_entry = {
                    "timestamp": entry.get("timestamp"),
                    "price": entry.get("price"),
                    "liquidity": entry.get("liquidity")
                }
                formatted_price_history.append(formatted_entry)
                
            return formatted_price_history
            
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Attempt {attempt + 1} failed to fetch price history for market {market_hash}: {str(e)}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error(f"Failed to fetch price history for market {market_hash} after {max_retries} attempts: {str(e)}")
                return []  # Return empty list on failure to avoid breaking the script

def fetch_trade_history(client: ClobClient, market_hash: str) -> List[Dict[str, Any]]:
    """
    Fetches trade history for a specific market from Polymarket's CLOB API.
    
    Args:
        client (ClobClient): The initialized Polymarket CLOB API client.
        market_hash (str): The FPMM contract address (hash) of the market.
        
    Returns:
        List[Dict[str, Any]]: A list of trade history data.
        
    Raises:
        Exception: If the API request fails.
    """
    max_retries = 3
    retry_delay = 2  # seconds
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Fetching trade history for market {market_hash}...")
            
            # Make API call to get trade history
            response = client.session.get(f"{client.api_url}/allTradesForMarket", params={"hash": market_hash})
            response.raise_for_status()
            
            trade_data = response.json()
            if not trade_data:
                logger.warning(f"No trade history found for market {market_hash}")
                return []
                
            logger.info(f"Successfully fetched {len(trade_data)} trade history entries for market {market_hash}")
            
            # Format the trade history data
            formatted_trade_history = []
            for entry in trade_data:
                formatted_entry = {
                    "timestamp": entry.get("timestamp"),
                    "trader": entry.get("trader"),
                    "amount": entry.get("amount"),
                    "price": entry.get("price")
                }
                formatted_trade_history.append(formatted_entry)
                
            return formatted_trade_history
            
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Attempt {attempt + 1} failed to fetch trade history for market {market_hash}: {str(e)}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error(f"Failed to fetch trade history for market {market_hash} after {max_retries} attempts: {str(e)}")
                return []  # Return empty list on failure to avoid breaking the script

def fetch_market_data(client: ClobClient) -> List[Dict[str, Any]]:
    """
    Fetches market data including price and trade history from Polymarket's CLOB API.
    
    Args:
        client (ClobClient): The initialized Polymarket CLOB API client.
        
    Returns:
        List[Dict[str, Any]]: A list of market data dictionaries with price and trade history.
        
    Raises:
        Exception: If the API request fails.
    """
    try:
        logger.info("Fetching markets from Polymarket CLOB API...")
        markets = client.get_markets()
        logger.info(f"Successfully fetched {len(markets)} markets.")
        
        # Enhance each market with price and trade history
        enhanced_markets = []
        for market in markets:
            market_hash = market.get("fpmm_contract_address")
            
            if not market_hash:
                logger.warning(f"Market missing fpmm_contract_address, skipping history fetch: {market}")
                enhanced_markets.append(market)
                continue
                
            # Fetch price history for this market
            price_history = fetch_price_history(client, market_hash)
            
            # Fetch trade history for this market
            trade_history = fetch_trade_history(client, market_hash)
            
            # Add price and trade history to the market data
            enhanced_market = market.copy()
            enhanced_market["price_history"] = price_history
            enhanced_market["trade_history"] = trade_history
            
            enhanced_markets.append(enhanced_market)
            
        return enhanced_markets
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
        # Structure the data for better organization
        structured_data = {
            "markets": data,
            "meta": {
                "timestamp": int(time.time()),
                "count": len(data)
            }
        }
        
        with open(filename, 'w') as f:
            json.dump(structured_data, f, indent=4)
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
        
        # Fetch market data with price and trade history
        market_data = fetch_market_data(client)
        
        # Print the number of markets fetched
        print(f"Fetched {len(market_data)} markets from Polymarket.")
        
        # Save the data to a JSON file
        save_to_json(market_data)
        
        print("Market data with price and trade history successfully saved to market_data.json")
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
