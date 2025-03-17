#!/usr/bin/env python3
import os
import json
import logging
import sys
from typing import Dict, List, Any, Optional
from pathlib import Path
from datetime import datetime

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def fetch_market_metadata() -> Dict[str, Any]:
    """
    Fetches market metadata from the Polymarket CLOB API.
    
    Returns:
        Dict[str, Any]: Dictionary containing market metadata.
        
    Raises:
        Exception: If API credentials are missing or API requests fail.
    """
    # Load API credentials from environment variables
    pk = os.getenv("PK")
    api_key = os.getenv("CLOB_API_KEY")
    api_secret = os.getenv("CLOB_SECRET")
    api_passphrase = os.getenv("CLOB_PASS_PHRASE")
    api_url = os.getenv("CLOB_API_URL")
    
    # Validate that all required credentials are present
    if not all([pk, api_key, api_secret, api_passphrase, api_url]):
        error_msg = "Missing API credentials. Please set PK, CLOB_API_KEY, CLOB_SECRET, CLOB_PASS_PHRASE, and CLOB_API_URL environment variables."
        logger.error(error_msg)
        raise Exception(error_msg)
    
    logger.info("Initializing CLOB client with credentials")
    
    try:
        # Initialize the CLOB client with credentials
        creds = ApiCreds(
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=api_passphrase
        )
        
        # Create chain_id 137 for Polygon network
        client = ClobClient(host=api_url, chain_id=137, key=pk, creds=creds)
        
        logger.info("Fetching markets from Polymarket CLOB API")
        
        # Fetch all markets
        markets_data = []
        next_cursor = "MA=="  # Initial cursor
        
        while next_cursor and next_cursor != "":
            try:
                response = client.get_markets(next_cursor=next_cursor)
                
                if "data" not in response:
                    logger.warning("Unexpected response format. Missing 'data' field.")
                    break
                
                market_batch = response["data"]
                markets_data.extend(market_batch)
                
                # Update cursor for pagination
                next_cursor = response.get("next_cursor", "")
                
                logger.info(f"Fetched batch of {len(market_batch)} markets. Total: {len(markets_data)}")
                
                # Handle rate limiting
                if len(market_batch) == 0 or next_cursor == "":
                    logger.info("Reached end of markets data")
                    break
                    
            except Exception as e:
                logger.error(f"Error fetching markets batch: {str(e)}")
                # Continue with next batch if possible
                if next_cursor == "MA==":
                    # If this is the first batch, we can't proceed
                    raise
                break
        
        # Process market data to extract metadata
        processed_markets = []
        
        for market in markets_data:
            try:
                # Get market details
                condition_id = market.get("condition_id")
                
                if not condition_id:
                    logger.warning(f"Market missing condition_id, skipping: {market}")
                    continue
                
                # Fetch detailed market data
                market_detail = client.get_market(condition_id)
                
                # Fetch tick size for this market
                tick_size = None
                if "token_ids" in market and len(market["token_ids"]) > 0:
                    try:
                        token_id = market["token_ids"][0]
                        tick_size = client.get_tick_size(token_id)
                    except Exception as e:
                        logger.warning(f"Failed to get tick size for token {token_id}: {str(e)}")
                
                # Determine if market is 50/50 outcome
                is_fifty_fifty = False
                if "token_ids" in market and len(market["token_ids"]) == 2:
                    is_fifty_fifty = True
                
                # Extract required metadata
                market_data = {
                    "market_id": condition_id,
                    "question": market.get("question", "Unknown"),
                    "description": market_detail.get("description", ""),
                    "status": market.get("status", "Unknown"),
                    "outcome_tokens": market.get("outcomes", []),
                    "token_ids": market.get("token_ids", []),
                    "minimum_order_size": market_detail.get("min_order_size", "0"),
                    "tick_size": tick_size,
                    "is_fifty_fifty": is_fifty_fifty,
                    "created_at": market.get("created_at", ""),
                    "updated_at": market.get("updated_at", ""),
                    "resolution_time": market.get("resolution_time", ""),
                    "volume": market.get("volume", "0")
                }
                
                processed_markets.append(market_data)
                
            except Exception as e:
                logger.warning(f"Error processing market {market.get('condition_id', 'unknown')}: {str(e)}")
                continue
        
        # Prepare final metadata object
        metadata = {
            "markets": processed_markets,
            "retrieved_at": datetime.utcnow().isoformat(),
            "total_markets": len(processed_markets)
        }
        
        # Save to file
        file_path = os.path.expanduser("~/workspace/market_metadata.json")
        save_to_json(metadata, file_path)
        
        logger.info(f"Successfully retrieved metadata for {len(processed_markets)} markets")
        return metadata
        
    except Exception as e:
        logger.error(f"Failed to fetch market metadata: {str(e)}")
        raise
    
def save_to_json(data: Dict[str, Any], filename: str) -> None:
    """
    Saves the market metadata to a JSON file.
    
    Args:
        data (Dict[str, Any]): The market metadata.
        filename (str): The path where to save the file.
        
    Raises:
        Exception: If writing to the file fails.
    """
    try:
        # Ensure directory exists
        directory = os.path.dirname(filename)
        if directory and not os.path.exists(directory):
            os.makedirs(directory)
            
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        logger.info(f"Successfully saved data to {filename}")
    except Exception as e:
        logger.error(f"Failed to save data to {filename}: {str(e)}")
        raise Exception(f"Failed to save data to {filename}: {str(e)}")

def main():
    """
    Main function to execute the script.
    """
    try:
        fetch_market_metadata()
        return 0
    except Exception as e:
        logger.error(f"Script execution failed: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
