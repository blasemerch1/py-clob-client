#!/usr/bin/env python3
import os
import json
import logging
import requests
from typing import Dict, List, Any

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Define file paths
MARKET_METADATA_FILE = "market_metadata.json"
MARKET_LIST_FILE = "market_list.json"
SKIPPED_MARKETS_FILE = "skipped_markets.json"

def _write_json_file(data: Any, filename: str) -> None:
    """
    Write data to a JSON file.
    
    Args:
        data: The data to write.
        filename: The name of the file to write to.
    """
    try:
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        logger.info(f"Successfully saved data to {filename}")
    except Exception as e:
        logger.error(f"Failed to save data to {filename}: {str(e)}")
        raise

def fetch_market_metadata(limit: int = 500) -> Dict[str, Any]:
    """
    Fetch market metadata from the Polymarket CLOB API.
    
    Args:
        limit: Maximum number of markets to fetch.
        
    Returns:
        Dict containing market metadata, market list, and skipped markets.
    """
    # Load API credentials from environment variables
    api_key = os.getenv("CLOB_API_KEY")
    api_secret = os.getenv("CLOB_SECRET")
    api_passphrase = os.getenv("CLOB_PASS_PHRASE")
    api_url = os.getenv("CLOB_API_URL")
    
    # Check if API credentials are set
    if not all([api_key, api_secret, api_passphrase, api_url]):
        error_msg = "API credentials not fully set. Please set CLOB_API_KEY, CLOB_SECRET, CLOB_PASS_PHRASE, and CLOB_API_URL environment variables."
        logger.error(error_msg)
        raise Exception(error_msg)
    
    try:
        # Set up headers with authentication
        headers = {
            "Content-Type": "application/json",
            "X-API-KEY": api_key,
            "X-API-SECRET": api_secret,
            "X-API-PASSPHRASE": api_passphrase
        }
        
        logger.info(f"Fetching up to {limit} markets from Polymarket CLOB API")
        
        # Initialize empty dictionaries and lists for results
        market_metadata = {}
        market_list = []
        skipped_markets = {}
        
        # Fetch markets from API
        response = requests.get(
            f"{api_url}/markets",
            headers=headers,
            params={"limit": limit}
        )
        
        # Check if request was successful
        response.raise_for_status()
        
        # Parse the response
        markets_data = response.json()
        
        # Handle case where markets_data might be a string
        if isinstance(markets_data, str):
            try:
                markets_data = json.loads(markets_data)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse markets_data string: {str(e)}")
                raise Exception(f"Failed to parse markets_data string: {str(e)}")
        
        # Handle case where markets_data might not be a list
        if not isinstance(markets_data, list):
            if isinstance(markets_data, dict) and "markets" in markets_data:
                markets_data = markets_data["markets"]
            else:
                logger.error(f"Unexpected format for markets_data: {type(markets_data)}")
                raise Exception(f"Unexpected format for markets_data: {type(markets_data)}")
        
        # Process each market
        for market in markets_data:
            if not isinstance(market, dict):
                logger.warning(f"Skipping non-dictionary market: {market}")
                continue
                
            market_slug = market.get("market_slug")
            condition_id = market.get("condition_id")
            
            if not market_slug:
                logger.warning(f"Skipping market without slug: {market}")
                continue
                
            if not condition_id:
                logger.warning(f"Skipping market without condition_id: {market_slug}")
                skipped_markets[market_slug] = {"reason": "Missing condition_id"}
                continue
            
            # Extract metadata
            metadata = {
                "metadata": {
                    "question": market.get("question", ""),
                    "description": market.get("description", ""),
                    "status": market.get("status", ""),
                    "tags": market.get("tags", [])
                }
            }
            
            # Add to results
            market_metadata[market_slug] = metadata
            market_list.append(market_slug)
        
        # Ensure market_list has no null values
        market_list = [slug for slug in market_list if slug is not None]
        
        # Write results to files
        _write_json_file(market_metadata, MARKET_METADATA_FILE)
        _write_json_file(market_list, MARKET_LIST_FILE)
        _write_json_file(skipped_markets, SKIPPED_MARKETS_FILE)
        
        logger.info(f"Successfully fetched and processed {len(market_list)} markets")
        logger.info(f"Skipped {len(skipped_markets)} markets due to missing condition_id")
        
        return {
            "market_metadata": market_metadata,
            "market_list": market_list,
            "skipped_markets": skipped_markets
        }
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch data from API: {str(e)}")
        raise Exception(f"Failed to fetch data from API: {str(e)}")
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse API response: {str(e)}")
        raise Exception(f"Failed to parse API response: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise

def test_fetch_market_metadata() -> None:
    """
    Test the market metadata fetching functionality by fetching one market.
    """
    print("Running test for fetch_market_metadata.py script...")
    
    try:
        # Set limit to 1 to just fetch one test market
        result = fetch_market_metadata(limit=1)
        
        # Check if data is not empty
        if not result["market_metadata"]:
            print("❌ Test failed: Fetched data is empty.")
            return
            
        # Get the first market's metadata
        if not result["market_list"]:
            print("❌ Test failed: No markets found in the result.")
            return
            
        first_market_slug = result["market_list"][0]
        sample_market = result["market_metadata"][first_market_slug]
        
        # Print the test market metadata
        print(f"✅ Test passed! Successfully fetched test market.")
        print(f"Market slug: {first_market_slug}")
        print(f"Market question: '{sample_market['metadata']['question']}'")
        print(f"Market description: '{sample_market['metadata']['description']}'")
        print(f"Market status: {sample_market['metadata']['status']}")
        print(f"Market tags: {', '.join(sample_market['metadata']['tags'])}")
        
    except Exception as e:
        print(f"❌ Test failed with error: {str(e)}")

if __name__ == "__main__":
    # Load private key for API authentication if needed
    pk = os.getenv("PK")
    if not pk:
        logger.warning("PK environment variable not set. Some API calls may fail.")
    
    # Run test if no arguments are provided
    test_fetch_market_metadata()
