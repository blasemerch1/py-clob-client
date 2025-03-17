#!/usr/bin/env python3
import os
import json
import logging
import sys
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
from datetime import datetime
import random

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def fetch_market_metadata(limit=500) -> Tuple[Dict[str, Any], List[str], List[Dict]]:
    """
    Fetches market metadata from the Polymarket CLOB API, limited to exactly 500 markets.
    
    Args:
        limit (int): Maximum number of markets to fetch. Defaults to 500.
    
    Returns:
        Tuple containing:
        - Dict[str, Any]: Dictionary of market metadata indexed by market slugs
        - List[str]: List of valid market slugs
        - List[Dict]: List of skipped markets with missing condition_id
        
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
    
    logger.info(f"Initializing CLOB client with credentials to fetch {limit} markets")
    
    try:
        # Initialize the CLOB client with credentials
        creds = ApiCreds(
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=api_passphrase
        )
        
        # Create chain_id 137 for Polygon network
        client = ClobClient(host=api_url, chain_id=137, key=pk, creds=creds)
        
        logger.info(f"Fetching exactly {limit} markets from Polymarket CLOB API")
        
        # Fetch a single batch of markets (no pagination/looping)
        response = client.get_markets(next_cursor="MA==")
        
        if "data" not in response:
            logger.error("Unexpected response format. Missing 'data' field.")
            raise Exception("API response missing 'data' field")
        
        # Take only the first 'limit' markets
        markets_data = response["data"][:limit]
        
        if len(markets_data) < limit:
            logger.warning(f"Requested {limit} markets but only received {len(markets_data)}. Processing what was obtained.")
        else:
            logger.info(f"Successfully fetched {len(markets_data)} markets")
        
        # Create market metadata organized by market slug
        metadata = {}
        market_slugs = []
        skipped_markets = []
        
        for market in markets_data:
            try:
                # Get market details - check for missing condition_id
                condition_id = market.get("condition_id")
                
                if not condition_id:
                    logger.warning(f"Market missing condition_id, logging for debugging: {market.get('question', 'Unknown question')}")
                    skipped_markets.append({
                        "reason": "missing_condition_id",
                        "market_data": market
                    })
                    continue
                
                # Extract or generate a market slug
                market_slug = market.get("slug") or f"market_{condition_id}"
                market_slugs.append(market_slug)
                
                # Fetch detailed market data
                try:
                    market_detail = client.get_market(condition_id)
                except Exception as detail_error:
                    logger.warning(f"Error fetching details for market {condition_id}: {str(detail_error)}")
                    market_detail = {}
                
                # Extract tags if available
                tags = []
                if "categories" in market:
                    tags = market["categories"]
                elif "category" in market:
                    tags = [market["category"]]
                
                # Extract required metadata
                metadata[market_slug] = {
                    "metadata": {
                        "question": market.get("question", "Unknown"),
                        "description": market_detail.get("description", ""),
                        "status": market.get("status", "Unknown"),
                        "tags": tags
                    }
                }
                
            except Exception as e:
                logger.warning(f"Error processing market: {str(e)}")
                skipped_markets.append({
                    "reason": "processing_error",
                    "error": str(e),
                    "market_data": market
                })
                continue
        
        logger.info(f"Successfully retrieved metadata for {len(metadata)} markets")
        logger.info(f"Skipped {len(skipped_markets)} markets due to missing condition_id or errors")
        
        # Save metadata to file
        metadata_file_path = os.path.expanduser("~/workspace/market_metadata.json")
        save_to_json(metadata, metadata_file_path)
        
        # Save market slugs list to file
        market_list_file_path = os.path.expanduser("~/workspace/market_list.json")
        save_to_json(market_slugs, market_list_file_path)
        
        # Save skipped markets to file
        skipped_markets_file_path = os.path.expanduser("~/workspace/skipped_markets.json")
        save_to_json(skipped_markets, skipped_markets_file_path)
        
        return metadata, market_slugs, skipped_markets
        
    except Exception as e:
        logger.error(f"Failed to fetch market metadata: {str(e)}")
        raise
    
def save_to_json(data: Any, filename: str) -> None:
    """
    Saves data to a JSON file.
    
    Args:
        data (Any): The data to save.
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

def test_single_market():
    """
    Test function to fetch and print data for a single market.
    """
    logger.info("Running test: fetching data for a single market")
    try:
        # Fetch all metadata (limited to 500 markets)
        metadata, market_slugs, skipped_markets = fetch_market_metadata(limit=500)
        
        if not metadata:
            logger.error("No markets found in metadata")
            return
        
        # Select one random market for testing
        market_slug = random.choice(list(metadata.keys()))
        market_data = metadata[market_slug]
        
        print("\n==== TEST MARKET DATA ====")
        print(f"Market Slug: {market_slug}")
        print(f"Question: {market_data['metadata']['question']}")
        print(f"Status: {market_data['metadata']['status']}")
        print(f"Tags: {', '.join(market_data['metadata']['tags'])}")
        print(f"Description: {market_data['metadata']['description'][:100]}..." if market_data['metadata']['description'] else "Description: None")
        print("========================\n")
        
        print(f"Total markets processed: {len(metadata)}")
        print(f"Total markets skipped: {len(skipped_markets)}")
        print(f"Market list saved to ~/workspace/market_list.json with {len(market_slugs)} entries")
        
        logger.info("Test completed successfully")
        
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")

def main():
    """
    Main function to execute the script.
    """
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        # Run test with a single market
        try:
            test_single_market()
            return 0
        except Exception as e:
            logger.error(f"Test execution failed: {str(e)}")
            return 1
    else:
        # Run normal operation
        try:
            metadata, market_slugs, skipped_markets = fetch_market_metadata()
            print(f"Script executed successfully. Processed {len(metadata)} markets.")
            print(f"Skipped {len(skipped_markets)} markets due to missing condition_id or errors.")
            print("Market metadata saved to ~/workspace/market_metadata.json")
            print("Market list saved to ~/workspace/market_list.json")
            print("Skipped markets saved to ~/workspace/skipped_markets.json")
            print("To run the test for a single market, use: python fetch_market_metadata.py --test")
            return 0
        except Exception as e:
            logger.error(f"Script execution failed: {str(e)}")
            return 1

if __name__ == "__main__":
    sys.exit(main())
