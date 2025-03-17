#!/usr/bin/env python3
import os
import json
import logging
import sys
import requests
import time
from typing import Dict, List, Any, Optional

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Directory to save files
OUTPUT_DIR = "/workspaces/py-clob-client/"

def fetch_market_metadata(limit: int = 500) -> Dict[str, Dict[str, Any]]:
    """
    Fetches market metadata from the Polymarket CLOB API.
    
    Args:
        limit (int): Maximum number of markets to fetch. Defaults to 500.
        
    Returns:
        Dict[str, Dict[str, Any]]: Dictionary of market metadata keyed by market slug.
    """
    # Load API credentials from environment variables
    api_key = os.getenv("CLOB_API_KEY")
    api_secret = os.getenv("CLOB_SECRET")
    api_passphrase = os.getenv("CLOB_PASS_PHRASE")
    api_url = os.getenv("CLOB_API_URL", "https://clob.polymarket.com")
    
    # Check if credentials are set
    if not all([api_key, api_secret, api_passphrase]):
        error_msg = "API credentials not fully set. Please set CLOB_API_KEY, CLOB_SECRET, and CLOB_PASS_PHRASE environment variables."
        logger.error(error_msg)
        raise Exception(error_msg)
    
    # Set up headers with authentication
    headers = {
        "Content-Type": "application/json",
        "X-API-KEY": api_key,
        "X-API-SECRET": api_secret,
        "X-API-PASSPHRASE": api_passphrase
    }
    
    # Define endpoint for fetching markets
    markets_endpoint = f"{api_url}/simplified-markets"
    
    logger.info(f"Fetching up to {limit} markets from {markets_endpoint}")
    
    # Initialize variables for pagination
    next_cursor = "MA=="  # Initial cursor
    end_cursor = "MA"  # End cursor
    markets_data = {}
    market_slugs = []
    skipped_markets = {}
    count = 0
    
    try:
        # Fetch markets with pagination until we reach the limit or there are no more markets
        while next_cursor != end_cursor and count < limit:
            url = f"{markets_endpoint}?next_cursor={next_cursor}"
            
            # Make API request with retry mechanism
            max_retries = 3
            retry_delay = 2  # seconds
            
            data = None
            for retry in range(max_retries):
                try:
                    logger.info(f"Making API request (attempt {retry + 1}/{max_retries})")
                    response = requests.get(url, headers=headers)
                    
                    # Check for rate limiting
                    if response.status_code == 429:
                        retry_after = int(response.headers.get("Retry-After", retry_delay))
                        logger.warning(f"Rate limited. Waiting for {retry_after} seconds...")
                        time.sleep(retry_after)
                        continue
                    
                    # Check for other errors
                    response.raise_for_status()
                    
                    # Parse response
                    data = response.json()
                    break
                except requests.exceptions.RequestException as e:
                    if retry < max_retries - 1:
                        logger.warning(f"Request failed: {str(e)}. Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                    else:
                        logger.error(f"Failed to fetch data after {max_retries} attempts: {str(e)}")
                        raise
            
            if not data:
                logger.error("Failed to retrieve valid data from API")
                break
                
            # Process markets from the current page
            current_markets = data.get("data", [])
            next_cursor = data.get("next_cursor", end_cursor)
            
            for market in current_markets:
                # Skip if we've reached the limit
                if count >= limit:
                    break
                
                # Ensure we have a valid market slug
                market_slug = market.get("slug")
                if not market_slug:
                    logger.warning("Skipping market with no slug")
                    continue
                
                condition_id = market.get("condition_id")
                
                # Skip markets without condition_id
                if not condition_id:
                    logger.warning(f"Skipping market {market_slug}: Missing condition_id")
                    skipped_markets[market_slug] = {
                        "reason": "Missing condition_id",
                        "market_data": market
                    }
                    continue
                
                # Extract relevant metadata
                metadata = {
                    "metadata": {
                        "question": market.get("question", ""),
                        "description": market.get("description", ""),
                        "status": market.get("status", ""),
                        "tags": market.get("tags", [])
                    }
                }
                
                # Add to results
                markets_data[market_slug] = metadata
                market_slugs.append(market_slug)
                count += 1
                
                if count % 50 == 0:
                    logger.info(f"Processed {count} markets")
        
        logger.info(f"Successfully fetched metadata for {len(markets_data)} markets")
        logger.info(f"Skipped {len(skipped_markets)} markets due to missing condition_id")
        
        # Save results to files
        save_results(markets_data, market_slugs, skipped_markets)
        
        return markets_data
    
    except Exception as e:
        logger.error(f"Error fetching market metadata: {str(e)}")
        # Save whatever data we have collected so far
        if markets_data or market_slugs or skipped_markets:
            logger.info("Saving partial results before exiting due to error")
            save_results(markets_data, market_slugs, skipped_markets)
        raise

def save_results(
    markets_data: Dict[str, Dict[str, Any]], 
    market_slugs: List[str], 
    skipped_markets: Dict[str, Dict[str, Any]]
) -> None:
    """
    Saves the fetched data to JSON files.
    
    Args:
        markets_data: Dictionary of market metadata keyed by market slug.
        market_slugs: List of market slugs.
        skipped_markets: Dictionary of skipped markets with reasons.
    """
    try:
        # Ensure output directory exists
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        # Save market metadata
        if markets_data:
            metadata_file = os.path.join(OUTPUT_DIR, "market_metadata.json")
            with open(metadata_file, 'w') as f:
                json.dump(markets_data, f, indent=2)
            logger.info(f"Saved market metadata for {len(markets_data)} markets to {metadata_file}")
        else:
            logger.warning("No market metadata to save")
        
        # Save market slugs list
        if market_slugs:
            slugs_file = os.path.join(OUTPUT_DIR, "market_list.json")
            with open(slugs_file, 'w') as f:
                json.dump(market_slugs, f, indent=2)
            logger.info(f"Saved {len(market_slugs)} market slugs to {slugs_file}")
        else:
            logger.warning("No market slugs to save")
        
        # Save skipped markets if any
        if skipped_markets:
            # Format skipped markets to only include reason
            formatted_skipped = {
                slug: {"reason": data.get("reason", "Unknown reason")}
                for slug, data in skipped_markets.items()
            }
            skipped_file = os.path.join(OUTPUT_DIR, "skipped_markets.json")
            with open(skipped_file, 'w') as f:
                json.dump(formatted_skipped, f, indent=2)
            logger.info(f"Saved {len(skipped_markets)} skipped markets to {skipped_file}")
    
    except Exception as e:
        logger.error(f"Error saving results: {str(e)}")
        raise

def test_fetch_market_metadata():
    """
    Test function to fetch one market and print its metadata.
    """
    print("Running test for fetch_market_metadata.py script...")
    
    try:
        # Fetch just one market for testing
        market_data = fetch_market_metadata(limit=1)
        
        if not market_data:
            print("❌ Test failed: No market data fetched.")
            return
        
        # Get the first market
        if market_data:
            market_slug = next(iter(market_data))
            market_metadata = market_data[market_slug]
            
            print("✅ Test successful!")
            print(f"Market slug: {market_slug}")
            print("Market metadata:")
            print(json.dumps(market_metadata, indent=2))
        else:
            print("❌ Test failed: No markets found in the response.")
        
    except Exception as e:
        print(f"❌ Test failed with error: {str(e)}")

if __name__ == "__main__":
    # Load private key from environment if needed for API auth
    if not os.getenv("PK"):
        logger.warning("PK environment variable not set. Some API calls may fail.")
    
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        test_fetch_market_metadata()
    else:
        fetch_market_metadata()
        print("Script executed successfully. To run test, use: python fetch_market_metadata.py --test")
