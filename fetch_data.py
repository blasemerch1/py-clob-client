#!/usr/bin/env python3
import requests
import json
import logging
import sys
from typing import Dict, List, Any, Optional
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# API endpoint
POLYMARKET_API_URL = "https://gamma-api.polymarket.com/query"

def get_query_string() -> str:
    """
    Constructs the GraphQL query string to fetch closed markets data.
    
    Returns:
        str: The constructed GraphQL query string.
    """
    return """
    query {
        markets(where: {isResolved: true}, orderBy: volume, orderDirection: desc, first: 100) {
            id
            question
            category
            isResolved
            resolution
            resolutionValue
            outcomes
            volume
            historicalPrices {
                timestamp
                price
                outcome
            }
        }
    }
    """

def fetch_query_results(query: str) -> Dict[str, Any]:
    """
    Fetches data from Polymarket's GraphQL API.
    
    Args:
        query (str): The GraphQL query string.
        
    Returns:
        Dict[str, Any]: The JSON response from the API.
        
    Raises:
        Exception: If the request fails or returns an error.
    """
    try:
        response = requests.post(
            POLYMARKET_API_URL,
            json={"query": query},
            headers={"Content-Type": "application/json"}
        )
        
        # Check if request was successful
        response.raise_for_status()
        
        # Parse the response
        data = response.json()
        
        # Check for errors in the response
        if "errors" in data:
            error_message = ", ".join([error.get("message", "Unknown error") for error in data["errors"]])
            raise Exception(f"GraphQL query returned errors: {error_message}")
        
        # Check if the expected data structure is present
        if "data" not in data or "markets" not in data.get("data", {}):
            raise Exception("Missing expected fields in the response")
        
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch data from API: {str(e)}")
        raise Exception(f"Failed to fetch data from API: {str(e)}")
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse API response: {str(e)}")
        raise Exception(f"Failed to parse API response: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise

def process_market_data(raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Processes the raw API response to extract and format market data.
    
    Args:
        raw_data (Dict[str, Any]): The raw API response.
        
    Returns:
        List[Dict[str, Any]]: A list of processed market data dictionaries.
    """
    processed_markets = []
    
    try:
        markets = raw_data.get("data", {}).get("markets", [])
        
        for market in markets:
            processed_market = {
                "id": market.get("id", "unknown"),
                "question": market.get("question", "unknown"),
                "category": market.get("category", "unknown"),
                "is_resolved": market.get("isResolved", False),
                "resolution": market.get("resolution", "unknown"),
                "outcome": market.get("resolutionValue", "unknown"),
                "volume": market.get("volume", "0"),
                "outcomes": market.get("outcomes", []),
                "historical_prices": []
            }
            
            # Process historical prices if available
            historical_prices = market.get("historicalPrices", [])
            for price_entry in historical_prices:
                if all(k in price_entry for k in ["timestamp", "price", "outcome"]):
                    processed_market["historical_prices"].append({
                        "timestamp": price_entry["timestamp"],
                        "price": price_entry["price"],
                        "outcome": price_entry["outcome"]
                    })
            
            processed_markets.append(processed_market)
        
        return processed_markets
    except Exception as e:
        logger.error(f"Error processing market data: {str(e)}")
        raise Exception(f"Error processing market data: {str(e)}")

def save_to_json(data: List[Dict[str, Any]], filename: str = "market_data.json") -> None:
    """
    Saves the processed market data to a JSON file.
    
    Args:
        data (List[Dict[str, Any]]): The processed market data.
        filename (str, optional): The name of the output file. Defaults to "market_data.json".
        
    Raises:
        Exception: If writing to the file fails.
    """
    try:
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        logger.info(f"Successfully saved data to {filename}")
    except Exception as e:
        logger.error(f"Failed to save data to {filename}: {str(e)}")
        raise Exception(f"Failed to save data to {filename}: {str(e)}")

def main() -> List[Dict[str, Any]]:
    """
    Main function to orchestrate the data fetching and saving process.
    
    Returns:
        List[Dict[str, Any]]: The processed market data.
    """
    logger.info("Starting data fetch from Polymarket API")
    
    try:
        query = get_query_string()
        raw_data = fetch_query_results(query)
        processed_data = process_market_data(raw_data)
        save_to_json(processed_data)
        logger.info(f"Successfully fetched and saved data for {len(processed_data)} markets")
        return processed_data
    except Exception as e:
        logger.error(f"Data fetch failed: {str(e)}")
        if __name__ == "__main__":
            exit(1)
        raise

def test_fetch_data_script() -> None:
    """
    Tests the Polymarket data fetching functionality.
    
    This function tests if the script can successfully fetch data from Polymarket's API,
    process it, and verify that the result contains market data.
    """
    print("Running test for fetch_data.py script...")
    
    try:
        # Run the main function to fetch and process data
        # Note: This will also save the file, which is fine for testing
        processed_data = main()
        
        # Check if data is not empty
        if not processed_data:
            print("❌ Test failed: Fetched data is empty.")
            return
            
        # Check if we have market data with expected fields
        sample_market = processed_data[0]
        required_fields = ["id", "question", "category", "is_resolved", "volume"]
        missing_fields = [field for field in required_fields if field not in sample_market]
        
        if missing_fields:
            print(f"❌ Test failed: Market data is missing required fields: {', '.join(missing_fields)}")
            return
            
        # Test passed
        print(f"✅ Test passed! Successfully fetched {len(processed_data)} markets.")
        print(f"Sample market question: '{sample_market['question']}'")
        print(f"Data saved to market_data.json")
        
    except Exception as e:
        print(f"❌ Test failed with error: {str(e)}")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        test_fetch_data_script()
    else:
        main()
        print("Script executed successfully. To run tests, use: python fetch_data.py --test")
