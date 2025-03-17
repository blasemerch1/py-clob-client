#!/usr/bin/env python3
import json
import requests


def fetch_closed_markets():
    """
    Fetches closed markets data from Polymarket's GraphQL API.
    
    Returns:
        list: A list of dictionaries containing market data, or an empty list if the request fails.
    """
    print("Fetching closed Polymarket markets...")
    
    # GraphQL API endpoint
    url = "https://api.thegraph.com/subgraphs/name/polymarket/matic-markets"
    
    # GraphQL query for closed markets
    query = """
    {
      fixedProductMarketMakers(
        where: {isResolved: true}
        first: 1000
      ) {
        id
        questionId
        title
        outcomeTokenPrices
        resolution
        creator
        volume
        liquidity
        createdAtTimestamp
        resolvedAtTimestamp
      }
    }
    """
    
    # Prepare the request payload
    payload = {"query": query}
    
    try:
        # Send the POST request to the GraphQL API
        response = requests.post(url, json=payload)
        response.raise_for_status()  # Raise an exception for HTTP errors
        
        # Parse the JSON response
        data = response.json()
        
        # Extract the market data from the response
        markets = data.get("data", {}).get("fixedProductMarketMakers", [])
        
        print(f"Successfully fetched {len(markets)} closed markets.")
        return markets
    
    except Exception as e:
        print(f"Error fetching market data: {e}")
        return []


def save_to_json(data, filename):
    """
    Saves the given data to a JSON file with proper formatting.
    
    Args:
        data (list): The data to save.
        filename (str): The name of the file to save the data to.
    """
    try:
        with open(filename, 'w') as f:
            json.dump(data, f, indent=4)
        print(f"Market data saved to {filename}")
    except Exception as e:
        print(f"Error saving data to {filename}: {e}")


def main():
    """
    Main function that orchestrates fetching and saving market data.
    """
    # Fetch closed markets
    markets = fetch_closed_markets()
    
    # Save to JSON file if we have data
    if markets:
        save_to_json(markets, "market_data.json")
    else:
        print("No market data to save.")


if __name__ == "__main__":
    main()
