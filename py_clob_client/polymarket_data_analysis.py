#!/usr/bin/env python3
import os
import json
import requests
import time
from datetime import datetime


def to_timestamp_ms(dt_str):
    """Convert a datetime string to timestamp in milliseconds."""
    if not dt_str:
        return None
    dt = datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
    return int(dt.timestamp() * 1000)


# Load API credentials from environment variables
PK = os.getenv("PK")
CLOB_API_KEY = os.getenv("CLOB_API_KEY")
CLOB_SECRET = os.getenv("CLOB_SECRET")
CLOB_PASS_PHRASE = os.getenv("CLOB_PASS_PHRASE")
CLOB_API_URL = os.getenv("CLOB_API_URL", "https://clob.polymarket.com")

# Ensure credentials are loaded
if not all([PK, CLOB_API_KEY, CLOB_SECRET, CLOB_PASS_PHRASE]):
    raise ValueError(
        "Missing one or more API credentials. Make sure they are set in ~/.bashrc and loaded."
    )


def get_market_data(limit=20, offset=0):
    """
    Fetch market data from Polymarket CLOB API.
    
    Args:
        limit (int): Maximum number of markets to return
        offset (int): Number of markets to skip
        
    Returns:
        dict: JSON response with market data or None if request failed
    """
    headers = {
        "Authorization": f"Bearer {CLOB_API_KEY}",
        "X-Secret": CLOB_SECRET,
        "X-Passphrase": CLOB_PASS_PHRASE
    }
    
    params = {
        "limit": limit,
        "offset": offset
    }
    
    try:
        response = requests.get(f"{CLOB_API_URL}/markets", headers=headers, params=params)
        
        if response.status_code == 200:
            data = response.json()
            
            # Debug: Print the raw API response structure
            print("\n--- Raw API Response Structure ---")
            print(json.dumps({k: type(v).__name__ for k, v in data.items()}, indent=2))
            
            # Check if expected keys exist in the response
            if 'markets' not in data and 'data' not in data:
                print("\nERROR: Neither 'markets' nor 'data' key found in API response!")
                print("\n--- Raw API Response Content ---")
                print(json.dumps(data, indent=2)[:1000])  # Print first 1000 chars to avoid overwhelming output
                print("..." if len(json.dumps(data)) > 1000 else "")
                return None
                
            return data
        else:
            print(f"API Error: {response.status_code} - {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Failed to parse API response as JSON: {e}")
        print(f"Raw response: {response.text[:500]}...")  # Print first 500 chars of the response
        return None


def get_market_orderbook(market_id):
    """
    Fetch orderbook data for a specific market.
    
    Args:
        market_id (str): The market ID to fetch orderbook for
        
    Returns:
        dict: JSON response with orderbook data or None if request failed
    """
    headers = {
        "Authorization": f"Bearer {CLOB_API_KEY}",
        "X-Secret": CLOB_SECRET,
        "X-Passphrase": CLOB_PASS_PHRASE
    }
    
    try:
        response = requests.get(f"{CLOB_API_URL}/orderbook/{market_id}", headers=headers)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"API Error: {response.status_code} - {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Failed to parse orderbook API response as JSON: {e}")
        print(f"Raw response: {response.text[:500]}...")  # Print first 500 chars of the response
        return None


def analyze_market_data(market_data):
    """
    Process and analyze market data.
    
    Args:
        market_data (dict): Market data from the API
        
    Returns:
        dict: Processed market insights with consistent keys
    """
    # Initialize insights with default values
    insights = {
        "total_markets": 0,
        "active_markets": 0,
        "market_summary": [],
        "error": None
    }
    
    # Check if market data is valid
    if not market_data:
        insights["error"] = "No market data received from API"
        return insights
    
    # Check for 'markets' key (old API structure)
    if 'markets' in market_data:
        insights["total_markets"] = len(market_data['markets'])
        
        for market in market_data['markets']:
            is_active = market.get('status') == 'active'
            
            if is_active:
                insights['active_markets'] += 1
            
            market_summary = {
                "id": market.get('id', 'unknown'),
                "name": market.get('name', 'unnamed'),
                "status": market.get('status', 'unknown'),
                "condition": market.get('condition', 'unknown'),
                "created_at": market.get('createdAt', ''),
                "updated_at": market.get('updatedAt', ''),
                "is_active": is_active
            }
            
            insights['market_summary'].append(market_summary)
    
    # Check for 'data' key (new API structure)
    elif 'data' in market_data:
        data = market_data['data']
        
        # If data is a list, count it directly
        if isinstance(data, list):
            insights["total_markets"] = len(data)
            
            for market in data:
                is_active = market.get('status') == 'active'
                
                if is_active:
                    insights['active_markets'] += 1
                
                market_summary = {
                    "id": market.get('id', 'unknown'),
                    "name": market.get('name', 'unnamed'),
                    "status": market.get('status', 'unknown'),
                    "condition": market.get('condition', 'unknown'),
                    "created_at": market.get('createdAt', ''),
                    "updated_at": market.get('updatedAt', ''),
                    "is_active": is_active
                }
                
                insights['market_summary'].append(market_summary)
        
        # If data is a dictionary with market IDs as keys
        elif isinstance(data, dict):
            insights["total_markets"] = len(data)
            
            for market_id, market in data.items():
                # Ensure market is a dictionary
                if not isinstance(market, dict):
                    continue
                
                # Add ID from the key if not in the market data
                if 'id' not in market:
                    market['id'] = market_id
                
                is_active = market.get('status') == 'active'
                
                if is_active:
                    insights['active_markets'] += 1
                
                market_summary = {
                    "id": market.get('id', 'unknown'),
                    "name": market.get('name', 'unnamed'),
                    "status": market.get('status', 'unknown'),
                    "condition": market.get('condition', 'unknown'),
                    "created_at": market.get('createdAt', ''),
                    "updated_at": market.get('updatedAt', ''),
                    "is_active": is_active
                }
                
                insights['market_summary'].append(market_summary)
        else:
            insights["error"] = f"Invalid 'data' format: expected list or dict, got {type(data).__name__}"
            return insights
    
    # Neither key exists
    else:
        insights["error"] = "Invalid market data format: neither 'markets' nor 'data' key found"
        print("\nAvailable keys in market_data:", list(market_data.keys()))
        return insights
    
    return insights


if __name__ == "__main__":
    print("Fetching Polymarket data...")
    
    # Fetch market data
    market_data = get_market_data()
    
    if market_data is None:
        print("Failed to fetch market data from the API. See the errors above for details.")
        exit(1)
        
    insights = analyze_market_data(market_data)
    
    # Check for errors
    if insights["error"]:
        print(f"Error: {insights['error']}")
        
        # Print the actual response structure to help debug
        print("\nActual response structure:")
        if market_data:
            print(f"Response type: {type(market_data)}")
            if isinstance(market_data, dict):
                print(f"Available keys: {list(market_data.keys())}")
                for key, value in market_data.items():
                    print(f"Key: {key}, Type: {type(value)}")
                    if isinstance(value, (list, dict)):
                        print(f"  Length/Size: {len(value)}")
    else:
        print("\n--- Market Overview ---")
        print(f"Total Markets: {insights['total_markets']}")
        print(f"Active Markets: {insights['active_markets']}")
        
        # Only print market details if we have markets
        if insights['market_summary']:
            # Print details of first 5 markets
            print("\n--- Sample Markets ---")
            for i, market in enumerate(insights['market_summary'][:5]):
                print(f"{i+1}. {market['name']} (Status: {market['status']})")
                print(f"   ID: {market['id']}")
                print(f"   Created: {market['created_at']}")
                
                # Get orderbook for this market
                orderbook = get_market_orderbook(market['id'])
                if orderbook:
                    asks = len(orderbook.get('asks', []))
                    bids = len(orderbook.get('bids', []))
                    print(f"   Orderbook: {bids} bids, {asks} asks")
                else:
                    print("   Orderbook: Unable to retrieve data")
                
                print()
            
            # Save full data to file if available
            try:
                with open("polymarket_data.json", "w") as f:
                    json.dump(market_data, f, indent=2)
                print("Full data saved to polymarket_data.json")
            except Exception as e:
                print(f"Failed to save data: {e}")
        else:
            print("No markets found in the response.")
