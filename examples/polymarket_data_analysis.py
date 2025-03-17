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
            return response.json()
        else:
            print(f"API Error: {response.status_code} - {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
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


def analyze_market_data(market_data):
    """
    Process and analyze market data.
    
    Args:
        market_data (dict): Market data from the API
        
    Returns:
        dict: Processed market insights
    """
    if not market_data or 'markets' not in market_data:
        return {"error": "No valid market data available"}
    
    insights = {
        "total_markets": len(market_data['markets']),
        "active_markets": 0,
        "market_summary": []
    }
    
    for market in market_data['markets']:
        is_active = market.get('status') == 'active'
        
        if is_active:
            insights['active_markets'] += 1
        
        market_summary = {
            "id": market.get('id'),
            "name": market.get('name'),
            "status": market.get('status'),
            "condition": market.get('condition'),
            "created_at": market.get('createdAt'),
            "updated_at": market.get('updatedAt'),
            "is_active": is_active
        }
        
        insights['market_summary'].append(market_summary)
    
    return insights


if __name__ == "__main__":
    print("Fetching Polymarket data...")
    
    # Fetch market data
    market_data = get_market_data()
    
    if market_data:
        print("\n--- Market Overview ---")
        insights = analyze_market_data(market_data)
        print(f"Total Markets: {insights['total_markets']}")
        print(f"Active Markets: {insights['active_markets']}")
        
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
            
            print()
        
        # Save full data to file
        with open("polymarket_data.json", "w") as f:
            json.dump(market_data, f, indent=2)
        print("Full data saved to polymarket_data.json")
    else:
        print("Failed to retrieve market data.")
