#!/usr/bin/env python3
import os
import json
import requests
import time
import pandas as pd
from datetime import datetime, timedelta
from collections import OrderedDict


def to_timestamp_ms(dt_str):
    """Convert a datetime string to timestamp in milliseconds."""
    if not dt_str:
        return None
    dt = datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
    return int(dt.timestamp() * 1000)


def from_timestamp_ms(ts_ms):
    """Convert a timestamp in milliseconds to a datetime object."""
    if not ts_ms:
        return None
    return datetime.fromtimestamp(ts_ms / 1000)


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


def get_auth_headers():
    """Get authentication headers for API requests."""
    return {
        "Authorization": f"Bearer {CLOB_API_KEY}",
        "X-Secret": CLOB_SECRET,
        "X-Passphrase": CLOB_PASS_PHRASE
    }


def get_market_data(limit=20, offset=0):
    """
    Fetch market data from Polymarket CLOB API.
    
    Args:
        limit (int): Maximum number of markets to return
        offset (int): Number of markets to skip
        
    Returns:
        dict: JSON response with market data or None if request failed
    """
    headers = get_auth_headers()
    
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


def get_market_metadata(market_id):
    """
    Fetch metadata for a specific market.
    
    Args:
        market_id (str): The market ID to fetch metadata for
        
    Returns:
        dict: JSON response with market metadata or None if request failed
    """
    headers = get_auth_headers()
    
    try:
        response = requests.get(f"{CLOB_API_URL}/markets/{market_id}", headers=headers)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"API Error: {response.status_code} - {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Failed to parse market metadata as JSON: {e}")
        return None


def get_market_orderbook(market_id):
    """
    Fetch orderbook data for a specific market.
    
    Args:
        market_id (str): The market ID to fetch orderbook for
        
    Returns:
        dict: JSON response with orderbook data or None if request failed
    """
    headers = get_auth_headers()
    
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


def get_historical_trades(market_id, limit=100, start_time=None, end_time=None):
    """
    Fetch historical trades for a specific market.
    
    Args:
        market_id (str): The market ID to fetch trades for
        limit (int): Maximum number of trades to return
        start_time (datetime or str): Start time for trades query (optional)
        end_time (datetime or str): End time for trades query (optional)
        
    Returns:
        dict: JSON response with trades data or None if request failed
    """
    headers = get_auth_headers()
    
    params = {"limit": limit}
    
    if start_time:
        if isinstance(start_time, datetime):
            params["startTime"] = int(start_time.timestamp() * 1000)
        else:
            params["startTime"] = to_timestamp_ms(start_time)
    
    if end_time:
        if isinstance(end_time, datetime):
            params["endTime"] = int(end_time.timestamp() * 1000)
        else:
            params["endTime"] = to_timestamp_ms(end_time)
    
    try:
        response = requests.get(f"{CLOB_API_URL}/trades/{market_id}", headers=headers, params=params)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"API Error: {response.status_code} - {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Failed to parse historical trades as JSON: {e}")
        return None


def get_historical_prices(market_id, days=30):
    """
    Calculate historical price data for a specific market over time.
    
    Args:
        market_id (str): The market ID to fetch price data for
        days (int): Number of days to look back for historical data
        
    Returns:
        dict: Dictionary containing price data time series
    """
    end_time = datetime.now()
    start_time = end_time - timedelta(days=days)
    
    # Get all trades for the specified period
    trades_data = get_historical_trades(
        market_id, 
        limit=1000,  # Adjust based on expected volume
        start_time=start_time,
        end_time=end_time
    )
    
    if not trades_data or 'trades' not in trades_data:
        print(f"No historical trades found for market {market_id}")
        return {
            "market_id": market_id,
            "prices": [],
            "error": "No historical trades found"
        }
    
    # Extract and sort trades by time
    trades = trades_data['trades']
    
    # Create a price series from trades
    price_series = []
    for trade in trades:
        price = float(trade.get('price', 0))
        size = float(trade.get('size', 0))
        created_at = trade.get('createdAt')
        timestamp = to_timestamp_ms(created_at)
        
        price_series.append({
            'timestamp': timestamp,
            'datetime': created_at,
            'price': price,
            'size': size
        })
    
    # Sort by timestamp
    price_series.sort(key=lambda x: x['timestamp'])
    
    return {
        "market_id": market_id,
        "prices": price_series,
        "earliest_date": price_series[0]['datetime'] if price_series else None,
        "latest_date": price_series[-1]['datetime'] if price_series else None,
        "price_count": len(price_series)
    }


def get_trading_volume(market_id, period='24h'):
    """
    Calculate trading volume for a specific market.
    
    Args:
        market_id (str): The market ID to fetch volume for
        period (str): Time period for volume calculation ('24h', '7d', '30d')
        
    Returns:
        dict: Dictionary containing volume metrics
    """
    # Determine time range based on period
    end_time = datetime.now()
    
    if period == '24h':
        start_time = end_time - timedelta(hours=24)
    elif period == '7d':
        start_time = end_time - timedelta(days=7)
    elif period == '30d':
        start_time = end_time - timedelta(days=30)
    else:
        start_time = end_time - timedelta(hours=24)  # Default to 24h
    
    # Get all trades for the specified period
    trades_data = get_historical_trades(
        market_id, 
        limit=1000,  # Adjust based on expected volume
        start_time=start_time,
        end_time=end_time
    )
    
    if not trades_data or 'trades' not in trades_data:
        return {
            "market_id": market_id,
            "period": period,
            "volume": 0,
            "trade_count": 0,
            "error": "No trades found for the specified period"
        }
    
    # Extract and calculate volume
    trades = trades_data['trades']
    total_volume = sum(float(trade.get('size', 0)) * float(trade.get('price', 0)) for trade in trades)
    
    return {
        "market_id": market_id,
        "period": period,
        "volume": total_volume,
        "trade_count": len(trades),
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat()
    }


def analyze_orderbook(orderbook):
    """
    Analyze orderbook data to extract key metrics.
    
    Args:
        orderbook (dict): Orderbook data from API
        
    Returns:
        dict: Extracted orderbook metrics
    """
    if not orderbook:
        return {
            "error": "No orderbook data available"
        }
    
    asks = orderbook.get('asks', [])
    bids = orderbook.get('bids', [])
    
    # Calculate metrics
    ask_count = len(asks)
    bid_count = len(bids)
    
    # Calculate best ask and bid prices
    best_ask = float(asks[0]['price']) if ask_count > 0 else None
    best_bid = float(bids[0]['price']) if bid_count > 0 else None
    
    # Calculate spread
    spread = best_ask - best_bid if (best_ask is not None and best_bid is not None) else None
    spread_percentage = (spread / best_bid * 100) if (spread is not None and best_bid is not None and best_bid > 0) else None
    
    # Calculate liquidity (sum of available orders)
    ask_liquidity = sum(float(ask['size']) for ask in asks)
    bid_liquidity = sum(float(bid['size']) for bid in bids)
    
    return {
        "bid_count": bid_count,
        "ask_count": ask_count,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread": spread,
        "spread_percentage": spread_percentage,
        "bid_liquidity": bid_liquidity,
        "ask_liquidity": ask_liquidity,
        "total_liquidity": ask_liquidity + bid_liquidity
    }


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


def analyze_market_complete(market_id):
    """
    Perform a complete analysis of a market including metadata, orderbook, and price history.
    
    Args:
        market_id (str): The market ID to analyze
        
    Returns:
        dict: Comprehensive market analysis
    """
    print(f"Analyzing market {market_id}...")
    
    # Initialize result
    analysis = {
        "market_id": market_id,
        "metadata": None,
        "orderbook_analysis": None,
        "price_history": None,
        "volume_metrics": None,
        "error": None
    }
    
    # Get market metadata
    metadata = get_market_metadata(market_id)
    if metadata:
        analysis["metadata"] = metadata
    else:
        analysis["error"] = "Failed to retrieve market metadata"
        return analysis
    
    # Get and analyze orderbook
    orderbook = get_market_orderbook(market_id)
    if orderbook:
        analysis["orderbook_analysis"] = analyze_orderbook(orderbook)
    
    # Get historical price data
    price_history = get_historical_prices(market_id, days=30)
    if price_history and price_history.get("prices"):
        analysis["price_history"] = price_history
    
    # Get volume metrics for different time periods
    analysis["volume_metrics"] = {
        "24h": get_trading_volume(market_id, period='24h'),
        "7d": get_trading_volume(market_id, period='7d'),
        "30d": get_trading_volume(market_id, period='30d')
    }
    
    return analysis


def format_price_for_display(price):
    """Format price for display purposes."""
    if price is None:
        return "N/A"
    return f"{price:.6f}"


def format_volume_for_display(volume):
    """Format volume for display purposes."""
    if volume is None:
        return "N/A"
    
    if volume >= 1_000_000:
        return f"${volume/1_000_000:.2f}M"
    elif volume >= 1_000:
        return f"${volume/1_000:.2f}K"
    else:
        return f"${volume:.2f}"


if __name__ == "__main__":
    print("Fetching Polymarket data...")
    
    # Fetch market data
    market_data = get_market_data(limit=50)  # Increased limit to get more markets
    
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
            # Find most active markets based on 24h volume
            active_markets = []
            
            print("\nFetching volume data for active markets...")
            for market in insights['market_summary'][:10]:  # Limit to first 10 for performance
                if market['is_active']:
                    market_id = market['id']
                    volume_data = get_trading_volume(market_id, period='24h')
                    market['volume_24h'] = volume_data.get('volume', 0)
                    active_markets.append(market)
            
            # Sort by 24h volume
            active_markets.sort(key=lambda x: x.get('volume_24h', 0), reverse=True)
            
            # Print top 5 most active markets
            print("\n--- Top 5 Most Active Markets (by 24h Volume) ---")
            for i, market in enumerate(active_markets[:5]):
                print(f"{i+1}. {market['name']} (Status: {market['status']})")
                print(f"   ID: {market['id']}")
                print(f"   24h Volume: {format_volume_for_display(market.get('volume_24h', 0))}")
                
                # Perform detailed analysis on this market
                market_analysis = analyze_market_complete(market['id'])
                
                # Print orderbook summary
                if market_analysis.get('orderbook_analysis'):
                    ob = market_analysis['orderbook_analysis']
                    print(f"   Orderbook: {ob.get('bid_count', 0)} bids, {ob.get('ask_count', 0)} asks")
                    print(f"   Best Bid: {format_price_for_display(ob.get('best_bid'))}")
                    print(f"   Best Ask: {format_price_for_display(ob.get('best_ask'))}")
                    if ob.get('spread_percentage') is not None:
                        print(f"   Spread: {ob.get('spread_percentage', 0):.2f}%")
                
                # Print historical price summary
                if market_analysis.get('price_history') and market_analysis['price_history'].get('prices'):
                    prices = market_analysis['price_history']['prices']
                    if prices:
                        oldest_price = prices[0]['price'] if prices else None
                        newest_price = prices[-1]['price'] if prices else None
                        price_change = ((newest_price - oldest_price) / oldest_price * 100) if (oldest_price and newest_price and oldest_price > 0) else None
                        
                        print(f"   30-Day Price Change: {price_change:.2f}%" if price_change is not None else "   30-Day Price Change: N/A")
                        print(f"   Price Data Points: {len(prices)}")
                
                # Print volume metrics
                if market_analysis.get('volume_metrics'):
                    vm = market_analysis['volume_metrics']
                    print(f"   Volume (24h): {format_volume_for_display(vm['24h'].get('volume', 0))}")
                    print(f"   Volume (7d): {format_volume_for_display(vm['7d'].get('volume', 0))}")
                    print(f"   Volume (30d): {format_volume_for_display(vm['30d'].get('volume', 0))}")
                
                print()
            
            # Save full data to file
            try:
                output_data = {
                    "market_data": market_data,
                    "insights": insights,
                    "detailed_analysis": {
                        market['id']: analyze_market_complete(market['id']) 
                        for market in active_markets[:5]
                    }
                }
                
                with open("polymarket_full_analysis.json", "w") as f:
                    json.dump(output_data, f, indent=2)
                print("Full analysis saved to polymarket_full_analysis.json")
            except Exception as e:
                print(f"Failed to save data: {e}")
        else:
            print("No markets found in the response.")
