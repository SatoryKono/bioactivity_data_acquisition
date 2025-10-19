import requests
from src.library.pipelines.target.iuphar_target import _parse_iuphar_target

def debug_iuphar():
    """Debug IUPHAR data extraction."""
    # Test IUPHAR API directly
    base_url = "https://www.guidetopharmacology.org/services"
    
    # Test search endpoint
    search_url = f"{base_url}/targets"
    params = {
        "search": "SGK1",  # One of our test targets
        "limit": 10
    }
    
    print("=== Testing IUPHAR API ===")
    print(f"Search URL: {search_url}")
    print(f"Params: {params}")
    
    try:
        response = requests.get(search_url, params=params, timeout=30)
        print(f"Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"Response type: {type(data)}")
            print(f"Response length: {len(data) if isinstance(data, list) else 'Not a list'}")
            
            if isinstance(data, list) and data:
                print(f"First result: {data[0]}")
                
                # Parse using our function
                parsed = _parse_iuphar_target(data[0])
                print(f"\n=== Parsed Results ===")
                for key, value in parsed.items():
                    print(f"{key}: {value}")
            else:
                print("No results found")
        else:
            print(f"Error response: {response.text}")
            
    except Exception as e:
        print(f"Exception: {e}")
    
    print("\n" + "="*50)
    
    # Test with different search terms
    test_terms = ["SGK1", "ABL1", "BRAF", "kinase", "receptor"]
    
    for term in test_terms:
        print(f"\n=== Testing search term: {term} ===")
        try:
            response = requests.get(search_url, params={"search": term, "limit": 5}, timeout=30)
            if response.status_code == 200:
                data = response.json()
                print(f"Found {len(data) if isinstance(data, list) else 0} results")
                if isinstance(data, list) and data:
                    print(f"First result keys: {list(data[0].keys())}")
            else:
                print(f"Error: {response.status_code}")
        except Exception as e:
            print(f"Exception: {e}")

if __name__ == "__main__":
    debug_iuphar()