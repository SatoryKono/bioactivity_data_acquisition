#!/usr/bin/env python3
"""Debug script to check ChEMBL API batch response for activities."""

import requests
import json
import sys

def check_chembl_batch(activity_ids):
    """Check what ChEMBL API returns for a batch of activity IDs."""
    ids_str = ",".join(activity_ids)
    url = f"https://www.ebi.ac.uk/chembl/api/data/activity.json?activity_id__in={ids_str}&format=json&limit=1000"
    
    print(f"Requesting: {url}")
    
    try:
        response = requests.get(url, timeout=30)
        print(f"Response status: {response.status_code}")
        response.raise_for_status()
        
        data = response.json()
        activities = data.get("activities", [])
        
        print(f"Found {len(activities)} activities in response")
        
        if not activities:
            print("No activities in response")
            return
        
        for activity in activities:
            activity_id = activity.get('activity_id')
            print(f"\n=== Activity {activity_id} ===")
            print(f"Standard Type: {activity.get('standard_type')}")
            print(f"Standard Value: {activity.get('standard_value')}")
            print(f"Standard Units: {activity.get('standard_units')}")
            print(f"Type: {activity.get('type')}")
            print(f"Value: {activity.get('value')}")
            print(f"Units: {activity.get('units')}")
            print(f"Data Validity Comment: {activity.get('data_validity_comment')}")
            print(f"Activity Comment: {activity.get('activity_comment')}")
            
            # Check if standard fields are missing
            missing_fields = []
            if not activity.get('standard_type'):
                missing_fields.append('standard_type')
            if not activity.get('standard_value'):
                missing_fields.append('standard_value')
                
            if missing_fields:
                print(f"Missing required fields: {missing_fields}")
            else:
                print("All required fields present")
                
    except Exception as e:
        print(f"Error checking batch: {e}")

if __name__ == "__main__":
    # Check the first two activities from the input file
    activity_ids = ["33279", "34166"]
    
    check_chembl_batch(activity_ids)
