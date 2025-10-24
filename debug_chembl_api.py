#!/usr/bin/env python3
"""Debug script to check ChEMBL API response for activities."""

import requests
import json
import sys

def check_chembl_activity(activity_id):
    """Check what ChEMBL API returns for a specific activity ID."""
    url = f"https://www.ebi.ac.uk/chembl/api/data/activity/{activity_id}?format=json"
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        activities = data.get("activities", [])
        
        if not activities:
            print(f"Activity {activity_id}: No activities in response")
            return
        
        activity = activities[0]
        print(f"\n=== Activity {activity_id} ===")
        print(f"Activity ID: {activity.get('activity_id')}")
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
            print(f"❌ Missing required fields: {missing_fields}")
        else:
            print("✅ All required fields present")
            
    except Exception as e:
        print(f"Error checking activity {activity_id}: {e}")

if __name__ == "__main__":
    # Check the first two activities from the input file
    activity_ids = ["33279", "34166"]
    
    for activity_id in activity_ids:
        check_chembl_activity(activity_id)
