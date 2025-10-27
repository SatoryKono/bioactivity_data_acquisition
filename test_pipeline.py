#!/usr/bin/env python3
"""Test script to diagnose document pipeline issues."""

import sys
import traceback
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from library.documents import DocumentPipeline, load_document_config
from library.documents import write_document_outputs
import pandas as pd

def main():
    try:
        print("=== Document Pipeline Test ===")
        
        # Load config
        print("1. Loading configuration...")
        config = load_document_config("configs/config_document.yaml")
        print(f"   Config loaded")
        print(f"   Config type: {type(config)}")
        print(f"   Config dict keys: {list(config.model_dump().keys())}")
        if hasattr(config, 'validation'):
            print(f"   validation.strict = {config.validation.strict}")
        else:
            print("   No validation field in config")
        
        # Load input data
        print("2. Loading input data...")
        df = pd.read_csv("data/input/document.csv")
        print(f"   Loaded {len(df)} records")
        print(f"   document_chembl_id: {df.iloc[0]['document_chembl_id']}")
        
        # Create pipeline
        print("3. Creating pipeline...")
        pipeline = DocumentPipeline(config)
        print(f"   Pipeline created, strict_mode = {pipeline.validator.strict_mode}")
        
        # Run pipeline
        print("4. Running pipeline...")
        result = pipeline.run(input_data=df)
        print(f"   Pipeline completed: {len(result.data)} records")
        
        # Write outputs
        print("5. Writing outputs...")
        output_paths = write_document_outputs(
            result=result,
            output_dir=config.io.output.dir,
            date_tag="20251025",
            config=config
        )
        
        print("6. Output files created:")
        for name, path in output_paths.items():
            print(f"   {name}: {path}")
            
        print("=== Test completed successfully ===")
        
    except Exception as e:
        print(f"ERROR: {e}")
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
