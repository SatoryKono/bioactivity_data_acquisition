"""Tests for determinism and reproducibility of data extraction."""

import hashlib
import json
import pytest
from unittest.mock import Mock, patch
import pandas as pd

from src.library.clients.chembl_v2 import ChemblClient
from src.library.config.models import ApiCfg, RetryCfg
from library.target.pipeline import TargetPipeline
from src.library.transforms.chembl import parse_target_record


class TestDeterminism:
    """Test deterministic behavior of data extraction."""

    def test_chembl_fetch_deterministic(self):
        """Test that ChemblClient produces deterministic results."""
        # Mock response data
        mock_response_data = {
            "targets": [
                {
                    "target_chembl_id": "CHEMBL123",
                    "pref_name": "Test Target",
                    "target_type": "SINGLE PROTEIN",
                    "tax_id": 9606,
                    "target_components": {
                        "target_component": [
                            {
                                "component_id": 1,
                                "component_description": "Test component",
                                "target_component_synonyms": {
                                    "target_component_synonym": [
                                        {
                                            "component_synonym": "TEST1",
                                            "syn_type": "GENE_SYMBOL"
                                        }
                                    ]
                                }
                            }
                        ]
                    }
                }
            ]
        }

        with patch('src.library.clients.chembl_v2.requests.Session.get') as mock_get:
            # Setup mock response
            mock_response = Mock()
            mock_response.json.return_value = mock_response_data
            mock_response.raise_for_status.return_value = None
            mock_response.status_code = 200
            mock_response.elapsed.total_seconds.return_value = 0.5
            mock_get.return_value.__enter__.return_value = mock_response

            client = ChemblClient()
            api_cfg = ApiCfg()
            
            # Make two identical requests
            result1 = client.fetch("https://test.com/target.json", api_cfg)
            result2 = client.fetch("https://test.com/target.json", api_cfg)
            
            # Results should be identical
            assert result1 == result2
            
            # JSON serialization should be deterministic
            json1 = json.dumps(result1, sort_keys=True)
            json2 = json.dumps(result2, sort_keys=True)
            assert json1 == json2

    def test_target_parsing_deterministic(self):
        """Test that target parsing produces deterministic results."""
        raw_data = {
            "target_chembl_id": "CHEMBL123",
            "pref_name": "Test Target",
            "target_type": "SINGLE PROTEIN",
            "tax_id": 9606,
            "target_components": {
                "target_component": [
                    {
                        "component_id": 1,
                        "component_description": "Test component",
                        "target_component_synonyms": {
                            "target_component_synonym": [
                                {
                                    "component_synonym": "TEST1",
                                    "syn_type": "GENE_SYMBOL"
                                },
                                {
                                    "component_synonym": "TEST2",
                                    "syn_type": "GENE_SYMBOL_OTHER"
                                }
                            ]
                        }
                    }
                ]
            }
        }

        # Parse multiple times
        results = []
        for _ in range(5):
            result = parse_target_record(raw_data)
            results.append(result)

        # All results should be identical
        for i in range(1, len(results)):
            assert results[i] == results[0]

        # JSON serialization should be deterministic
        json_results = [json.dumps(r, sort_keys=True) for r in results]
        for i in range(1, len(json_results)):
            assert json_results[i] == json_results[0]

    def test_ec_number_normalization_deterministic(self):
        """Test that EC number normalization is deterministic."""
        from src.library.transforms.chembl import normalize_reaction_ec_numbers

        test_values = ["EC 1.1.1.1", "1.2.3.4", "EC:2.3.4.5", "3.4.5.6"]
        
        # Normalize multiple times
        results = []
        for _ in range(5):
            result = normalize_reaction_ec_numbers(test_values)
            results.append(result)

        # All results should be identical
        for i in range(1, len(results)):
            assert results[i] == results[0]

    def test_jitter_deterministic(self):
        """Test that jitter function is deterministic with fixed seed."""
        retry_cfg = RetryCfg(backoff_jitter_seed=42)
        jitter_func = retry_cfg.build_jitter()
        
        # Multiple calls should produce same results due to fixed seed
        results = [jitter_func(1.0) for _ in range(10)]
        
        # All results should be the same (deterministic)
        for i in range(1, len(results)):
            assert results[i] == results[0]
        
        # But different from input (jitter applied)
        assert results[0] != 1.0

    def test_pipeline_reproducible(self):
        """Test that target pipeline produces reproducible results."""
        # Mock data for pipeline
        mock_targets = [
            {
                "target_chembl_id": "CHEMBL123",
                "pref_name": "Test Target 1",
                "target_type": "SINGLE PROTEIN",
                "tax_id": 9606,
            },
            {
                "target_chembl_id": "CHEMBL456",
                "pref_name": "Test Target 2", 
                "target_type": "SINGLE PROTEIN",
                "tax_id": 9606,
            }
        ]

        with patch('src.library.clients.chembl_v2.requests.Session.get') as mock_get:
            # Setup mock response
            mock_response = Mock()
            mock_response.json.return_value = {"targets": mock_targets}
            mock_response.raise_for_status.return_value = None
            mock_response.status_code = 200
            mock_response.elapsed.total_seconds.return_value = 0.5
            mock_get.return_value.__enter__.return_value = mock_response

            client = ChemblClient()
            api_cfg = ApiCfg()
            
            # Run pipeline twice
            from src.library.config.models import UniprotMappingCfg
            mapping_cfg = UniprotMappingCfg()
            
            _, _, df1 = fetch_targets(
                ["CHEMBL123", "CHEMBL456"],
                cfg=api_cfg,
                client=client,
                mapping_cfg=mapping_cfg,
                chunk_size=2
            )
            
            _, _, df2 = fetch_targets(
                ["CHEMBL123", "CHEMBL456"],
                cfg=api_cfg,
                client=client,
                mapping_cfg=mapping_cfg,
                chunk_size=2
            )
            
            # DataFrames should be identical
            pd.testing.assert_frame_equal(df1, df2)
            
            # CSV output should be identical
            csv1 = df1.to_csv(index=False)
            csv2 = df2.to_csv(index=False)
            assert csv1 == csv2
            
            # SHA256 should be identical
            hash1 = hashlib.sha256(csv1.encode()).hexdigest()
            hash2 = hashlib.sha256(csv2.encode()).hexdigest()
            assert hash1 == hash2

    def test_json_serialization_deterministic(self):
        """Test that JSON serialization is deterministic."""
        data = {
            "c": 3,
            "a": 1,
            "b": 2,
            "nested": {
                "z": 26,
                "x": 24,
                "y": 25
            }
        }
        
        # Serialize multiple times
        results = []
        for _ in range(5):
            result = json.dumps(data, sort_keys=True, separators=(",", ":"))
            results.append(result)
        
        # All results should be identical
        for i in range(1, len(results)):
            assert results[i] == results[0]
        
        # Should be sorted
        expected = '{"a":1,"b":2,"c":3,"nested":{"x":24,"y":25,"z":26}}'
        assert results[0] == expected

    def test_dataframe_sorting_deterministic(self):
        """Test that DataFrame operations are deterministic."""
        # Create DataFrame with non-deterministic order
        data = [
            {"id": "B", "value": 2},
            {"id": "A", "value": 1},
            {"id": "C", "value": 3},
        ]
        
        df1 = pd.DataFrame(data)
        df2 = pd.DataFrame(data)
        
        # Sort by id to make deterministic
        df1_sorted = df1.sort_values("id").reset_index(drop=True)
        df2_sorted = df2.sort_values("id").reset_index(drop=True)
        
        # Should be identical
        pd.testing.assert_frame_equal(df1_sorted, df2_sorted)
        
        # CSV should be identical
        csv1 = df1_sorted.to_csv(index=False)
        csv2 = df2_sorted.to_csv(index=False)
        assert csv1 == csv2

    def test_multiple_runs_identical(self):
        """Test that multiple runs of the same pipeline produce identical results."""
        # This is a comprehensive test that runs the full pipeline multiple times
        # and verifies that all outputs are byte-identical
        
        mock_targets = [
            {
                "target_chembl_id": "CHEMBL123",
                "pref_name": "Test Target",
                "target_type": "SINGLE PROTEIN",
                "tax_id": 9606,
                "target_components": {
                    "target_component": [
                        {
                            "component_id": 1,
                            "component_description": "Test component",
                            "target_component_synonyms": {
                                "target_component_synonym": [
                                    {
                                        "component_synonym": "TEST1",
                                        "syn_type": "GENE_SYMBOL"
                                    }
                                ]
                            }
                        }
                    ]
                }
            }
        ]

        with patch('src.library.clients.chembl_v2.requests.Session.get') as mock_get:
            # Setup mock response
            mock_response = Mock()
            mock_response.json.return_value = {"targets": mock_targets}
            mock_response.raise_for_status.return_value = None
            mock_response.status_code = 200
            mock_response.elapsed.total_seconds.return_value = 0.5
            mock_get.return_value.__enter__.return_value = mock_response

            client = ChemblClient()
            api_cfg = ApiCfg()
            from src.library.config.models import UniprotMappingCfg
            mapping_cfg = UniprotMappingCfg()
            
            # Run pipeline 3 times
            results = []
            for _ in range(3):
                _, _, df = fetch_targets(
                    ["CHEMBL123"],
                    cfg=api_cfg,
                    client=client,
                    mapping_cfg=mapping_cfg,
                    chunk_size=1
                )
                csv_output = df.to_csv(index=False)
                results.append(csv_output)
            
            # All results should be identical
            for i in range(1, len(results)):
                assert results[i] == results[0]
            
            # SHA256 should be identical
            hashes = [hashlib.sha256(r.encode()).hexdigest() for r in results]
            for i in range(1, len(hashes)):
                assert hashes[i] == hashes[0]
