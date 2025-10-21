"""Tests for atomic write utilities."""

import json

import pandas as pd
import pytest

from library.io_.atomic_writes import (
    atomic_write_context,
    atomic_csv_write,
    atomic_parquet_write,
    atomic_json_write,
    atomic_text_write,
    atomic_binary_write,
    safe_file_operation,
    verify_atomic_write,
    cleanup_backups
)


class TestAtomicWriteContext:
    """Test the atomic write context manager."""

    def test_basic_atomic_write(self, tmp_path):
        """Test basic atomic write functionality."""
        target_path = tmp_path / "test.txt"
        content = "Hello, World!"
        
        with atomic_write_context(target_path) as temp_path:
            temp_path.write_text(content)
        
        assert target_path.exists()
        assert target_path.read_text() == content

    def test_atomic_write_with_backup(self, tmp_path):
        """Test atomic write with backup creation."""
        target_path = tmp_path / "test.txt"
        original_content = "Original content"
        new_content = "New content"
        
        # Create original file
        target_path.write_text(original_content)
        
        with atomic_write_context(target_path, backup=True) as temp_path:
            temp_path.write_text(new_content)
        
        # Check that new content is written
        assert target_path.read_text() == new_content
        
        # Check that backup exists
        backup_path = target_path.with_suffix(f"{target_path.suffix}.backup")
        assert backup_path.exists()
        assert backup_path.read_text() == original_content

    def test_atomic_write_no_backup(self, tmp_path):
        """Test atomic write without backup."""
        target_path = tmp_path / "test.txt"
        original_content = "Original content"
        new_content = "New content"
        
        # Create original file
        target_path.write_text(original_content)
        
        with atomic_write_context(target_path, backup=False) as temp_path:
            temp_path.write_text(new_content)
        
        # Check that new content is written
        assert target_path.read_text() == new_content
        
        # Check that no backup exists
        backup_path = target_path.with_suffix(f"{target_path.suffix}.backup")
        assert not backup_path.exists()

    def test_atomic_write_error_cleanup(self, tmp_path):
        """Test that temporary files are cleaned up on error."""
        target_path = tmp_path / "test.txt"
        
        with pytest.raises(ValueError):
            with atomic_write_context(target_path) as temp_path:
                temp_path.write_text("Some content")
                raise ValueError("Test error")
        
        # Target file should not exist
        assert not target_path.exists()
        
        # No temporary files should remain
        temp_files = list(tmp_path.glob(".*"))
        assert len(temp_files) == 0

    def test_atomic_write_custom_temp_dir(self, tmp_path):
        """Test atomic write with custom temporary directory."""
        target_path = tmp_path / "subdir" / "test.txt"
        temp_dir = tmp_path / "temp"
        content = "Hello, World!"
        
        with atomic_write_context(target_path, temp_dir=temp_dir) as temp_path:
            temp_path.write_text(content)
        
        assert target_path.exists()
        assert target_path.read_text() == content

    def test_atomic_write_logging(self, tmp_path):
        """Test atomic write with logging."""
        target_path = tmp_path / "test.txt"
        content = "Hello, World!"
        
        # Test without logger (should not crash)
        with atomic_write_context(target_path, logger=None) as temp_path:
            temp_path.write_text(content)
        
        # Should have completed successfully
        assert target_path.exists()
        assert target_path.read_text() == content


class TestAtomicDataFrameWrites:
    """Test atomic DataFrame write functions."""

    def test_atomic_csv_write(self, tmp_path):
        """Test atomic CSV write."""
        target_path = tmp_path / "test.csv"
        df = pd.DataFrame({
            "A": [1, 2, 3],
            "B": ["x", "y", "z"]
        })
        
        result_path = atomic_csv_write(df, target_path)
        
        assert result_path == target_path
        assert target_path.exists()
        
        # Verify content (note: pandas adds index column by default)
        loaded_df = pd.read_csv(target_path)
        # Remove the index column that pandas adds
        if 'Unnamed: 0' in loaded_df.columns:
            loaded_df = loaded_df.drop('Unnamed: 0', axis=1)
        pd.testing.assert_frame_equal(df, loaded_df)

    def test_atomic_parquet_write(self, tmp_path):
        """Test atomic Parquet write."""
        target_path = tmp_path / "test.parquet"
        df = pd.DataFrame({
            "A": [1, 2, 3],
            "B": ["x", "y", "z"]
        })
        
        result_path = atomic_parquet_write(df, target_path)
        
        assert result_path == target_path
        assert target_path.exists()
        
        # Verify content
        loaded_df = pd.read_parquet(target_path)
        pd.testing.assert_frame_equal(df, loaded_df)

    def test_atomic_csv_write_with_options(self, tmp_path):
        """Test atomic CSV write with custom options."""
        target_path = tmp_path / "test.csv"
        df = pd.DataFrame({
            "A": [1, 2, 3],
            "B": ["x", "y", "z"]
        })
        
        atomic_csv_write(df, target_path, index=True, sep=";")
        
        assert target_path.exists()
        
        # Verify content with custom separator
        loaded_df = pd.read_csv(target_path, sep=";", index_col=0)
        pd.testing.assert_frame_equal(df, loaded_df)


class TestAtomicDataWrites:
    """Test atomic data write functions."""

    def test_atomic_json_write(self, tmp_path):
        """Test atomic JSON write."""
        target_path = tmp_path / "test.json"
        data = {"key": "value", "number": 42, "list": [1, 2, 3]}
        
        result_path = atomic_json_write(data, target_path)
        
        assert result_path == target_path
        assert target_path.exists()
        
        # Verify content
        with open(target_path) as f:
            loaded_data = json.load(f)
        assert loaded_data == data

    def test_atomic_text_write(self, tmp_path):
        """Test atomic text write."""
        target_path = tmp_path / "test.txt"
        content = "Hello, World!\nThis is a test."
        
        result_path = atomic_text_write(content, target_path)
        
        assert result_path == target_path
        assert target_path.exists()
        assert target_path.read_text() == content

    def test_atomic_binary_write(self, tmp_path):
        """Test atomic binary write."""
        target_path = tmp_path / "test.bin"
        content = b"Hello, World!\x00\x01\x02"
        
        result_path = atomic_binary_write(content, target_path)
        
        assert result_path == target_path
        assert target_path.exists()
        assert target_path.read_bytes() == content


class TestSafeFileOperation:
    """Test safe file operation wrapper."""

    def test_safe_file_operation_basic(self, tmp_path):
        """Test basic safe file operation functionality."""
        target_path = tmp_path / "test.txt"
        
        def simple_operation():
            return "success"
        
        result = safe_file_operation(simple_operation, target_path)
        
        # Operation should complete successfully
        assert result == "success"


class TestUtilityFunctions:
    """Test utility functions."""

    def test_verify_atomic_write_success(self, tmp_path):
        """Test successful atomic write verification."""
        target_path = tmp_path / "test.txt"
        content = "Hello, World!"
        target_path.write_text(content)
        
        assert verify_atomic_write(target_path) is True
        assert verify_atomic_write(target_path, expected_size=len(content.encode())) is True

    def test_verify_atomic_write_failure(self, tmp_path):
        """Test failed atomic write verification."""
        target_path = tmp_path / "nonexistent.txt"
        
        assert verify_atomic_write(target_path) is False
        assert verify_atomic_write(target_path, expected_size=100) is False

    def test_verify_atomic_write_wrong_size(self, tmp_path):
        """Test atomic write verification with wrong size."""
        target_path = tmp_path / "test.txt"
        content = "Hello, World!"
        target_path.write_text(content)
        
        # Wrong expected size should fail
        assert verify_atomic_write(target_path, expected_size=999) is False

    def test_cleanup_backups(self, tmp_path):
        """Test backup cleanup functionality."""
        # Create some backup files
        backup1 = tmp_path / "file1.csv.backup"
        backup2 = tmp_path / "file2.txt.backup"
        regular_file = tmp_path / "file3.csv"
        
        backup1.write_text("backup1")
        backup2.write_text("backup2")
        regular_file.write_text("regular")
        
        # Clean up backups
        removed_count = cleanup_backups(tmp_path)
        
        assert removed_count == 2
        assert not backup1.exists()
        assert not backup2.exists()
        assert regular_file.exists()  # Regular file should remain

    def test_cleanup_backups_custom_pattern(self, tmp_path):
        """Test backup cleanup with custom pattern."""
        # Create files with different backup patterns
        backup1 = tmp_path / "file1.csv.backup"
        backup2 = tmp_path / "file2.txt.old"
        backup3 = tmp_path / "file3.csv.tmp"
        
        backup1.write_text("backup1")
        backup2.write_text("backup2")
        backup3.write_text("backup3")
        
        # Clean up only .old files
        removed_count = cleanup_backups(tmp_path, pattern="*.old")
        
        assert removed_count == 1
        assert backup1.exists()  # .backup file should remain
        assert not backup2.exists()  # .old file should be removed
        assert backup3.exists()  # .tmp file should remain


class TestIntegration:
    """Integration tests for atomic writes."""

    def test_concurrent_writes_simulation(self, tmp_path):
        """Simulate concurrent writes to test atomic behavior."""
        target_path = tmp_path / "concurrent.txt"
        
        # Simulate two concurrent writes
        def write_content_1():
            with atomic_write_context(target_path) as temp_path:
                temp_path.write_text("Content 1")
        
        def write_content_2():
            with atomic_write_context(target_path) as temp_path:
                temp_path.write_text("Content 2")
        
        # Execute writes sequentially (in real scenario they'd be concurrent)
        write_content_1()
        assert target_path.read_text() == "Content 1"
        
        write_content_2()
        assert target_path.read_text() == "Content 2"

    def test_large_file_atomic_write(self, tmp_path):
        """Test atomic write with a large file."""
        target_path = tmp_path / "large.csv"
        
        # Create a large DataFrame
        large_df = pd.DataFrame({
            "A": range(10000),
            "B": [f"value_{i}" for i in range(10000)],
            "C": [i * 0.1 for i in range(10000)]
        })
        
        atomic_csv_write(large_df, target_path)
        
        assert target_path.exists()
        
        # Verify content (remove index column if present)
        loaded_df = pd.read_csv(target_path)
        if 'Unnamed: 0' in loaded_df.columns:
            loaded_df = loaded_df.drop('Unnamed: 0', axis=1)
        pd.testing.assert_frame_equal(large_df, loaded_df)

    def test_nested_directory_creation(self, tmp_path):
        """Test atomic write with nested directory creation."""
        target_path = tmp_path / "nested" / "deep" / "file.txt"
        content = "Nested content"
        
        with atomic_write_context(target_path) as temp_path:
            temp_path.write_text(content)
        
        assert target_path.exists()
        assert target_path.read_text() == content
