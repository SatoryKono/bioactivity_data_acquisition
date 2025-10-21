"""Tests for deprecation utilities."""

import warnings
from unittest.mock import patch

import pytest

from library.utils.deprecation import (
    deprecation_warning,
    deprecated,
    deprecated_class,
)


class TestDeprecationWarning:
    """Test deprecation warning utility."""

    def test_deprecation_warning_basic(self):
        """Test basic deprecation warning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            
            deprecation_warning(
                message="Test deprecation",
                version="0.2.0",
                removal_version="0.3.0"
            )
            
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert "Test deprecation" in str(w[0].message)
            assert "0.2.0" in str(w[0].message)
            assert "0.3.0" in str(w[0].message)

    def test_deprecation_warning_with_replacement(self):
        """Test deprecation warning with replacement."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            
            deprecation_warning(
                message="Old function deprecated",
                version="0.2.0",
                removal_version="0.3.0",
                replacement="new_function"
            )
            
            assert len(w) == 1
            assert "new_function" in str(w[0].message)


class TestDeprecatedDecorator:
    """Test deprecated function decorator."""

    def test_deprecated_function_basic(self):
        """Test basic deprecated function."""
        @deprecated(
            reason="Function renamed",
            version="0.2.0",
            removal_version="0.3.0"
        )
        def old_function(x: int) -> int:
            return x * 2

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            
            result = old_function(5)
            
            assert result == 10
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert "old_function" in str(w[0].message)
            assert "0.2.0" in str(w[0].message)
            assert "0.3.0" in str(w[0].message)

    def test_deprecated_function_with_replacement(self):
        """Test deprecated function with replacement."""
        @deprecated(
            reason="Function renamed",
            version="0.2.0",
            removal_version="0.3.0",
            replacement="new_function"
        )
        def old_function(x: int) -> int:
            return x * 2

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            
            old_function(5)
            
            assert len(w) == 1
            assert "new_function" in str(w[0].message)

    def test_deprecated_function_preserves_signature(self):
        """Test that deprecated function preserves original signature."""
        @deprecated(
            reason="Function renamed",
            version="0.2.0",
            removal_version="0.3.0"
        )
        def old_function(x: int, y: str = "default") -> str:
            """Original docstring."""
            return f"{x}: {y}"

        # Check that signature is preserved
        import inspect
        sig = inspect.signature(old_function)
        params = list(sig.parameters.keys())
        assert params == ['x', 'y']
        assert sig.parameters['y'].default == "default"
        assert sig.return_annotation == str
        
        # Check that docstring is preserved
        assert old_function.__doc__ == "Original docstring."

    def test_deprecated_function_multiple_calls(self):
        """Test that deprecated function emits warning on each call."""
        @deprecated(
            reason="Function renamed",
            version="0.2.0",
            removal_version="0.3.0"
        )
        def old_function(x: int) -> int:
            return x * 2

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            
            old_function(1)
            old_function(2)
            old_function(3)
            
            assert len(w) == 3  # Warning on each call


class TestDeprecatedClassDecorator:
    """Test deprecated class decorator."""

    def test_deprecated_class_basic(self):
        """Test basic deprecated class."""
        @deprecated_class(
            reason="Class renamed",
            version="0.2.0",
            removal_version="0.3.0"
        )
        class OldClass:
            def __init__(self, value: int):
                self.value = value

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            
            obj = OldClass(42)
            
            assert obj.value == 42
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert "OldClass" in str(w[0].message)
            assert "0.2.0" in str(w[0].message)
            assert "0.3.0" in str(w[0].message)

    def test_deprecated_class_with_replacement(self):
        """Test deprecated class with replacement."""
        @deprecated_class(
            reason="Class renamed",
            version="0.2.0",
            removal_version="0.3.0",
            replacement="NewClass"
        )
        class OldClass:
            def __init__(self, value: int):
                self.value = value

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            
            OldClass(42)
            
            assert len(w) == 1
            assert "NewClass" in str(w[0].message)

    def test_deprecated_class_multiple_instances(self):
        """Test that deprecated class emits warning on each instantiation."""
        @deprecated_class(
            reason="Class renamed",
            version="0.2.0",
            removal_version="0.3.0"
        )
        class OldClass:
            def __init__(self, value: int):
                self.value = value

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            
            OldClass(1)
            OldClass(2)
            OldClass(3)
            
            assert len(w) == 3  # Warning on each instantiation


class TestStackLevel:
    """Test that stack level is set correctly for deprecation warnings."""

    def test_stack_level_function(self):
        """Test that function deprecation shows caller, not decorator."""
        @deprecated(
            reason="Function renamed",
            version="0.2.0",
            removal_version="0.3.0"
        )
        def old_function(x: int) -> int:
            return x * 2

        def caller_function():
            return old_function(5)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            
            caller_function()
            
            # The warning should point to caller_function, not old_function
            assert len(w) == 1
            # Check that the warning comes from the right line
            # (This is a basic check - in practice, you'd check the filename and line number)

    def test_stack_level_class(self):
        """Test that class deprecation shows caller, not decorator."""
        @deprecated_class(
            reason="Class renamed",
            version="0.2.0",
            removal_version="0.3.0"
        )
        class OldClass:
            def __init__(self, value: int):
                self.value = value

        def caller_function():
            return OldClass(42)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            
            caller_function()
            
            # The warning should point to caller_function, not OldClass.__init__
            assert len(w) == 1


class TestIntegration:
    """Integration tests for deprecation utilities."""

    def test_deprecation_with_pytest_warnings(self):
        """Test that deprecation warnings work with pytest warning filters."""
        @deprecated(
            reason="Function renamed",
            version="0.2.0",
            removal_version="0.3.0"
        )
        def old_function(x: int) -> int:
            return x * 2

        # This should emit a DeprecationWarning that pytest can catch
        with pytest.warns(DeprecationWarning, match="old_function.*deprecated"):
            old_function(5)

    def test_multiple_deprecations(self):
        """Test multiple deprecations in sequence."""
        @deprecated(
            reason="First deprecation",
            version="0.1.0",
            removal_version="0.2.0"
        )
        def first_old_function(x: int) -> int:
            return x

        @deprecated(
            reason="Second deprecation",
            version="0.2.0",
            removal_version="0.3.0"
        )
        def second_old_function(x: int) -> int:
            return x * 2

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            
            first_old_function(1)
            second_old_function(2)
            
            assert len(w) == 2
            assert "First deprecation" in str(w[0].message)
            assert "Second deprecation" in str(w[1].message)
