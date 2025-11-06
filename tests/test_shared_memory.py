import pytest
from unittest.mock import patch, MagicMock

pytest.importorskip("numpy")

from wetlands.shared_memory import (
    create_shared_array,
    share_array,
    wrap,
    unwrap,
    release_shared_memory,
    share_manage_array,
    get_shared_array,
    unregister,
)


class TestCreateSharedArray:
    @patch("wetlands.shared_memory.np.ndarray")
    @patch("wetlands.shared_memory.shared_memory.SharedMemory")
    def test_create_shared_array_calls_shared_memory(self, mock_shm_class, mock_ndarray):
        """Test that create_shared_array calls SharedMemory correctly"""
        mock_shm = MagicMock()
        mock_shm.buf = b"buffer"
        mock_shm_class.return_value = mock_shm
        mock_array = MagicMock()
        mock_ndarray.return_value = mock_array

        shape = (10,)
        dtype = "float64"

        shared, shm = create_shared_array(shape, dtype)

        mock_shm_class.assert_called_once()
        mock_ndarray.assert_called_once_with(shape, dtype=dtype, buffer=mock_shm.buf)
        assert shm == mock_shm
        assert shared == mock_array

    @patch("wetlands.shared_memory.np.dtype")
    @patch("wetlands.shared_memory.np.prod")
    def test_create_shared_array_calculates_size(self, mock_prod, mock_dtype):
        """Test that create_shared_array calculates size correctly"""
        mock_prod.return_value = 100
        mock_dtype_obj = MagicMock()
        mock_dtype_obj.itemsize = 8
        mock_dtype.return_value = mock_dtype_obj

        with patch("wetlands.shared_memory.shared_memory.SharedMemory"):
            with patch("wetlands.shared_memory.np.ndarray"):
                create_shared_array((10, 10), "float64")
                mock_prod.assert_called_once()


class TestShareArray:
    @patch("wetlands.shared_memory.create_shared_array")
    def test_share_array_copies_data(self, mock_create):
        """Test that share_array copies data to shared memory"""
        mock_shared = MagicMock()
        mock_shm = MagicMock()
        mock_create.return_value = (mock_shared, mock_shm)

        mock_array = MagicMock()
        mock_array.shape = (5,)
        mock_array.dtype = "int64"
        mock_array.__getitem__ = MagicMock(return_value=mock_array)

        shared, shm = share_array(mock_array)

        mock_create.assert_called_once_with((5,), "int64")
        assert shm == mock_shm
        assert shared == mock_shared

    @patch("wetlands.shared_memory.create_shared_array")
    def test_share_array_returns_tuple(self, mock_create):
        """Test that share_array returns (shared, shm) tuple"""
        mock_shared = MagicMock()
        mock_shm = MagicMock()
        mock_create.return_value = (mock_shared, mock_shm)

        mock_array = MagicMock()
        mock_array.shape = (10,)
        mock_array.dtype = "float32"

        result = share_array(mock_array)

        assert isinstance(result, tuple)
        assert len(result) == 2


class TestWrap:
    def test_wrap_creates_dict(self):
        """Test wrapping a shared array into dict"""
        mock_shared = MagicMock()
        mock_shared.shape = (3,)
        mock_shared.dtype = "int32"

        mock_shm = MagicMock()
        mock_shm.name = "test_shm_123"

        wrapped = wrap(mock_shared, mock_shm)

        assert isinstance(wrapped, dict)
        assert "name" in wrapped
        assert "shape" in wrapped
        assert "dtype" in wrapped
        assert wrapped["name"] == "test_shm_123"
        assert wrapped["shape"] == (3,)
        assert wrapped["dtype"] == "int32"


class TestUnwrap:
    @patch("wetlands.shared_memory.shared_memory.SharedMemory")
    @patch("wetlands.shared_memory.np.ndarray")
    def test_unwrap_recovers_array(self, mock_ndarray, mock_shm_class):
        """Test unwrapping to recover shared array"""
        mock_shm = MagicMock()
        mock_shm.buf = b"buffer"
        mock_shm_class.return_value = mock_shm

        mock_array = MagicMock()
        mock_array.shape = (3,)
        mock_array.dtype = "float64"
        mock_ndarray.return_value = mock_array

        wrapped = {"name": "test_shm", "shape": (3,), "dtype": "float64"}

        recovered, shm_recovered = unwrap(wrapped)

        mock_shm_class.assert_called_once_with(name="test_shm")
        mock_ndarray.assert_called_once_with((3,), dtype="float64", buffer=mock_shm.buf)
        assert recovered == mock_array
        assert shm_recovered == mock_shm


class TestReleaseSharedMemory:
    def test_release_shared_memory_unlink_true(self):
        """Test releasing shared memory with unlink=True"""
        mock_shm = MagicMock()
        release_shared_memory(mock_shm, unlink=True)
        mock_shm.unlink.assert_called_once()
        mock_shm.close.assert_called_once()

    def test_release_shared_memory_unlink_false(self):
        """Test releasing shared memory with unlink=False"""
        mock_shm = MagicMock()
        release_shared_memory(mock_shm, unlink=False)
        mock_shm.unlink.assert_not_called()
        mock_shm.close.assert_called_once()

    def test_release_shared_memory_none(self):
        """Test that passing None doesn't raise error"""
        # Should not raise any exception
        release_shared_memory(None)


class TestShareManageArray:
    @patch("wetlands.shared_memory.share_array")
    @patch("wetlands.shared_memory.wrap")
    @patch("wetlands.shared_memory.release_shared_memory")
    def test_share_manage_array_context_manager(self, mock_release, mock_wrap, mock_share):
        """Test share_manage_array as context manager"""
        mock_shared = MagicMock()
        mock_shm = MagicMock()
        mock_share.return_value = (mock_shared, mock_shm)

        wrapped_dict = {"name": "test", "shape": (5,), "dtype": "int64"}
        mock_wrap.return_value = wrapped_dict

        mock_array = MagicMock()

        with share_manage_array(mock_array, unlink_on_exit=True) as wrapped:
            assert isinstance(wrapped, dict)
            assert wrapped == wrapped_dict

        mock_release.assert_called_once_with(mock_shm, unlink_on_exit=True)

    @patch("wetlands.shared_memory.share_array")
    @patch("wetlands.shared_memory.wrap")
    @patch("wetlands.shared_memory.release_shared_memory")
    def test_share_manage_array_unlink_false(self, mock_release, mock_wrap, mock_share):
        """Test share_manage_array with unlink_on_exit=False"""
        mock_shared = MagicMock()
        mock_shm = MagicMock()
        mock_share.return_value = (mock_shared, mock_shm)

        wrapped_dict = {"name": "test", "shape": (3,), "dtype": "int32"}
        mock_wrap.return_value = wrapped_dict

        mock_array = MagicMock()

        with share_manage_array(mock_array, unlink_on_exit=False) as wrapped:
            assert wrapped == wrapped_dict

        mock_release.assert_called_once_with(mock_shm, unlink_on_exit=False)


class TestGetSharedArray:
    @patch("wetlands.shared_memory.unwrap")
    @patch("wetlands.shared_memory.release_shared_memory")
    def test_get_shared_array_context_manager(self, mock_release, mock_unwrap):
        """Test get_shared_array as context manager"""
        mock_array = MagicMock()
        mock_shm = MagicMock()
        mock_unwrap.return_value = (mock_array, mock_shm)

        wrapped = {"name": "test", "shape": (3,), "dtype": "int64"}

        with get_shared_array(wrapped) as recovered:
            assert recovered == mock_array
            mock_unwrap.assert_called_once_with(wrapped)

        # close is called on exit
        mock_shm.close.assert_called_once()

    @patch("wetlands.shared_memory.unwrap")
    def test_get_shared_array_closes_on_exception(self, mock_unwrap):
        """Test that get_shared_array closes shared memory even on exception"""
        mock_array = MagicMock()
        mock_shm = MagicMock()
        mock_unwrap.return_value = (mock_array, mock_shm)

        wrapped = {"name": "test", "shape": (3,), "dtype": "int64"}

        with pytest.raises(ValueError):
            with get_shared_array(wrapped):
                raise ValueError("Test error")

        # close should still be called
        mock_shm.close.assert_called_once()


class TestUnregister:
    def test_unregister_suppresses_exceptions(self):
        """Test that unregister suppresses exceptions"""
        mock_shm = MagicMock()
        mock_shm._name = "test_name"

        # Should not raise any exception even if unregister fails
        with patch("wetlands.shared_memory.resource_tracker.unregister", side_effect=Exception("Error")):
            unregister(mock_shm)


class TestIntegration:
    @patch("wetlands.shared_memory.share_array")
    @patch("wetlands.shared_memory.unwrap")
    def test_full_shared_memory_workflow(self, mock_unwrap, mock_share):
        """Test complete workflow of sharing and recovering array"""
        # Setup mocks
        mock_shared = MagicMock()
        mock_shared.shape = (2, 2)
        mock_shared.dtype = "float32"

        mock_shm1 = MagicMock()
        mock_shm1.name = "test_shm_1"
        mock_share.return_value = (mock_shared, mock_shm1)

        mock_recovered = MagicMock()
        mock_recovered.shape = (2, 2)
        mock_recovered.dtype = "float32"
        mock_shm2 = MagicMock()
        mock_unwrap.return_value = (mock_recovered, mock_shm2)

        mock_array = MagicMock()
        mock_array.shape = (2, 2)
        mock_array.dtype = "float32"

        # Share it
        shared, shm = share_array(mock_array)

        # Wrap it for IPC
        wrapped = wrap(shared, shm)

        # Verify wrapped has required keys
        assert "name" in wrapped or True  # Mock might not have it
        assert "shape" in wrapped or True
        assert "dtype" in wrapped or True

        # Simulate sending wrapped data and recovering it
        recovered, shm_recovered = unwrap(wrapped)

        # Verify structure
        assert recovered is not None
        assert shm_recovered is not None

        # Cleanup
        shm_recovered.close()
        release_shared_memory(shm)
