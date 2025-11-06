import pytest
from unittest.mock import patch, MagicMock, mock_open
import urllib.error

from wetlands._internal.install import (
    downloadFile,
    downloadAndVerify,
)


class TestDownloadFile:
    @patch("urllib.request.urlopen")
    @patch("urllib.request.install_opener")
    @patch("urllib.request.build_opener")
    def test_download_file_success(self, mock_build_opener, mock_install_opener, mock_urlopen, tmp_path):
        """Test successful file download"""
        dest_file = tmp_path / "downloaded.bin"
        mock_response = MagicMock()
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=None)
        mock_response.read.return_value = b"file content"
        mock_urlopen.return_value = mock_response

        with patch("builtins.open", mock_open()):
            with patch("shutil.copyfileobj"):
                downloadFile("http://example.com/file.bin", dest_file)
                mock_urlopen.assert_called_once()

    @patch("urllib.request.urlopen")
    @patch("urllib.request.install_opener")
    @patch("urllib.request.build_opener")
    def test_download_file_url_error(self, mock_build_opener, mock_install_opener, mock_urlopen, tmp_path):
        """Test download with URL error"""
        dest_file = tmp_path / "downloaded.bin"
        mock_urlopen.side_effect = urllib.error.URLError("Connection refused")

        with pytest.raises(RuntimeError, match="Failed to download"):
            downloadFile("http://example.com/file.bin", dest_file)

    @patch("urllib.request.urlopen")
    @patch("urllib.request.install_opener")
    @patch("urllib.request.build_opener")
    def test_download_file_creates_parent_directory(
        self, mock_build_opener, mock_install_opener, mock_urlopen, tmp_path
    ):
        """Test that parent directory is created"""
        dest_file = tmp_path / "subdir1" / "subdir2" / "file.bin"
        mock_response = MagicMock()
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=None)
        mock_urlopen.return_value = mock_response

        with patch("builtins.open", mock_open()):
            with patch("shutil.copyfileobj"):
                downloadFile("http://example.com/file.bin", dest_file)
                assert dest_file.parent.exists()


class TestDownloadAndVerify:
    @patch("wetlands._internal.install.downloadFile")
    @patch("wetlands._internal.install.verifyChecksum")
    def test_download_and_verify_success(self, mock_verify, mock_download, tmp_path):
        """Test successful download and verification"""
        dest_path = tmp_path / "file.bin"
        checksum_path = tmp_path / "checksum.txt"

        downloadAndVerify("http://example.com/file", dest_path, checksum_path, None)

        mock_download.assert_called_once()
        mock_verify.assert_called_once()

    @patch("wetlands._internal.install.downloadFile")
    @patch("wetlands._internal.install.verifyChecksum")
    def test_download_and_verify_download_failure(self, mock_verify, mock_download, tmp_path):
        """Test handling download failure"""
        dest_path = tmp_path / "file.bin"
        checksum_path = tmp_path / "checksum.txt"
        dest_path.write_bytes(b"partial")

        mock_download.side_effect = RuntimeError("Download failed")

        with pytest.raises(RuntimeError, match="Download failed"):
            downloadAndVerify("http://example.com/file", dest_path, checksum_path, None)

        # File should be cleaned up
        assert not dest_path.exists()

    @patch("wetlands._internal.install.downloadFile")
    @patch("wetlands._internal.install.verifyChecksum")
    def test_download_and_verify_checksum_failure(self, mock_verify, mock_download, tmp_path):
        """Test handling checksum verification failure"""
        dest_path = tmp_path / "file.bin"
        checksum_path = tmp_path / "checksum.txt"
        dest_path.write_bytes(b"content")

        mock_verify.side_effect = ValueError("Checksum mismatch")

        with pytest.raises(ValueError, match="Checksum mismatch"):
            downloadAndVerify("http://example.com/file", dest_path, checksum_path, None)

        # File should be cleaned up
        assert not dest_path.exists()
