import pytest
from unittest.mock import patch, MagicMock, mock_open
import urllib.error
import hashlib

from wetlands._internal.install import (
    downloadFile,
    downloadAndVerify,
    verify_checksum,
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
    @patch("wetlands._internal.install.verify_checksum")
    def test_download_and_verify_success(self, mock_verify, mock_download, tmp_path):
        """Test successful download and verification"""
        dest_path = tmp_path / "file.bin"
        expected_checksum = "a" * 64

        downloadAndVerify("http://example.com/file", dest_path, expected_checksum, None)

        mock_download.assert_called_once()
        mock_verify.assert_called_once_with(dest_path, expected_checksum)

    @patch("wetlands._internal.install.downloadFile")
    @patch("wetlands._internal.install.verify_checksum")
    def test_download_and_verify_download_failure(self, mock_verify, mock_download, tmp_path):
        """Test handling download failure"""
        dest_path = tmp_path / "file.bin"
        dest_path.write_bytes(b"partial")

        mock_download.side_effect = RuntimeError("Download failed")

        with pytest.raises(RuntimeError, match="Download failed"):
            downloadAndVerify("http://example.com/file", dest_path, "a" * 64, None)

        # File should be cleaned up
        assert not dest_path.exists()

    @patch("wetlands._internal.install.downloadFile")
    @patch("wetlands._internal.install.verify_checksum")
    def test_download_and_verify_checksum_failure(self, mock_verify, mock_download, tmp_path):
        """Test handling checksum verification failure"""
        dest_path = tmp_path / "file.bin"
        dest_path.write_bytes(b"content")

        mock_verify.side_effect = ValueError("Checksum mismatch")

        with pytest.raises(ValueError, match="Checksum mismatch"):
            downloadAndVerify("http://example.com/file", dest_path, "a" * 64, None)

        # File should be cleaned up
        assert not dest_path.exists()


def test_verify_checksum_accepts_matching_download(tmp_path):
    artifact = tmp_path / "artifact.bin"
    artifact.write_bytes(b"verified content")

    verify_checksum(artifact, hashlib.sha256(b"verified content").hexdigest())


def test_verify_checksum_reports_artifact_expected_and_calculated_digest(tmp_path):
    artifact = tmp_path / "artifact.bin"
    artifact.write_bytes(b"unexpected content")
    expected = "a" * 64
    calculated = hashlib.sha256(b"unexpected content").hexdigest()

    with pytest.raises(ValueError) as error:
        verify_checksum(artifact, expected)

    message = str(error.value)
    assert artifact.name in message
    assert expected in message
    assert calculated in message
