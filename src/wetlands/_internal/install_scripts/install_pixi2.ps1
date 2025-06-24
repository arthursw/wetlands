<#
.SYNOPSIS
    Pixi install script.
.DESCRIPTION
    This script is used to install Pixi on Windows from the command line.
.PARAMETER PixiVersion
    Specifies the version of Pixi to install.
    The default value is 'latest'. You can also specify it by setting the
    environment variable 'PIXI_VERSION'.
.PARAMETER PixiHome
    Specifies Pixi's home directory.
    The default value is '$Env:USERPROFILE\.pixi'. You can also specify it by
    setting the environment variable 'PIXI_HOME'.
.PARAMETER NoPathUpdate
    If specified, the script will not update the PATH environment variable.
.PARAMETER PixiRepourl
    Specifies Pixi's repo url.
    The default value is 'https://github.com/prefix-dev/pixi'. You can also specify it by
    setting the environment variable 'PIXI_REPOURL'.
.PARAMETER ProxyString
    Specifies the user proxy settings, in the conda proxy_servers format (https://www.anaconda.com/docs/tools/working-with-conda/reference/proxy#updating-the-condarc-file).
.LINK
    https://pixi.sh
.LINK
    https://github.com/prefix-dev/pixi
.NOTES
    Version: v0.48.2
#>
param (
    [string] $PixiVersion = 'latest',
    [string] $PixiHome = "$Env:USERPROFILE\.pixi",
    [switch] $NoPathUpdate,
    [string] $PixiRepourl = 'https://github.com/prefix-dev/pixi',
    [string] $ProxyString = ''
)

# --- Dot-source the get_proxy.ps1 script to make functions available ---
. "$PSScriptRoot\utils.ps1"

$ProxyArgs = GetProxyArgs -ProxyString $ProxyString

Set-StrictMode -Version Latest

function Get-TargetTriple() {
  try {
    # NOTE: this might return X64 on ARM64 Windows, which is OK since emulation is available.
    # It works correctly starting in PowerShell Core 7.3 and Windows PowerShell in Win 11 22H2.
    # Ideally this would just be
    #   [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture
    # but that gets a type from the wrong assembly on Windows PowerShell (i.e. not Core)
    $a = [System.Reflection.Assembly]::LoadWithPartialName("System.Runtime.InteropServices.RuntimeInformation")
    $t = $a.GetType("System.Runtime.InteropServices.RuntimeInformation")
    $p = $t.GetProperty("OSArchitecture")
    # Possible OSArchitecture Values: https://learn.microsoft.com/dotnet/api/system.runtime.interopservices.architecture
    # Rust supported platforms: https://doc.rust-lang.org/stable/rustc/platform-support.html
    switch ($p.GetValue($null).ToString())
    {
      "X86" { return "i686-pc-windows-msvc" }
      "X64" { return "x86_64-pc-windows-msvc" }
      "Arm" { return "thumbv7a-pc-windows-msvc" }
      "Arm64" { return "aarch64-pc-windows-msvc" }
    }
  } catch {
    # The above was added in .NET 4.7.1, so Windows PowerShell in versions of Windows
    # prior to Windows 10 v1709 may not have this API.
    Write-Verbose "Get-TargetTriple: Exception when trying to determine OS architecture."
    Write-Verbose $_
  }

  # This is available in .NET 4.0. We already checked for PS 5, which requires .NET 4.5.
  Write-Verbose("Get-TargetTriple: falling back to Is64BitOperatingSystem.")
  if ([System.Environment]::Is64BitOperatingSystem) {
    return "x86_64-pc-windows-msvc"
  } else {
    return "i686-pc-windows-msvc"
  }
}

if ($Env:PIXI_VERSION) {
    $PixiVersion = $Env:PIXI_VERSION
}

if ($Env:PIXI_HOME) {
    $PixiHome = $Env:PIXI_HOME
}

if ($Env:PIXI_NO_PATH_UPDATE) {
    $NoPathUpdate = $true
}

if ($Env:PIXI_REPOURL) {
    $PixiRepourl = $Env:PIXI_REPOURL -replace '/$', ''
}

# Repository name
$ARCH = Get-TargetTriple

if (-not @("x86_64-pc-windows-msvc", "aarch64-pc-windows-msvc") -contains $ARCH) {
    throw "ERROR: could not find binaries for this platform ($ARCH)."
}

$BINARY = "pixi-$ARCH"

if ($PixiVersion -eq 'latest') {
    $DOWNLOAD_URL = "$PixiRepourl/releases/latest/download/$BINARY.zip"
} else {
    # Check if version is incorrectly specified without prefix 'v', and prepend 'v' in this case
    $PixiVersion = "v" + ($PixiVersion -replace '^v', '')
    $DOWNLOAD_URL = "$PixiRepourl/releases/download/$PixiVersion/$BINARY.zip"
}

$BinDir = Join-Path $PixiHome 'bin'

Write-Host "This script will automatically download and install Pixi ($PixiVersion) for you."
Write-Host "Getting it from this url: $DOWNLOAD_URL"
Write-Host "The binary will be installed into '$BinDir'"

$TEMP_FILE = [System.IO.Path]::GetTempFileName()

try {
    Invoke-WebRequest @ProxyArgs -Uri $DOWNLOAD_URL -OutFile $TEMP_FILE

    # Create the install dir if it doesn't exist
    if (!(Test-Path -Path $BinDir)) {
        New-Item -ItemType Directory -Path $BinDir | Out-Null
    }

    $ZIP_FILE = $TEMP_FILE + ".zip"
    Rename-Item -Path $TEMP_FILE -NewName $ZIP_FILE

    # Verify checksum
    Verify-FileChecksum -FilePath $ZIP_FILE -ChecksumFilePath "$PSScriptRoot\checksums\$BINARY.zip.sha256"

    # Create bin dir if needed
    if (!(Test-Path -Path $BinDir)) {
        New-Item -ItemType Directory -Path $BinDir | Out-Null
    }

    # Extract pixi from the downloaded zip file
    Expand-Archive -Path $ZIP_FILE -DestinationPath $BinDir -Force
    

} catch {
    Write-Host "Error: '$DOWNLOAD_URL' is not available or failed to download"
    exit 1
} finally {
    Remove-Item -Path $ZIP_FILE
}
