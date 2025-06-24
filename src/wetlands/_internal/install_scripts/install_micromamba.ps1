<#
.SYNOPSIS
    Micromamba install script.
.DESCRIPTION
    This script is used to install Micromamba on Windows from the command line.
.PARAMETER MicromambaVersion
    Specifies the version of Micromamba to install.
    The default value is 'latest'. You can also specify it by setting the
    environment variable 'MICROMAMBA_VERSION'.
.PARAMETER InstallPath
    Specifies the micromamba installation path.
    The default value is '$Env:USERPROFILE\.micromamba'. You can also specify it by
    setting the environment variable 'MICROMAMBA_INSTALL_PATH'.
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
    [string] $MicromambaVersion = 'latest',
    [string] $InstallPath = "$Env:USERPROFILE\.micromamba",
    [string] $ProxyString = ''
)

if ($Env:MICROMAMBA_VERSION) {
    $MicromambaVersion = $Env:MICROMAMBA_VERSION
}

if ($Env:MICROMAMBA_INSTALL_PATH) {
    $InstallPath = $Env:MICROMAMBA_INSTALL_PATH
}

# --- Dot-source the get_proxy.ps1 script to make functions available ---
. "$PSScriptRoot\utils.ps1"

$ProxyArgs = get_proxy_args -ProxyString $ProxyString


Set-Location -Path "$InstallPath"

Write-Output "Installing Visual C++ Redistributable if necessary..."

$VC_REDIST_PATH = Join-Path -Path $Env:Temp -ChildPath "vc_redist.x64.exe"

Invoke-WebRequest @ProxyArgs -URI "https://aka.ms/vs/17/release/vc_redist.x64.exe" -OutFile $VC_REDIST_PATH

# Verify checksum
Verify-FileChecksum -FilePath $ZIP_FILE -ChecksumFilePath "$PSScriptRoot\checksums\vc_redist.x64.exe.sha256"

Start-Process $VC_REDIST_PATH -ArgumentList "/quiet /norestart" -Wait
Remove-Item $VC_REDIST_PATH

# check if VERSION env variable is set, otherwise use "latest"
$RELEASE_URL = if ("latest" -eq $MicromambaVersion) {
    "https://github.com/mamba-org/micromamba-releases/releases/latest/download/micromamba-win-64"
} else {
    "https://github.com/mamba-org/micromamba-releases/releases/download/$MicromambaVersion/micromamba-win-64"
}

Write-Output "Downloading micromamba from $RELEASE_URL"


Write-Output "Installing micromamba..."
Invoke-Webrequest @ProxyArgs -URI $RELEASE_URL -OutFile micromamba.exe

# Verify checksum
Verify-FileChecksum -FilePath "micromamba.exe" -ChecksumFilePath "$PSScriptRoot\checksums\micromamba-win-64.sha256"
