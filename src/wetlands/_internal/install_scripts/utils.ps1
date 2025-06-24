function GetProxyArgs {
    <#
    .SYNOPSIS
    Parses a proxy string and returns an array of arguments suitable for splatting
    with commands that support -Proxy and -ProxyCredential parameters.

    .DESCRIPTION
    This function takes a proxy string (e.g., "http://user:pass@proxy.example.com:8080" or
    "http://proxy.example.com:8080"). It extracts the username, password, and URI if credentials
    are present. It then constructs a PowerShell credential object if needed and returns
    an array of parameters (-Proxy, -ProxyCredential) for use with splatting.

    .PARAMETER ProxyString
    The full proxy URI string. Examples:
    - "http://myuser:mypassword@myproxy.example.com:8080"
    - "http://myproxy.example.com:8080"
    - "" (empty string)

    .OUTPUTS
    An array (`$ProxyArgs`) containing parameter names and their corresponding values
    (e.g., "-Proxy", "http://...", "-ProxyCredential", $credentialObject)
    which can be used for splatting.

    .NOTES
    This function uses `ConvertTo-SecureString -AsPlainText -Force` for demonstration purposes.
    In a production environment, avoid hardcoding or directly exposing passwords.
    Consider using more secure methods like `Get-Credential`, `SecretManagement` module,
    or `Import-CliXml` for handling sensitive credentials.

    .EXAMPLE
    # Assume $myCondaProxyString contains the proxy info
    # e.g., $myCondaProxyString = "http://myuser:mypassword@myproxy.example.com:8080"

    # Dot-source the script to make the function available
    . "$PSScriptRoot\get_proxy.ps1"

    # Call the function to get the proxy arguments
    $myProxyArgs = get_proxy_args -ProxyString $myCondaProxyString

    # Use the arguments with a command that supports splatting, e.g., Invoke-WebRequest
    # Invoke-WebRequest -Uri "http://example.com" @myProxyArgs
    #>
    [CmdletBinding()]
    Param(
        [Parameter(Mandatory=$true)]
        [string]$ProxyString
    )

    # Initialize ProxyArgs as an empty array for splatting
    $ProxyArgs = @()

    # Check if $ProxyString is not null or empty
    if (![string]::IsNullOrEmpty($ProxyString)) {
        # Attempt to match the pattern with username and password
        # Regex explanation:
        # ^[a-zA-Z]+://  -> Matches a protocol like http:// or https:// at the beginning
        # (.*?)          -> Group 1: Non-greedy match for username
        # :              -> Matches the colon separating username and password
        # (.*?)          -> Group 2: Non-greedy match for password
        # @              -> Matches the @ symbol
        # (.+)           -> Group 3: Matches the rest of the string (host:port)
        if ($ProxyString -match "^[a-zA-Z]+://(.*?):(.*?)@(.+)") {
            # Regex matched: contains credentials
            $proxyUsername = $Matches[1]
            $proxyPassword = $Matches[2]
            # $Matches[3] will contain the host:port, e.g., "myproxy.example.com:8080"
            $proxyUri = ($ProxyString -split '@', 2)[1] # Extract the URI part after '@'

            # Convert the plain text password to a secure string.
            # TODO: Use a secure method like using Get-Credential interactively, SecretManagement, or Import-CliXml.
            $securePassword = ConvertTo-SecureString $proxyPassword -AsPlainText -Force
            
            # Create a PSCredential object
            $proxyCredentials = New-Object System.Management.Automation.PSCredential($proxyUsername, $securePassword)
            
            # Add -Proxy and -ProxyCredential to the arguments array
            # Pass the original $ProxyString as it includes the protocol (e.g., http://)
            $ProxyArgs += "-Proxy", $ProxyString
            $ProxyArgs += "-ProxyCredential", $proxyCredentials

        } else {
            # Regex did NOT match: $ProxyString is not empty but does not contain username:password@
            # This means it's likely just a proxy URI (e.g., "http://myproxy.example.com:8080")
            
            # Add only -Proxy to the arguments array
            $ProxyArgs += "-Proxy", $ProxyString
        }
    }

    # Return the constructed ProxyArgs array
    return $ProxyArgs
}

function Verify-FileChecksum {
    <#
    .SYNOPSIS
        Verifies the SHA256 checksum of a file against an expected value read from a checksum file.

    .DESCRIPTION
        This function calculates the SHA256 hash of a specified file and compares it
        to an expected SHA256 checksum string. The expected checksum is read from
        the first word of the content of a provided .sha256 checksum file.
        It outputs whether the checksum verification passed or failed. If it fails,
        it provides details about the expected and actual checksums and throws an error,
        stopping further execution in the calling script unless caught.

    .PARAMETER FilePath
        The full path to the file whose checksum is to be verified.
        This parameter is mandatory.

    .PARAMETER ChecksumFilePath
        The full path to the .sha256 file containing the expected SHA256 checksum.
        The function will read the first word (the hash) from this file.
        This parameter is mandatory.

    .EXAMPLE
        # Example 1: Verify a file with a correct checksum using a .sha256 file
        # Assuming 'C:\temp\my_file.zip' exists and 'C:\temp\my_file.zip.sha256' contains its hash
        # e.g., 'C:\temp\my_file.zip.sha256' might contain: "ABC123DEF456...  my_file.zip"
        Try {
            Verify-FileChecksum -FilePath "C:\temp\my_file.zip" -ChecksumFilePath "C:\temp\my_file.zip.sha256"
            Write-Host "File verification successful."
        } Catch {
            Write-Error "File verification failed: $($_.Exception.Message)"
        }

    .NOTES
        Requires PowerShell 5.1 or later for Get-FileHash cmdlet.
    #>
    [CmdletBinding()]
    Param(
        [Parameter(Mandatory=$true)]
        [string]$FilePath,

        [Parameter(Mandatory=$true)]
        [string]$ChecksumFilePath
    )

    # --- Input Validation ---
    # Check if the file to be verified exists
    if (-not (Test-Path -Path $FilePath -PathType Leaf)) {
        $errorMessage = "File not found at path: '$FilePath'. Cannot verify checksum."
        Write-Error $errorMessage
        throw $errorMessage # Throw an error to stop execution
    }

    # Check if the checksum file exists
    if (-not (Test-Path -Path $ChecksumFilePath -PathType Leaf)) {
        $errorMessage = "Checksum file not found at path: '$ChecksumFilePath'. Cannot verify checksum."
        Write-Error $errorMessage
        throw $errorMessage # Throw an error to stop execution
    }

    Write-Host "Verifying checksum for '$FilePath' using '$ChecksumFilePath'..."

    # --- Read Expected Checksum from File ---
    # Get the content of the checksum file.
    # We expect the format to be "CHECKSUM_VALUE  filename" or just "CHECKSUM_VALUE".
    # We split by whitespace and take the first element, which should be the checksum.
    Try {
        $ExpectedChecksum = Get-Content -Path $ChecksumFilePath | ForEach-Object { ($_ -split '\s+')[0] }
        if ([string]::IsNullOrWhiteSpace($ExpectedChecksum)) {
            $errorMessage = "Could not read a valid checksum from '$ChecksumFilePath'. File appears empty or incorrectly formatted."
            Write-Error $errorMessage
            throw $errorMessage # Throw an error to stop execution
        }
    } Catch {
        $errorMessage = "Failed to read content from checksum file '$ChecksumFilePath'. Error: $($_.Exception.Message)"
        Write-Error $errorMessage
        throw $errorMessage # Rethrow the caught error to stop execution
    }

    # --- Calculate Actual Checksum ---
    # Get the SHA256 hash of the specified file.
    # We expand the 'Hash' property to get just the string value.
    $ActualChecksum = Get-FileHash -Path $FilePath -Algorithm SHA256 | Select-Object -ExpandProperty Hash

    # --- Compare Checksums ---
    # Perform a case-insensitive comparison of the expected and actual checksums.
    # Checksums are typically uppercase hexadecimal strings.
    if ($ExpectedChecksum -ne $ActualChecksum) {
        # If checksums do not match, write an error message with details.
        $errorMessage = "Checksum verification failed for '$FilePath'.`nExpected: $ExpectedChecksum`nActual:   $ActualChecksum"
        Write-Error $errorMessage
        # Throw a custom error to allow calling scripts to handle the failure specifically.
        throw "ChecksumMismatch: $errorMessage"
    } else {
        # If checksums match, write a success message.
        Write-Host "Checksum verification passed for '$FilePath'."
    }
}
