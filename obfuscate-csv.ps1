# Paths
$inputFile  = "C:\input\data.csv"
$outputFile = "C:\output\data_obfuscated.csv"

# Function to generate random numeric string of same length
function Get-RandomNumericString {
    param (
        [int]$Length
    )

    if ($Length -le 0) { return "" }

    -join (1..$Length | ForEach-Object { Get-Random -Minimum 0 -Maximum 10 })
}

# Read all lines
$lines = Get-Content $inputFile

# Capture header
$header = $lines[0]

# Process data rows only
$obfuscatedLines = for ($i = 1; $i -lt $lines.Count; $i++) {

    $line = $lines[$i]

    # Parse CSV row (respects quotes)
    $row = $line | ConvertFrom-Csv -Header f1, f2, f3, f4

    # Obfuscate fields by original length
    $newFields = @(
        Get-RandomNumericString $row.f1.Length
        Get-RandomNumericString $row.f2.Length
        Get-RandomNumericString $row.f3.Length
        Get-RandomNumericString $row.f4.Length
    )

    # Rebuild CSV row
    '"' + ($newFields -join '","') + '"'
}

# Write header + obfuscated data
@($header) + $obfuscatedLines | Set-Content $outputFile

Write-Host "Obfuscation complete (header preserved): $outputFile"