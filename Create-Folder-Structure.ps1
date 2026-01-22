
# Define base folder path
$baseFolder = "C:\BaseFolder"

# Create base folder
New-Item -ItemType Directory -Path $baseFolder -Force | Out-Null

# Create subfolders a, b, c
$subFolders = @("a", "b", "c")
foreach ($folder in $subFolders) {
    New-Item -ItemType Directory -Path (Join-Path $baseFolder $folder) -Force | Out-Null
}

# Create subfolders d and e under a
$aSubFolders = @("d", "e")
foreach ($folder in $aSubFolders) {
    New-Item -ItemType Directory -Path (Join-Path $baseFolder "a\$folder") -Force | Out-Null
}

Write-Host "Folder structure created successfully."
