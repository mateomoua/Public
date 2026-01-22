
param (
    [Parameter(Mandatory)]
    [string]$ServerName,

    [Parameter(Mandatory)]
    [string]$DatabaseName,

    [string]$OutputFolder = "C:\SqlExport",

    [System.Management.Automation.PSCredential]$Credential,

    [string]$CredentialTarget = "SqlServerCredential"
)

if (-not (Test-Path $OutputFolder)) {
    New-Item -ItemType Directory -Path $OutputFolder | Out-Null
}

Add-Type -AssemblyName "Microsoft.SqlServer.Smo"
Add-Type -AssemblyName "Microsoft.SqlServer.ConnectionInfo"
Add-Type -AssemblyName "Microsoft.SqlServer.SmoExtended"

$Server = New-Object Microsoft.SqlServer.Management.Smo.Server $ServerName

if ($Credential) {
    $Server.ConnectionContext.LoginSecure = $false
    $Server.ConnectionContext.Login = $Credential.UserName
    $Server.ConnectionContext.SecurePassword = $Credential.Password
}
else {
    $Server.ConnectionContext.LoginSecure = $true
    try {
        Import-Module CredentialManager -ErrorAction Stop
        $StoredCred = Get-StoredCredential -Target $CredentialTarget
        if ($StoredCred) {
            $Server.ConnectionContext.LoginSecure = $false
            $Server.ConnectionContext.Login = $StoredCred.UserName
            $Server.ConnectionContext.SecurePassword = $StoredCred.Password
        }
    }
    catch { }
}

$Database = $Server.Databases[$DatabaseName]
if (-not $Database) {
    throw "Database '$DatabaseName' not found on server '$ServerName'"
}

$TableData = foreach ($Table in $Database.Tables | Where-Object { -not $_.IsSystemObject }) {
    foreach ($Column in $Table.Columns) {

        $IsPK = $false
        foreach ($Index in $Table.Indexes | Where-Object { $_.IndexKeyType -eq "DriPrimaryKey" }) {
            if ($Index.IndexedColumns.Name -contains $Column.Name) {
                $IsPK = $true
            }
        }

        [PSCustomObject]@{
            Schema        = $Table.Schema
            TableName     = $Table.Name
            ColumnName    = $Column.Name
            DataType      = $Column.DataType.Name
            MaxLength     = $Column.DataType.MaximumLength
            Precision     = $Column.DataType.NumericPrecision
            Scale         = $Column.DataType.NumericScale
            IsNullable    = $Column.Nullable
            IsIdentity    = $Column.Identity
            IsPrimaryKey  = $IsPK
        }
    }
}

$TableData | Export-Csv -Path (Join-Path $OutputFolder "Tables.csv") -NoTypeInformation -Encoding UTF8

$SprocData = foreach ($Sproc in $Database.StoredProcedures | Where-Object { -not $_.IsSystemObject }) {
    [PSCustomObject]@{
        Schema        = $Sproc.Schema
        ProcedureName = $Sproc.Name
        Definition    = ($Sproc.TextHeader + $Sproc.TextBody).Trim()
    }
}

$SprocData | Export-Csv -Path (Join-Path $OutputFolder "StoredProcedures.csv") -NoTypeInformation -Encoding UTF8

Write-Host "Export complete."
