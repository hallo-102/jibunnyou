[CmdletBinding()]
param(
    [string]$Destination,
    [switch]$DatabaseOnly
)

$ErrorActionPreference = "Stop"
$RepositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
if (-not $Destination) {
    $Destination = Join-Path $RepositoryRoot ".backups"
}

function Invoke-DockerChecked {
    param([string[]]$Arguments)

    & docker @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "docker command failed: docker $($Arguments -join ' ')"
    }
}

$PostgresContainer = (& docker ps --filter "label=com.docker.compose.service=postgres" --format "{{.ID}}" | Select-Object -First 1)
if (-not $PostgresContainer) {
    throw "Keiba AI StudioのPostgreSQLコンテナが起動していません。"
}

$Database = (& docker exec $PostgresContainer printenv POSTGRES_DB).Trim()
$DatabaseUser = (& docker exec $PostgresContainer printenv POSTGRES_USER).Trim()
if (-not $Database -or -not $DatabaseUser) {
    throw "PostgreSQLコンテナからDB設定を取得できません。"
}

$Timestamp = [TimeZoneInfo]::ConvertTimeBySystemTimeZoneId([DateTimeOffset]::UtcNow, "Tokyo Standard Time").ToString("yyyyMMdd_HHmmss")
$BackupId = "backup_${Timestamp}_JST"
$BackupRoot = Join-Path (Resolve-Path -LiteralPath (New-Item -ItemType Directory -Force -Path $Destination)).Path $BackupId
New-Item -ItemType Directory -Path $BackupRoot | Out-Null

$ContainerDump = "/tmp/$BackupId.dump"
$DatabaseDump = Join-Path $BackupRoot "database.dump"

try {
    Invoke-DockerChecked -Arguments @("exec", $PostgresContainer, "pg_dump", "-U", $DatabaseUser, "-d", $Database, "--format=custom", "--file=$ContainerDump")
    Invoke-DockerChecked -Arguments @("cp", "${PostgresContainer}:${ContainerDump}", $DatabaseDump)

    if (-not $DatabaseOnly) {
        $Sources = @(
            @{ Name = "raw\input"; Path = "data\input" },
            @{ Name = "raw\odds"; Path = "data\ozzu_csv" },
            @{ Name = "raw\legacy_output"; Path = "data\output" },
            @{ Name = "master"; Path = "data\master" },
            @{ Name = "exports"; Path = "data\exports" },
            @{ Name = "logs"; Path = "data\logs" }
        )
        foreach ($Source in $Sources) {
            $SourcePath = Join-Path $RepositoryRoot $Source.Path
            if (Test-Path -LiteralPath $SourcePath) {
                $TargetPath = Join-Path $BackupRoot $Source.Name
                New-Item -ItemType Directory -Force -Path (Split-Path $TargetPath -Parent) | Out-Null
                Copy-Item -LiteralPath $SourcePath -Destination $TargetPath -Recurse -Force
            }
        }
    }

    $Revision = (& docker exec $PostgresContainer psql -U $DatabaseUser -d $Database -Atc "SELECT version_num FROM alembic_version LIMIT 1").Trim()
    $PostgresVersion = (& docker exec $PostgresContainer postgres --version).Trim()
    $Files = Get-ChildItem -LiteralPath $BackupRoot -File -Recurse | ForEach-Object {
        [ordered]@{
            path = [IO.Path]::GetRelativePath($BackupRoot, $_.FullName).Replace("\", "/")
            size = $_.Length
            sha256 = (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
        }
    }
    $Manifest = [ordered]@{
        backup_id = $BackupId
        completed_at = [DateTimeOffset]::Now.ToString("o")
        database = $Database
        alembic_revision = $Revision
        postgres_version = $PostgresVersion
        database_only = [bool]$DatabaseOnly
        required_secret_names = @("KEIBA_POSTGRES_PASSWORD", "KEIBA_DATABASE_URL", "OPENAI_API_KEY")
        files = @($Files)
    }
    $ManifestPath = Join-Path $BackupRoot "manifest.json"
    $Manifest | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $ManifestPath -Encoding utf8
    (Get-FileHash -LiteralPath $ManifestPath -Algorithm SHA256).Hash.ToLowerInvariant() |
        Set-Content -LiteralPath (Join-Path $BackupRoot "manifest.sha256") -Encoding ascii

    Write-Output $BackupRoot
}
finally {
    & docker exec $PostgresContainer rm -f $ContainerDump 2>$null | Out-Null
}
