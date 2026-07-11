[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$BackupPath,
    [Parameter(Mandatory = $true)]
    [string]$TargetDatabase,
    [switch]$ConfirmRestore,
    [switch]$AllowPrimaryDatabase,
    [switch]$RestoreFiles
)

$ErrorActionPreference = "Stop"
$RepositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$BackupRoot = (Resolve-Path -LiteralPath $BackupPath).Path
if (-not $ConfirmRestore) {
    throw "復元には-ConfirmRestoreが必要です。"
}
if ($TargetDatabase -notmatch "^[A-Za-z0-9_]+$") {
    throw "TargetDatabaseは英数字とunderscoreだけを使用してください。"
}

function Invoke-DockerChecked {
    param([string[]]$Arguments)

    & docker @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "docker command failed: docker $($Arguments -join ' ')"
    }
}

$ManifestPath = Join-Path $BackupRoot "manifest.json"
$ManifestHashPath = Join-Path $BackupRoot "manifest.sha256"
$DatabaseDump = Join-Path $BackupRoot "database.dump"
foreach ($RequiredPath in @($ManifestPath, $ManifestHashPath, $DatabaseDump)) {
    if (-not (Test-Path -LiteralPath $RequiredPath -PathType Leaf)) {
        throw "必須バックアップファイルがありません: $RequiredPath"
    }
}

$ExpectedManifestHash = (Get-Content -LiteralPath $ManifestHashPath -Raw).Trim().ToLowerInvariant()
$ActualManifestHash = (Get-FileHash -LiteralPath $ManifestPath -Algorithm SHA256).Hash.ToLowerInvariant()
if ($ExpectedManifestHash -ne $ActualManifestHash) {
    throw "manifest.jsonのSHA-256が一致しません。"
}
$Manifest = Get-Content -LiteralPath $ManifestPath -Raw | ConvertFrom-Json
foreach ($File in $Manifest.files) {
    $FullPath = [IO.Path]::GetFullPath((Join-Path $BackupRoot $File.path))
    if (-not $FullPath.StartsWith($BackupRoot, [StringComparison]::OrdinalIgnoreCase)) {
        throw "Manifestにバックアップ外のパスがあります。"
    }
    if (-not (Test-Path -LiteralPath $FullPath -PathType Leaf)) {
        throw "Manifest記載ファイルがありません: $($File.path)"
    }
    $ActualHash = (Get-FileHash -LiteralPath $FullPath -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($ActualHash -ne $File.sha256) {
        throw "SHA-256不一致: $($File.path)"
    }
}

$PostgresContainer = (& docker ps --filter "label=com.docker.compose.service=postgres" --format "{{.ID}}" | Select-Object -First 1)
if (-not $PostgresContainer) {
    throw "Keiba AI StudioのPostgreSQLコンテナが起動していません。"
}
$PrimaryDatabase = (& docker exec $PostgresContainer printenv POSTGRES_DB).Trim()
$DatabaseUser = (& docker exec $PostgresContainer printenv POSTGRES_USER).Trim()
if ($TargetDatabase -eq $PrimaryDatabase -and -not $AllowPrimaryDatabase) {
    throw "稼働DBの復元には-AllowPrimaryDatabaseが必要です。事前にAPI/Worker/Beatを停止してください。"
}
if ($RestoreFiles -and $TargetDatabase -ne $PrimaryDatabase) {
    throw "ファイル復元は稼働DBと同じ復元点に限ります。"
}

$ContainerDump = "/tmp/restore_$([guid]::NewGuid().ToString('N')).dump"
try {
    Invoke-DockerChecked -Arguments @("cp", $DatabaseDump, "${PostgresContainer}:${ContainerDump}")
    Invoke-DockerChecked -Arguments @("exec", $PostgresContainer, "psql", "-U", $DatabaseUser, "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-c", "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '$TargetDatabase' AND pid <> pg_backend_pid();")
    Invoke-DockerChecked -Arguments @("exec", $PostgresContainer, "dropdb", "-U", $DatabaseUser, "--if-exists", $TargetDatabase)
    Invoke-DockerChecked -Arguments @("exec", $PostgresContainer, "createdb", "-U", $DatabaseUser, $TargetDatabase)
    Invoke-DockerChecked -Arguments @("exec", $PostgresContainer, "pg_restore", "-U", $DatabaseUser, "-d", $TargetDatabase, "--exit-on-error", $ContainerDump)

    $RestoredRevision = (& docker exec $PostgresContainer psql -U $DatabaseUser -d $TargetDatabase -Atc "SELECT version_num FROM alembic_version LIMIT 1").Trim()
    if ($RestoredRevision -ne $Manifest.alembic_revision) {
        throw "Alembic revisionが一致しません。expected=$($Manifest.alembic_revision), actual=$RestoredRevision"
    }

    if ($RestoreFiles) {
        $Mappings = @(
            @{ Backup = "raw\input"; Target = "data\input" },
            @{ Backup = "raw\odds"; Target = "data\ozzu_csv" },
            @{ Backup = "raw\legacy_output"; Target = "data\output" },
            @{ Backup = "master"; Target = "data\master" },
            @{ Backup = "exports"; Target = "data\exports" },
            @{ Backup = "logs"; Target = "data\logs" }
        )
        foreach ($Mapping in $Mappings) {
            $SourcePath = Join-Path $BackupRoot $Mapping.Backup
            if (Test-Path -LiteralPath $SourcePath) {
                $TargetPath = Join-Path $RepositoryRoot $Mapping.Target
                New-Item -ItemType Directory -Force -Path $TargetPath | Out-Null
                Copy-Item -Path (Join-Path $SourcePath "*") -Destination $TargetPath -Recurse -Force
            }
        }
    }

    Write-Output "restore verified: database=$TargetDatabase revision=$RestoredRevision"
}
finally {
    & docker exec $PostgresContainer rm -f $ContainerDump 2>$null | Out-Null
}
