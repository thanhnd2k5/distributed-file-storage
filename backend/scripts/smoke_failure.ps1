# Explicit M4 P6: repair with node-2 offline, restart pending DELETE, API-only cleanup.
param([string] $LegacyManifest)
$ErrorActionPreference = 'Stop'
$repo = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$composeArgs = @('compose', '--env-file', (Join-Path $repo 'deploy/.env'),
    '-f', (Join-Path $repo 'deploy/compose.local.yml'))
$fixture = 'm4-smoke-' + [Guid]::NewGuid().ToString('N')
$containerManifest = '/tmp/' + $fixture + '.json'
$hostDirectory = Join-Path $repo '.runtime'
New-Item -ItemType Directory -Force -Path $hostDirectory | Out-Null
$hostManifest = Join-Path $hostDirectory ($fixture + '.json')
$containerLegacy = '/tmp/' + $fixture + '-legacy.json'
if ($LegacyManifest) {
    $LegacyManifest = (Resolve-Path -LiteralPath $LegacyManifest).Path
    if ((Split-Path $LegacyManifest -Parent) -ne $hostDirectory -or
        (Split-Path $LegacyManifest -Leaf) -notmatch '^m3-smoke-[0-9a-f]{32}\.json$') {
        throw 'Legacy fixture requires an owned M3 manifest under this repository .runtime.'
    }
}

function Invoke-Compose {
    param([string[]] $Arguments)
    & docker @composeArgs @Arguments
    if ($LASTEXITCODE -ne 0) { throw "Compose failed: $($Arguments -join ' ')" }
}
function Invoke-Smoke {
    param([string] $Mode, [string[]] $Extra = @())
    Invoke-Compose -Arguments (@('exec', '-T', 'metadata', 'python', '-O',
        'scripts/smoke_failure.py', $Mode, '--manifest', $containerManifest) + $Extra)
    Invoke-Compose -Arguments @('cp', "metadata:$containerManifest", $hostManifest)
}
function Invoke-Baseline {
    Invoke-Compose -Arguments @('exec', '-T', 'metadata', 'python', '-O',
        'scripts/smoke_metadata.py', 'baseline')
}
function Wait-Down {
    Invoke-Compose -Arguments @('exec', '-T', 'metadata', 'python', '-O',
        'scripts/smoke_metadata.py', 'down', '--node', 'node-2')
}

$platform = & docker info --format '{{.OSType}}'
if ($LASTEXITCODE -ne 0 -or $platform -ne 'linux') { throw 'Docker Linux is required.' }
$state = @(Invoke-Compose -Arguments @('ps', '--format', 'json') |
    ForEach-Object { $_ | ConvertFrom-Json })
foreach ($service in @('postgres', 'metadata', 'storage-node-1', 'storage-node-2', 'storage-node-3')) {
    $entry = @($state | Where-Object { $_.Service -eq $service })
    if ($entry.Count -ne 1 -or $entry[0].State -ne 'running' -or $entry[0].Health -ne 'healthy') {
        throw "Prerequisite $service must be healthy. Recovery: docker compose --env-file deploy/.env -f deploy/compose.local.yml up -d --wait"
    }
}
$volumes = @{}
foreach ($service in @('postgres', 'storage-node-1', 'storage-node-2', 'storage-node-3')) {
    $id = Invoke-Compose -Arguments @('ps', '-q', $service)
    $mounts = & docker inspect --format '{{json .Mounts}}' $id | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0) { throw "Cannot inspect $service mounts" }
    $destination = if ($service -eq 'postgres') { '/var/lib/postgresql/data' } else { '/data/chunks' }
    $mount = @($mounts | Where-Object { $_.Destination -eq $destination })
    if ($mount.Count -ne 1 -or $mount[0].Type -ne 'volume') { throw "Expected named volume for $service" }
    $volumes[$service] = $mount[0].Name
}
if (@($volumes.Values | Select-Object -Unique).Count -ne 4) { throw 'Named volumes must be distinct' }
$nodeStopped = $false
$createStarted = $false
try {
    Invoke-Baseline
    Write-Host "M4 P6 manifest: $hostManifest; preserved volumes: $($volumes.Values -join ', ')"
    $extra = @()
    if ($LegacyManifest) {
        Invoke-Compose -Arguments @('cp', $LegacyManifest, "metadata:$containerLegacy")
        $extra = @('--legacy-manifest', $containerLegacy)
    }
    $createStarted = $true
    Invoke-Smoke -Mode 'create' -Extra $extra
    $saved = Get-Content -Raw -LiteralPath $hostManifest | ConvertFrom-Json
    $saved | Add-Member -NotePropertyName 'volumes' -NotePropertyValue $volumes
    $saved | ConvertTo-Json -Depth 50 | Set-Content -LiteralPath $hostManifest -Encoding utf8
    Invoke-Compose -Arguments @('cp', $hostManifest, "metadata:$containerManifest")

    $nodeStopped = $true
    Invoke-Compose -Arguments @('stop', 'storage-node-2')
    Wait-Down
    Invoke-Smoke -Mode 'down-repair'
    Invoke-Compose -Arguments @('start', '--wait', 'storage-node-2')
    Invoke-Baseline
    $nodeStopped = $false
    Invoke-Smoke -Mode 'recovered'

    $nodeStopped = $true
    Invoke-Compose -Arguments @('stop', 'storage-node-2')
    Wait-Down
    Invoke-Smoke -Mode 'pending-delete'
    Invoke-Compose -Arguments @('restart', 'metadata')
    # Readiness and the stopped node's history are refreshed after our deliberate restart.
    Invoke-Smoke -Mode 'pending-verify'
    Wait-Down
    Invoke-Compose -Arguments @('start', '--wait', 'storage-node-2')
    Invoke-Baseline
    $nodeStopped = $false
    Invoke-Smoke -Mode 'finish'
    Invoke-Baseline
    Invoke-Smoke -Mode 'final'
    foreach ($service in $volumes.Keys) {
        $id = Invoke-Compose -Arguments @('ps', '-q', $service)
        $mounts = & docker inspect --format '{{json .Mounts}}' $id | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0 -or $volumes[$service] -notin @($mounts.Name)) {
            throw "Volume changed: $service"
        }
    }
    Write-Host "M4 P6 passed; all owned fixtures deleted via API, retained tombstones and manifest: $hostManifest"
}
finally {
    try {
        if ($nodeStopped) {
            Invoke-Compose -Arguments @('start', '--wait', 'storage-node-2')
            Invoke-Baseline
        }
    }
    finally {
        # Keep IDs even if an assertion failed; resume individual modes from this manifest.
        if ($createStarted) { Invoke-Compose -Arguments @('cp', "metadata:$containerManifest", $hostManifest) }
    }
}
