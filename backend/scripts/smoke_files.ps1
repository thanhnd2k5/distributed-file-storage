# Explicit P6 local lifecycle: one RF=2 file, stop node-2, restart Metadata.
param([string] $Manifest)
$ErrorActionPreference = 'Stop'
$repo = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$composeArgs = @('compose', '--env-file', (Join-Path $repo 'deploy/.env'),
    '-f', (Join-Path $repo 'deploy/compose.local.yml'))
$fixture = 'm3-smoke-' + [Guid]::NewGuid().ToString('N')
$containerManifest = '/tmp/' + $fixture + '.json'
$hostDirectory = Join-Path $repo '.runtime'
New-Item -ItemType Directory -Force -Path $hostDirectory | Out-Null
$hostManifest = Join-Path $hostDirectory ($fixture + '.json')
if ($Manifest) {
    $hostManifest = (Resolve-Path -LiteralPath $Manifest).Path
    if ((Split-Path $hostManifest -Parent) -ne $hostDirectory -or
        (Split-Path $hostManifest -Leaf) -notmatch '^m3-smoke-[0-9a-f]{32}\.json$') {
        throw 'Resume requires an existing owned manifest under the repository .runtime directory.'
    }
    $containerManifest = '/tmp/' + (Split-Path $hostManifest -Leaf)
}

function Invoke-Compose {
    param([string[]] $Arguments)
    & docker @composeArgs @Arguments
    if ($LASTEXITCODE -ne 0) { throw "Compose failed: $($Arguments -join ' ')" }
}
function Invoke-Files {
    param([string[]] $Arguments)
    Invoke-Compose -Arguments (@('exec', '-T', 'metadata', 'python', '-O', 'scripts/smoke_files.py') +
        $Arguments + @('--manifest', $containerManifest))
}
function Invoke-Baseline {
    param([string[]] $Arguments = @())
    Invoke-Compose -Arguments (@('exec', '-T', 'metadata', 'python', '-O', 'scripts/smoke_metadata.py', 'baseline') + $Arguments)
}

$platform = & docker info --format '{{.OSType}}'
if ($LASTEXITCODE -ne 0 -or $platform -ne 'linux') { throw 'Docker Linux is required.' }
$services = Invoke-Compose -Arguments @('ps', '--format', 'json')
$state = @($services | ForEach-Object { $_ | ConvertFrom-Json })
foreach ($service in @('postgres', 'metadata', 'storage-node-1', 'storage-node-2', 'storage-node-3')) {
    $entry = @($state | Where-Object { $_.Service -eq $service })
    if ($entry.Count -ne 1 -or $entry[0].State -ne 'running' -or $entry[0].Health -ne 'healthy') {
        throw "Prerequisite $service must be healthy. Recovery: docker compose --env-file deploy/.env -f deploy/compose.local.yml up -d --wait"
    }
}
$volumes = @{}
foreach ($service in @('storage-node-1', 'storage-node-2', 'storage-node-3')) {
    $id = Invoke-Compose -Arguments @('ps', '-q', $service)
    $mounts = & docker inspect --format '{{json .Mounts}}' $id | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0) { throw "Cannot inspect $service mounts" }
    $mount = @($mounts | Where-Object { $_.Destination -eq '/data/chunks' })
    if ($mount.Count -ne 1 -or $mount[0].Type -ne 'volume') { throw "Expected named volume for $service" }
    $volumes[$service] = $mount[0].Name
}
if (@($volumes.Values | Select-Object -Unique).Count -ne 3) { throw 'Storage volumes must be distinct' }
$nodeStopped = $false
$uploadStarted = $false
try {
    Invoke-Baseline
    Write-Host 'P6 API http://127.0.0.1:8000/api/v1: verify 10 MiB + 17 byte RF=2 fixture; stop storage-node-2, then restart Metadata.'
    Write-Host "Manifest: $hostManifest; distinct volumes: $($volumes.Values -join ', ')"
    $uploadStarted = $true
    if ($Manifest) {
        Invoke-Compose -Arguments @('cp', $hostManifest, "metadata:$containerManifest")
        Invoke-Files -Arguments @('verify')
    }
    else { Invoke-Files -Arguments @('create') }
    Invoke-Compose -Arguments @('cp', "metadata:$containerManifest", $hostManifest)
    $nodeStopped = $true
    Invoke-Compose -Arguments @('stop', 'storage-node-2')
    Invoke-Compose -Arguments @('exec', '-T', 'metadata', 'python', '-O', 'scripts/smoke_metadata.py', 'down', '--node', 'node-2')
    Invoke-Files -Arguments @('verify', '--down-node', 'node-2')
    Invoke-Compose -Arguments @('start', '--wait', 'storage-node-2')
    Invoke-Baseline
    $nodeStopped = $false
    Invoke-Files -Arguments @('verify')
    Invoke-Compose -Arguments @('restart', 'metadata')
    $after = [DateTime]::UtcNow.ToString('o')
    Invoke-Baseline -Arguments @('--after', $after)
    Invoke-Files -Arguments @('verify')
    foreach ($service in $volumes.Keys) {
        $id = Invoke-Compose -Arguments @('ps', '-q', $service)
        $mounts = & docker inspect --format '{{json .Mounts}}' $id | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0 -or $volumes[$service] -notin @($mounts.Name)) { throw "Volume changed: $service" }
    }
    Write-Host "P6 passed. Fixture remains for M4 DELETE; manifest: $hostManifest"
}
finally {
    try {
        if ($nodeStopped) {
            Invoke-Compose -Arguments @('start', '--wait', 'storage-node-2')
            Invoke-Baseline
        }
    }
    finally {
        # Preserve the owned file IDs even after a later smoke assertion fails.
        if ($uploadStarted) { Invoke-Compose -Arguments @('cp', "metadata:$containerManifest", $hostManifest) }
    }
}
