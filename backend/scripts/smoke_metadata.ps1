# Explicit local-dev lifecycle smoke: stop/start node-2, restart Metadata and PostgreSQL.
$ErrorActionPreference = 'Stop'
$repo = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$composeArgs = @('compose', '--env-file', (Join-Path $repo 'deploy/.env'),
    '-f', (Join-Path $repo 'deploy/compose.local.yml'))

function Invoke-Compose {
    param([string[]] $Arguments)
    $output = & docker @composeArgs @Arguments
    if ($LASTEXITCODE -ne 0) { throw "Compose failed: $($Arguments -join ' ')" }
    return $output
}

function Invoke-Smoke {
    param([string[]] $Arguments)
    Invoke-Compose -Arguments (@('exec', '-T', 'metadata', 'python', 'scripts/smoke_metadata.py') + $Arguments)
}

# Read-only prerequisites; do not start an initially unavailable service.
$platform = & docker info --format '{{.OSType}}'
if ($LASTEXITCODE -ne 0 -or $platform -ne 'linux') { throw 'Docker Linux is required.' }
$services = Invoke-Compose -Arguments @('ps', '--format', 'json')
$state = @($services | ForEach-Object { $_ | ConvertFrom-Json })
$required = @('postgres', 'metadata', 'storage-node-1', 'storage-node-2', 'storage-node-3')
foreach ($service in $required) {
    $entry = @($state | Where-Object { $_.Service -eq $service })
    if ($entry.Count -ne 1 -or $entry[0].State -ne 'running' -or $entry[0].Health -ne 'healthy') {
        throw "Prerequisite $service must already be running and healthy. Recovery: docker compose --env-file deploy/.env -f deploy/compose.local.yml up -d --wait"
    }
}

$schema = 'm2_smoke_' + [Guid]::NewGuid().ToString('N')
$fixtureCreated = $false
$nodeStopped = $false
try {
    Write-Host 'P5 baseline: REST identity/domain/metrics, cluster and live/ready'
    Invoke-Smoke -Arguments @('baseline')

    $volumesBefore = @{}
    foreach ($service in $required) {
        if ($service -eq 'metadata') { continue }
        $id = Invoke-Compose -Arguments @('ps', '-q', $service)
        $mountsJson = & docker inspect --format '{{json .Mounts}}' $id
        if ($LASTEXITCODE -ne 0) { throw "Cannot inspect mounts for $service" }
        $mounts = @($mountsJson | ConvertFrom-Json)
        $destination = if ($service -eq 'postgres') { '/var/lib/postgresql/data' } else { '/data/chunks' }
        $mount = @($mounts | Where-Object { $_.Destination -eq $destination })
        if ($mount.Count -ne 1 -or $mount[0].Type -ne 'volume') { throw "Expected named volume for $service" }
        $volumesBefore[$service] = $mount[0].Name
    }
    $storageVolumes = @($volumesBefore.GetEnumerator() | Where-Object { $_.Key -like 'storage-*' } | ForEach-Object { $_.Value })
    if (@($storageVolumes | Select-Object -Unique).Count -ne 3) { throw 'Storage volumes must be distinct' }
    Write-Host "Distinct Storage volumes: $($storageVolumes -join ', ')"

    Write-Host 'P5 node down: stop node-2 and observe detector through REST'
    $nodeStopped = $true
    Invoke-Compose -Arguments @('stop', 'storage-node-2')
    Invoke-Smoke -Arguments @('down', '--node', 'node-2')
    Write-Host 'P5 node recovery: start the existing container and volume'
    Invoke-Compose -Arguments @('start', '--wait', 'storage-node-2')
    $nodeStopped = $false
    Invoke-Smoke -Arguments @('baseline')

    Write-Host 'P5 Metadata restart: require fresh health snapshots after restart'
    Invoke-Compose -Arguments @('restart', 'metadata')
    $after = [DateTime]::UtcNow.ToString('o')
    Invoke-Smoke -Arguments @('baseline', '--after', $after)

    Write-Host 'P5 PostgreSQL persistence: seed only a new UUID fixture schema'
    $fixtureCreated = $true
    $fixture = Invoke-Smoke -Arguments @('fixture-seed', '--schema', $schema) | ConvertFrom-Json
    Invoke-Compose -Arguments @('restart', 'postgres')
    $after = [DateTime]::UtcNow.ToString('o')
    Invoke-Smoke -Arguments @('baseline', '--after', $after)
    Invoke-Smoke -Arguments @('fixture-verify', '--schema', $schema, '--digest', $fixture.digest)

    foreach ($service in $volumesBefore.Keys) {
        $id = Invoke-Compose -Arguments @('ps', '-q', $service)
        $mountsJson = & docker inspect --format '{{json .Mounts}}' $id
        if ($LASTEXITCODE -ne 0) { throw "Cannot inspect mounts after restart: $service" }
        $names = @(($mountsJson | ConvertFrom-Json) | ForEach-Object { $_.Name })
        if ($volumesBefore[$service] -notin $names) { throw "Volume changed for $service" }
    }
    Write-Host 'P5 passed: REST down/recovery, real service restarts, DB fixture preserved, volumes unchanged'
}
finally {
    # Attempt both cleanup steps even if restoring a service fails.
    try {
        if ($nodeStopped) { Invoke-Compose -Arguments @('start', '--wait', 'storage-node-2') }
    }
    finally {
        if ($fixtureCreated) { Invoke-Smoke -Arguments @('fixture-drop', '--schema', $schema) }
    }
}
