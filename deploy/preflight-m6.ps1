# M6 preflight — prepare / verify before two-host bring-up.
# Run from repo root (script resolves root from its location).
#
#   powershell -ExecutionPolicy Bypass -File deploy/preflight-m6.ps1 -Role Check
#   powershell -ExecutionPolicy Bypass -File deploy/preflight-m6.ps1 -Role B
#   powershell -ExecutionPolicy Bypass -File deploy/preflight-m6.ps1 -Role B -EnsureFirewall
#   powershell -ExecutionPolicy Bypass -File deploy/preflight-m6.ps1 -Role A -MachineBHost 192.168.1.11
#   powershell -ExecutionPolicy Bypass -File deploy/preflight-m6.ps1 -Role A -MachineBHost 192.168.1.11 -ProbePorts

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('Check', 'A', 'B')]
    [string] $Role,

    [string] $MachineBHost = '',

    [switch] $EnsureFirewall,

    [switch] $ProbePorts
)

$ErrorActionPreference = 'Stop'

$repo = if ($PSScriptRoot) {
    (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
} else {
    (Resolve-Path '.').Path
}

function Write-Step([string] $Message) {
    Write-Host ""
    Write-Host "==> $Message"
}

function Write-Ok([string] $Message) { Write-Host "OK  $Message" -ForegroundColor Green }
function Write-Warn([string] $Message) { Write-Host "WARN $Message" -ForegroundColor Yellow }
function Write-Fail([string] $Message) { Write-Host "FAIL $Message" -ForegroundColor Red }

function Get-GitHead {
    Push-Location $repo
    try {
        $head = (& git rev-parse --short HEAD 2>$null)
        if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($head)) {
            throw 'Cannot read git HEAD. Run inside a cloned repo.'
        }
        $branch = (& git rev-parse --abbrev-ref HEAD 2>$null)
        $dirty = (& git status --porcelain 2>$null)
        return [pscustomobject]@{
            Head   = $head.Trim()
            Branch = if ($branch) { $branch.Trim() } else { '?' }
            Dirty  = -not [string]::IsNullOrWhiteSpace(($dirty -join '').Trim())
        }
    } finally {
        Pop-Location
    }
}

function Assert-DockerLinux {
    Write-Step 'Docker Linux containers'
    $platform = & docker info --format '{{.OSType}}' 2>$null
    if ($LASTEXITCODE -ne 0) {
        throw 'Docker is not ready. Start Docker Desktop and retry.'
    }
    if ($platform -ne 'linux') {
        throw "Linux containers required, got: $platform"
    }
    Write-Ok "Docker OSType=$platform"
}

function Assert-ComposeConfig([string] $EnvFile, [string] $ComposeFile) {
    $envPath = Join-Path $repo $EnvFile
    $composePath = Join-Path $repo $ComposeFile
    if (-not (Test-Path $envPath)) { throw "Missing $EnvFile" }
    if (-not (Test-Path $composePath)) { throw "Missing $ComposeFile" }
    & docker compose --env-file $envPath -f $composePath config --quiet
    if ($LASTEXITCODE -ne 0) { throw "compose config failed: $ComposeFile" }
    Write-Ok "compose config: $ComposeFile"
}

function Ensure-EnvFromExample([string] $ExampleRel, [string] $TargetRel) {
    $example = Join-Path $repo $ExampleRel
    $target = Join-Path $repo $TargetRel
    if (-not (Test-Path $example)) { throw "Missing $ExampleRel" }
    if (-not (Test-Path $target)) {
        Copy-Item $example $target
        Write-Ok "Created $TargetRel from example"
    } else {
        Write-Ok "Found $TargetRel (not overwritten)"
    }
    return $target
}

function Get-LanIPv4Candidates {
    Get-NetIPAddress -AddressFamily IPv4 -ErrorAction SilentlyContinue |
        Where-Object {
            $_.IPAddress -notlike '127.*' -and
            $_.IPAddress -notlike '169.254.*' -and
            $_.PrefixOrigin -ne 'WellKnown'
        } |
        Sort-Object -Property InterfaceAlias, IPAddress |
        Select-Object -Property IPAddress, InterfaceAlias, PrefixLength
}

function Get-PreferredLanIPv4([object[]] $Candidates) {
    if (-not $Candidates -or $Candidates.Count -eq 0) { return $null }
    $preferred = @(
        $Candidates | Where-Object {
            $_.InterfaceAlias -match '(?i)wi-?fi|wlan|wireless' -and
            $_.InterfaceAlias -notmatch '(?i)vethernet|vmware|virtualbox|hyper-v|wsl|loopback|docker|veth'
        }
    )
    if ($preferred.Count -gt 0) { return $preferred[0].IPAddress }
    $physical = @(
        $Candidates | Where-Object {
            $_.InterfaceAlias -notmatch '(?i)vethernet|vmware|virtualbox|hyper-v|wsl|loopback|docker|veth|virtual'
        }
    )
    if ($physical.Count -gt 0) { return $physical[0].IPAddress }
    return $Candidates[0].IPAddress
}

function Set-MachineBHost([string] $EnvPath, [string] $HostIp) {
    if ([string]::IsNullOrWhiteSpace($HostIp)) {
        throw 'Role A requires -MachineBHost <IPv4 of machine B>.'
    }
    if ($HostIp -match '^(127\.|0\.|localhost$)') {
        throw "MACHINE_B_HOST must not be localhost/127.x: $HostIp"
    }
    if ($HostIp -notmatch '^\d{1,3}(\.\d{1,3}){3}$') {
        Write-Warn "MACHINE_B_HOST is not dotted IPv4: $HostIp (allowed if intentional hostname)"
    }
    $raw = Get-Content -Path $EnvPath -Raw
    if ($raw -match '(?m)^MACHINE_B_HOST=') {
        $raw = [regex]::Replace($raw, '(?m)^MACHINE_B_HOST=.*$', "MACHINE_B_HOST=$HostIp")
    } else {
        $raw = $raw.TrimEnd() + "`r`nMACHINE_B_HOST=$HostIp`r`n"
    }
    Set-Content -Path $EnvPath -Value $raw -NoNewline
    Write-Ok "MACHINE_B_HOST=$HostIp in deploy/.env.machine-a"
}

function Ensure-StorageFirewallRules {
    Write-Step 'Windows Firewall inbound TCP 50052/50053'
    $rules = @(
        @{ Name = 'DFS-M6-Storage-50052'; Port = 50052 },
        @{ Name = 'DFS-M6-Storage-50053'; Port = 50053 }
    )
    foreach ($rule in $rules) {
        $existing = Get-NetFirewallRule -DisplayName $rule.Name -ErrorAction SilentlyContinue
        if ($existing) {
            Write-Ok "Rule exists: $($rule.Name)"
            continue
        }
        try {
            New-NetFirewallRule `
                -DisplayName $rule.Name `
                -Direction Inbound `
                -Action Allow `
                -Protocol TCP `
                -LocalPort $rule.Port `
                -Profile Any |
                Out-Null
            Write-Ok "Created rule $($rule.Name) (TCP $($rule.Port))"
        } catch {
            Write-Fail "Cannot create $($rule.Name): $($_.Exception.Message)"
            Write-Warn 'Re-run in Admin PowerShell: .\deploy\preflight-m6.ps1 -Role B -EnsureFirewall'
            throw
        }
    }
}

function Test-RemotePorts([string] $HostIp) {
    Write-Step "Probe TCP ${HostIp}:50052 / :50053"
    foreach ($port in @(50052, 50053)) {
        $result = Test-NetConnection -ComputerName $HostIp -Port $port -WarningAction SilentlyContinue
        if ($result.TcpTestSucceeded) {
            Write-Ok "${HostIp}:${port} reachable"
        } else {
            Write-Fail "${HostIp}:${port} NOT reachable"
            Write-Warn 'Is machine B up? Firewall on B? Same LAN/hotspot? AP isolation?'
            throw "Probe failed: ${HostIp}:${port}"
        }
    }
}

Write-Host "M6 preflight Role=$Role  repo=$repo"

$git = Get-GitHead
Write-Step 'Git commit (both hosts must share the same HEAD)'
Write-Ok "branch=$($git.Branch) HEAD=$($git.Head)"
if ($git.Dirty) {
    Write-Warn 'Working tree dirty - commit/stash or keep the same patch on both hosts before demo.'
} else {
    Write-Ok 'Working tree clean'
}

Assert-DockerLinux

switch ($Role) {
    'Check' {
        Write-Step 'Validate compose A/B from examples (no real machine B required)'
        Assert-ComposeConfig 'deploy/.env.machine-a.example' 'deploy/compose.machine-a.yml'
        Assert-ComposeConfig 'deploy/.env.machine-b.example' 'deploy/compose.machine-b.yml'
        Write-Step 'LAN IPv4 candidates on this host (send one if this is machine B)'
        $ips = @(Get-LanIPv4Candidates)
        if ($ips.Count -eq 0) {
            Write-Warn 'No LAN IPv4 found. Check Wi-Fi/hotspot.'
        } else {
            $ips | ForEach-Object {
                Write-Host ("  {0,-15}  {1}" -f $_.IPAddress, $_.InterfaceAlias)
            }
            $hint = Get-PreferredLanIPv4 $ips
            if ($hint) { Write-Host "  Suggested (if this is B): $hint" }
        }
        Write-Host ""
        Write-Ok 'P2 Check done. Tomorrow: Role B on machine B, Role A -MachineBHost <IP> on machine A.'
    }

    'B' {
        Write-Step 'Prepare machine B'
        Ensure-EnvFromExample 'deploy/.env.machine-b.example' 'deploy/.env.machine-b'
        Assert-ComposeConfig 'deploy/.env.machine-b' 'deploy/compose.machine-b.yml'

        Write-Step 'LAN IPv4 - send one address to machine A (MACHINE_B_HOST)'
        $ips = @(Get-LanIPv4Candidates)
        if ($ips.Count -eq 0) {
            Write-Fail 'No LAN IPv4'
            throw 'Connect Wi-Fi/hotspot and retry.'
        }
        $ips | ForEach-Object {
            Write-Host ("  {0,-15}  {1}" -f $_.IPAddress, $_.InterfaceAlias)
        }
        $hint = Get-PreferredLanIPv4 $ips
        Write-Host "  Suggested: $hint"

        if ($EnsureFirewall) {
            Ensure-StorageFirewallRules
        } else {
            Write-Warn 'Firewall not ensured. When meeting: -EnsureFirewall (Admin).'
        }

        Write-Host ""
        Write-Ok 'Machine B preflight done. Next: compose up machine-b, then tell A the IP.'
        Write-Host '  docker compose --env-file deploy/.env.machine-b -f deploy/compose.machine-b.yml up -d --build --wait'
    }

    'A' {
        Write-Step 'Prepare machine A'
        $envA = Ensure-EnvFromExample 'deploy/.env.machine-a.example' 'deploy/.env.machine-a'
        Set-MachineBHost -EnvPath $envA -HostIp $MachineBHost
        Assert-ComposeConfig 'deploy/.env.machine-a' 'deploy/compose.machine-a.yml'

        if ($ProbePorts) {
            Test-RemotePorts -HostIp $MachineBHost
        } else {
            Write-Warn 'Ports not probed yet. After B is up, add -ProbePorts'
        }

        Write-Host ""
        Write-Ok 'Machine A preflight done. Next (after B up): compose up machine-a + Vite.'
        Write-Host '  docker compose --env-file deploy/.env.machine-a -f deploy/compose.machine-a.yml up -d --build --wait'
    }
}
