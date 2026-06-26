param(
    [string]$Group = "com.android.tools.build",
    [string]$Module = "gradle",
    [string]$Version = "7.0.3"
)

$ErrorActionPreference = "Stop"

$cache = Join-Path $env:USERPROFILE ".gradle\caches\modules-2\files-2.1"
$repo = Join-Path (Split-Path -Parent $PSScriptRoot) ".local-maven"
New-Item -ItemType Directory -Force -Path $repo | Out-Null

$seen = @{}
$queue = New-Object System.Collections.Generic.Queue[object]
$queue.Enqueue([pscustomobject]@{ g = $Group; m = $Module; v = $Version })

function Group-Path([string]$g) {
    return $g.Replace(".", [System.IO.Path]::DirectorySeparatorChar)
}

function Resolve-Token($value, $props) {
    if (-not $value) { return $null }
    $s = [string]$value
    if ($s -match '^\$\{(.+)\}$') {
        $key = $Matches[1]
        if ($props.ContainsKey($key)) { return [string]$props[$key] }
        return $null
    }
    return $s
}

function Copy-Coord([string]$g, [string]$m, [string]$v) {
    if (-not $g -or -not $m -or -not $v) { return @() }
    $key = "${g}:${m}:${v}"
    if ($seen.ContainsKey($key)) { return @() }
    $seen[$key] = $true

    $srcDir = Join-Path $cache (Join-Path $g (Join-Path $m $v))
    if (-not (Test-Path $srcDir)) {
        Write-Host "missing $key"
        return @()
    }

    $dstDir = Join-Path $repo (Join-Path (Group-Path $g) (Join-Path $m $v))
    New-Item -ItemType Directory -Force -Path $dstDir | Out-Null

    Get-ChildItem $srcDir -Recurse -File |
        Where-Object { $_.Name -like "$m-$v*" -and $_.Extension -in ".pom", ".jar", ".aar", ".module" } |
        ForEach-Object { Copy-Item $_.FullName -Destination (Join-Path $dstDir $_.Name) -Force }

    $deps = @()
    $moduleFile = Get-ChildItem $dstDir -Filter "$m-$v.module" -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($moduleFile) {
        $json = Get-Content $moduleFile.FullName -Raw | ConvertFrom-Json
        foreach ($variant in @($json.variants)) {
            foreach ($dep in @($variant.dependencies)) {
                $dv = $null
                if ($dep.version) {
                    $dv = $dep.version.requires
                    if (-not $dv) { $dv = $dep.version.strictly }
                    if (-not $dv) { $dv = $dep.version.prefers }
                }
                if ($dep.group -and $dep.module -and $dv) {
                    $deps += [pscustomobject]@{ g = $dep.group; m = $dep.module; v = $dv }
                }
            }
        }
    }

    $pomFile = Get-ChildItem $dstDir -Filter "$m-$v.pom" -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($pomFile) {
        [xml]$pom = Get-Content $pomFile.FullName
        $props = @{}
        if ($pom.project.properties) {
            foreach ($child in $pom.project.properties.ChildNodes) {
                $props[$child.Name] = $child.InnerText
            }
        }
        if ($pom.project.parent) {
            $pg = Resolve-Token $pom.project.parent.groupId $props
            $pm = Resolve-Token $pom.project.parent.artifactId $props
            $pv = Resolve-Token $pom.project.parent.version $props
            if ($pg -and $pm -and $pv) {
                $deps += [pscustomobject]@{ g = $pg; m = $pm; v = $pv }
            }
        }
        foreach ($dep in @($pom.project.dependencies.dependency)) {
            $scope = [string]$dep.scope
            $optional = [string]$dep.optional
            if ($scope -eq "test" -or $optional -eq "true") { continue }
            $dg = Resolve-Token $dep.groupId $props
            $dm = Resolve-Token $dep.artifactId $props
            $dv = Resolve-Token $dep.version $props
            if ($dg -and $dm -and $dv) {
                $deps += [pscustomobject]@{ g = $dg; m = $dm; v = $dv }
            }
        }
    }

    return $deps
}

while ($queue.Count -gt 0) {
    $coord = $queue.Dequeue()
    foreach ($dep in Copy-Coord $coord.g $coord.m $coord.v) {
        $queue.Enqueue($dep)
    }
}

Write-Host "mirrored=$($seen.Count)"
Write-Host "repo=$repo"
