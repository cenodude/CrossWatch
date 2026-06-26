$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$sdk = Join-Path $root ".android-sdk"
if (-not (Test-Path $sdk)) {
    $sdk = $env:ANDROID_HOME
}
if (-not $sdk -or -not (Test-Path $sdk)) {
    throw "Android SDK not found. Set ANDROID_HOME or create .android-sdk."
}

$platform = Join-Path $sdk "platforms\android-36.1"
$buildTools = Join-Path $sdk "build-tools\36.0.0"
$androidJar = Join-Path $platform "android.jar"
$aapt2 = Join-Path $buildTools "aapt2.exe"
$d8 = Join-Path $buildTools "d8.bat"
$zipalign = Join-Path $buildTools "zipalign.exe"
$apksigner = Join-Path $buildTools "apksigner.bat"

foreach ($tool in @($androidJar, $aapt2, $d8, $zipalign, $apksigner)) {
    if (-not (Test-Path $tool)) { throw "Missing build input: $tool" }
}

$localJavaHome = "C:\Users\pasca\.jdks\openjdk-17.0.1"
$javaHome = if (Test-Path (Join-Path $localJavaHome "bin\javac.exe")) { $localJavaHome } else { $env:JAVA_HOME }
$javac = Join-Path $javaHome "bin\javac.exe"
$keytool = Join-Path $javaHome "bin\keytool.exe"
if (-not (Test-Path $javac)) { throw "javac not found. Set JAVA_HOME to a JDK." }

$out = Join-Path $root "app\build\manual"
$classes = Join-Path $out "classes"
$dex = Join-Path $out "dex"
$compiledRes = Join-Path $out "compiled-res"
$generated = Join-Path $out "generated"
$apkUnsigned = Join-Path $out "crosswatch-companion-unsigned.apk"
$apkDexed = Join-Path $out "crosswatch-companion-dexed.apk"
$apkAligned = Join-Path $out "crosswatch-companion-aligned.apk"
$apkSigned = Join-Path $out "crosswatch-companion-debug.apk"
$keystore = Join-Path $out "debug.keystore"

New-Item -ItemType Directory -Force -Path $classes, $dex, $compiledRes, $generated | Out-Null
Get-ChildItem $classes -Recurse -File -ErrorAction SilentlyContinue | Remove-Item -Force
Get-ChildItem $dex -Recurse -File -ErrorAction SilentlyContinue | Remove-Item -Force
Get-ChildItem $compiledRes -Recurse -File -ErrorAction SilentlyContinue | Remove-Item -Force
Get-ChildItem $generated -Recurse -File -ErrorAction SilentlyContinue | Remove-Item -Force

$manifest = Join-Path $root "app\src\main\AndroidManifest.xml"
$sourceRoot = Join-Path $root "app\src\main\java"
$sources = @(Get-ChildItem $sourceRoot -Recurse -Filter "*.java" | ForEach-Object { $_.FullName })
$res = Join-Path $root "app\src\main\res"
$libDir = Join-Path $root "app\libs"
$libJars = @()
if (Test-Path $libDir) {
    $libJars = @(Get-ChildItem $libDir -Filter "*.jar" | ForEach-Object { $_.FullName })
}

& $aapt2 compile `
    --dir $res `
    -o (Join-Path $compiledRes "resources.zip")
if ($LASTEXITCODE -ne 0) { throw "aapt2 compile failed" }

& $aapt2 link `
    -o $apkUnsigned `
    -I $androidJar `
    --java $generated `
    --manifest $manifest `
    --min-sdk-version 26 `
    --target-sdk-version 36 `
    (Join-Path $compiledRes "resources.zip")
if ($LASTEXITCODE -ne 0) { throw "aapt2 link failed" }

$generatedSources = @(Get-ChildItem $generated -Recurse -Filter "*.java" | ForEach-Object { $_.FullName })
$compileClasspath = @($androidJar) + $libJars

& $javac `
    -source 11 `
    -target 11 `
    -classpath ($compileClasspath -join ";") `
    -d $classes `
    @sources `
    @generatedSources
if ($LASTEXITCODE -ne 0) { throw "javac failed" }

$classFiles = @(Get-ChildItem $classes -Recurse -Filter "*.class" | ForEach-Object { $_.FullName })
& $d8 `
    --min-api 26 `
    --output $dex `
    @classFiles `
    @libJars
if ($LASTEXITCODE -ne 0) { throw "d8 failed" }

Copy-Item $apkUnsigned $apkDexed -Force
Add-Type -AssemblyName System.IO.Compression
Add-Type -AssemblyName System.IO.Compression.FileSystem
$zip = [System.IO.Compression.ZipFile]::Open($apkDexed, [System.IO.Compression.ZipArchiveMode]::Update)
try {
    $existing = $zip.GetEntry("classes.dex")
    if ($existing) { $existing.Delete() }
    [System.IO.Compression.ZipFileExtensions]::CreateEntryFromFile($zip, (Join-Path $dex "classes.dex"), "classes.dex") | Out-Null
}
finally {
    $zip.Dispose()
}

& $zipalign -f 4 $apkDexed $apkAligned
if ($LASTEXITCODE -ne 0) { throw "zipalign failed" }

if (-not (Test-Path $keystore)) {
    & $keytool -genkeypair `
        -keystore $keystore `
        -storepass android `
        -keypass android `
        -alias androiddebugkey `
        -keyalg RSA `
        -keysize 2048 `
        -validity 10000 `
        -dname "CN=Android Debug,O=Android,C=US"
    if ($LASTEXITCODE -ne 0) { throw "debug keystore generation failed" }
}

& $apksigner sign `
    --ks $keystore `
    --ks-pass pass:android `
    --key-pass pass:android `
    --out $apkSigned `
    $apkAligned
if ($LASTEXITCODE -ne 0) { throw "apksigner failed" }

& $apksigner verify $apkSigned
if ($LASTEXITCODE -ne 0) { throw "apksigner verify failed" }

Write-Host "APK: $apkSigned"
