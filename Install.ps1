param($installPath, $toolsPath, $package, $project) 

$dryadLibDir = Join-Path $installPath "lib\\net45"
$targetsFile = Join-Path $installPath "build\\Microsoft.Research.Dryad.targets"
#Write-Host "Package installed in " + $dryadLibDir + ".  Checking targets file " + $targetsFile 

(Get-Content $targetsFile) | Foreach-Object { $_ -replace 'DRYAD_PACKAGE_DIRECTORY', $installPath } | Set-Content $targetsFile
