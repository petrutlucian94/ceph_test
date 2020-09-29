Param(
    $size="512M",
    $imageNamePrefix="cephCacheTest-",
    $reinstallDriver=$false,
    $driverInstallScript="C:\wnbd\reinstall.ps1",
    $testFileCount = 50
)

$ErrorActionPreference = "Stop"

function get_utc_iso8601_time() {
    Get-Date(Get-Date).ToUniversalTime() -uformat '+%Y-%m-%dT%H:%M:%S.000Z'
}

function log_message($message) {
    write-host "[$(get_utc_iso8601_time)] $message"
}

function get_disk_number($image, $tries=5, $tryInterval=2) {
    while ($tries) {
        $mappingJson = rbd-wnbd show $image --format=json
        $mappingJson = $mappingJson | ConvertFrom-Json
        $diskNumber = $mappingJson.disk_number
        if ($diskNumber -gt 0) {
            echo $diskNumber
            return
        }
        else {
            $tries -= 1
            start-sleep $tryInterval
        }
    }
    throw "Could not get disk number for $image."
}

function write_files($path, $count) {
    1..$count | % { echo $count > "$path\$count.txt" }
}

function check_files($path, $count) {
    1..$count | % {
        $file = "$path\$count.txt"
        $content = gc $file
        if ($count -ne $content) {
            throw "Invalid file $file content: $content"
        }
    }
}

function eject_volume($driveLetter) {
    $vol = gwmi -Class win32_volume | ? { $_.Name -eq "${driveLetter}:\" }
    # $vol.DriveLetter = $null
    # $vol.Put()
    $vol.Dismount($false, $false)
}

$uuid = [guid]::NewGuid().Guid
$imageName = "${imageNamePrefix}$uuid"

if($reinstallDriver) {
    log_message "Reinstalling driver."
    & $driverInstallScript
}

log_message "Creating image: $imageName"
rbd create $imageName --size $size --image-shared
$mapped = $false

try {
    log_message "Mapping image: $imageName"
    rbd-wnbd map $imageName
    $mapped = $true

    $diskNumber = get_disk_number $imageName
    log_message "Mapped image: $imageName. Disk number: $diskNumber"

    log_message "Initializing disk and creating partition."
    Get-Disk -Number $diskNumber | `
        Initialize-Disk -PassThru | `
        New-Partition -AssignDriveLetter -UseMaximumSize | `
        Format-Volume -Force -Confirm:$false

    $driveLetter = (Get-Partition -DiskNumber $diskNumber -PartitionNumber 2).DriveLetter

    $testPath = "${driveLetter}:\"
    write_files $testPath $testFileCount
    # flush-volume $driveLetter
    # set-disk -Number $diskNumber -IsOffline:$true
    log_message "Ejecting volume"
    eject_volume $driveLetter

    log_message "Unmapping image"
    rbd-wnbd unmap $imageName
    $mapped = $false

    rbd-wnbd map $imageName
    $mapped = $true

    $diskNumber = get_disk_number $imageName
    $driveLetter = (Get-Partition -DiskNumber $diskNumber -PartitionNumber 2).DriveLetter
    $testPath = "${driveLetter}:\"
    log_message "Remapped image: $imageName. Disk number: $diskNumber. Drive letter: $driveLetter"

    check_files $testPath $testFileCount
}
finally {
    if($mapped) {
        log_message "Unmapping image: $imageName"
        rbd-wnbd unmap $imageName
    }

    log_message "Removing image: $imageName"
    rbd rm $imageName
}
