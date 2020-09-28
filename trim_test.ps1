Param(
    $size="512M",
    $imageNamePrefix="cephUnmapTest-",
    $reinstallDriver=$false,
    $driverInstallScript="C:\wnbd\reinstall.ps1"
)

$ErrorActionPreference = "Stop"

function get_utc_iso8601_time() {
    Get-Date(Get-Date).ToUniversalTime() -uformat '+%Y-%m-%dT%H:%M:%S.000Z'
}

function log_message($message) {
    write-host "[$(get_utc_iso8601_time)] $message"
}

function get_disk_number($image) {
    $mappingJson = rbd-nbd show $image --format=json
    $mappingJson = $mappingJson | ConvertFrom-Json
    $diskNumber = $mappingJson.disk_number
    echo $diskNumber
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
    rbd-nbd map $imageName
    $mapped = $true

    $diskNumber = get_disk_number $imageName
    log_message "Mapped image: $imageName. Disk number: $diskNumber"

    log_message "Trim support:"
    trim_check.exe "\\.\PhysicalDrive${diskNumber}"

    log_message "Initializing disk and creating partition."
    Get-Disk -Number $diskNumber | `
        Initialize-Disk -PassThru | `
        New-Partition -AssignDriveLetter -UseMaximumSize | `
        Format-Volume -Force -Confirm:$false

    $driveLetter = (Get-Partition -DiskNumber $diskNumber -PartitionNumber 2).DriveLetter

    log_message "Optimizing volume $driveLetter using TRIM."
    Optimize-Volume -DriveLetter $driveLetter -ReTrim
}
finally {
    if($mapped) {
        log_message "Unmapping image: $imageName"
        rbd-nbd unmap $imageName
    }

    log_message "Removing image: $imageName"
    rbd rm $imageName
}
