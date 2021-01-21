Param(
    $size="512M",
    $imageNamePrefix="cephCacheTest-",
    $reinstallDriver=$false,
    $driverInstallScript="C:\wnbd\reinstall.ps1",
    $fileSystem="NTFS",
    $doEject=$false,
    $fioRun=$false,
    $fioSize="500M",
    $testFileCount=50,
    $testListFiles=$true,
    $iterationCount=1
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
    log_message "Writing $count test files."
    1..$count | % { echo $_ > "$path\$_.txt" }
}

function run_fio($path, $size) {
    # Unfortunately the "--filename" parameter doesn't work with
    # Windows partitions.
    pushd $path
    log_message "Running fio: $path $size"
    fio --randrepeat=1 --direct=1 --gtod_reduce=1 `
        --name=test --bs=2M --iodepth=64 --size=$size `
        --readwrite=randwrite --numjobs=1
    popd
}

function check_files($path, $count) {
    log_message "Checking $count test files."
    1..$count | % {
        $file = "$path\$_.txt"
        $content = gc $file
        if ($_ -ne $content) {
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
        Format-Volume -FileSystem $fileSystem -Force -Confirm:$false

    $driveLetter = (Get-Partition -DiskNumber $diskNumber -PartitionNumber 2).DriveLetter

    $testPath = "${driveLetter}:\"
    if($testListFiles) {
        write_files $testPath $testFileCount
    }
    if($doEject) {
        log_message "Ejecting volume"
        # flush-volume $driveLetter
        # set-disk -Number $diskNumber -IsOffline:$true
        eject_volume $driveLetter
    }

    if ($fioRun) {
        run_fio $testPath $fioSize
    }

    1..$iterationCount | % {
        log_message "Unmapping image"
        rbd-wnbd unmap $imageName
        $mapped = $false

        rbd-wnbd map $imageName
        $mapped = $true

        $diskNumber = get_disk_number $imageName
        $driveLetter = (Get-Partition -DiskNumber $diskNumber -PartitionNumber 2).DriveLetter
        $testPath = "${driveLetter}:\"
        log_message "Remapped image: $imageName. Disk number: $diskNumber. Drive letter: $driveLetter"

        if($testListFiles) {
            check_files $testPath $testFileCount
        }
        log_message "Partition info:"
        get-volume $driveLetter
    }
}
finally {
    if($mapped) {
        log_message "Unmapping image: $imageName"
        rbd-wnbd unmap $imageName
    }

    log_message "Removing image: $imageName"
    rbd rm $imageName
}
