Param(
    $size="3G",
    $bs="2M",
    $iodepth=64,
    $numjobs=2,
    $loops=3,
    $jobtype="read",
    $imageNamePrefix="cephTest",
    $reinstallDriver=$false,
    $driverInstallScript="C:\wnbd\reinstall.ps1",
    $runIndefinitely=$false
)

$ErrorActionPreference = "Stop"

function get_utc_iso8601_time() {
    Get-Date(Get-Date).ToUniversalTime() -uformat '+%Y-%m-%dT%H:%M:%S.000Z'
}

function log_message($message) {
    write-host "[$(get_utc_iso8601_time)] $message"
}

function get_disk_number($image) {
    $mappingJson = rbd-wnbd show $image --format=json
    $mappingJson = $mappingJson | ConvertFrom-Json
    $diskNumber = $mappingJson.disk_number
    echo $diskNumber
}

function run_fio($disk_number, $size, $bs, $iodepth, $loops, $numjobs, $jobtype) {
    log_message "Running fio test."
    log_message "Disk number: $disk_number, size: $size, block size: $bs, concurrent reqs: $iodepth"
    log_message "Number of workers: $numjobs, job type: $jobtype"
    fio --filename="\\.\PhysicalDrive${disk_number}" `
        --randrepeat=1 --direct=1 --gtod_reduce=1 `
        --name=test --bs=$bs --iodepth=$iodepth --size=$size `
        --readwrite=$jobtype --numjobs=$numjobs --loops=$loops `
        
}

do {
    $uuid = [guid]::NewGuid().Guid
    $imageName = "${imageNamePrefix}-$uuid"

    if($reinstallDriver) {
        log_message "Reinstalling driver."
        & $driverInstallScript
    }

    log_message "Creating image: $imageName"
    rbd create $imageName --size $size --image-shared
    $mapped = $false

    try {
        log_message "Mapping image: $imageName"
        rbd-wnbd map $imageName --wnbd_thread_count=1
        $mapped = $true

        $diskNumber = get_disk_number $imageName
        log_message "Mapped image: $imageName. Disk number: $diskNumber"

	log_message "run_fio $diskNumber $size $bs $iodepth $loops $numjobs $jobtype"
        run_fio $diskNumber $size $bs $iodepth $loops $numjobs $jobtype
    }
    finally {
        if($mapped) {
            log_message "Unmapping image: $imageName"
            rbd-wnbd unmap $imageName
        }

        log_message "Removing image: $imageName"
        rbd rm $imageName
    }
} while ($runIndefinitely)
