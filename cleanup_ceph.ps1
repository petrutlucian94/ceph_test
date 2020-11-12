rbd-wnbd list | awk '{print $5}' | % { write-host "Unmapping $_"; rbd-wnbd unmap $_ --hard-disconnect}
rbd ls | % { write-host "Removing $_"; rbd rm $_ }
