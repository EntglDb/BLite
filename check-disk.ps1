$d = Get-WmiObject Win32_LogicalDisk | Where-Object { $_.DeviceID -eq 'C:' }
$free = [math]::Round($d.FreeSpace / 1GB, 2)
$total = [math]::Round($d.Size / 1GB, 1)
Write-Host "Free: $free GB  /  Total: $total GB"
