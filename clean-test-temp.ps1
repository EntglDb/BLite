# Ripulisce le cartelle temporanee create dai test BLite in $env:TEMP
# Sicuro: rimuove solo cartelle con prefissi noti + GUID nel nome

$tmp = $env:TEMP
$prefixes = @(
    'blite_e2e_',
    'blite_sm_',
    'blite_mig_',
    'blite_multifile_',
    'blite_adv_',
    'blite_eng_',
    'blite_mf_',
    'blite_bc_',
    'blite_idx_',
    'blite_chk_',
    'blite_cdc_',
    'blite_kv_',
    'blite_ts_',
    'blite_sp_',
    'blite_vect_',
    'blite_wtr_',
    'blite_perf_',
    'blite_sch_',
    'blql_cov_',
    'blql_',
    'idx_linq_',
    'test_crosspath_',
    'linq_tests_'
)

$guidPattern = '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'

$removed = 0
$errors  = 0
$bytes   = 0L

foreach ($prefix in $prefixes) {
    $dirs = Get-ChildItem -Path $tmp -Directory -Filter "${prefix}*" -ErrorAction SilentlyContinue |
            Where-Object { $_.Name -match $guidPattern }

    foreach ($dir in $dirs) {
        try {
            $sz = (Get-ChildItem $dir.FullName -Recurse -File -ErrorAction SilentlyContinue |
                   Measure-Object -Property Length -Sum).Sum
            $bytes += $sz
            [System.IO.Directory]::Delete($dir.FullName, $true)
            $removed++
        } catch {
            Write-Warning "Impossibile eliminare: $($dir.FullName) - $_"
            $errors++
        }
    }
}

$mb = [math]::Round($bytes / 1MB, 1)
Write-Host ""
Write-Host "============================================"
Write-Host " BLite Temp Cleanup - completato"
Write-Host "============================================"
Write-Host " Cartelle rimosse : $removed"
Write-Host " Spazio liberato  : $mb MB"
if ($errors -gt 0) {
    Write-Host " Errori           : $errors (controlla i warning sopra)"
}
Write-Host "============================================"
