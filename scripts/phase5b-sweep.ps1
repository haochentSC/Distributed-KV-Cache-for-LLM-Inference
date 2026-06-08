# Phase 5b sweep: efficiency-vs-fairness Pareto curve for gdsf-elastic.
# Runs the identical 5a multi-tenant workload (seed 7, 16 MiB shard) against LRU and against
# gdsf-elastic at a range of fairness weights, printing overall + per-tenant hit rate per run.
$ErrorActionPreference = "Continue"
$srv  = "$PSScriptRoot\..\bin\cache-server.exe"
$load = "$PSScriptRoot\..\bin\loadgen.exe"
$floors = "A=6291456,B=7340032,C=3145728"   # same byte budgets as 5a; FLOORS under gdsf-elastic
$max = 16777216

$seeds = 7, 11, 23   # average over seeds: concurrency-8 eviction ordering is nondeterministic

function Run-One($label, $serverArgs) {
  $oA=@(); $sumO=0.0; $sumA=0.0; $sumB=0.0; $sumC=0.0; $totViol=0
  foreach ($seed in $seeds) {
    $p = Start-Process -FilePath $srv -ArgumentList $serverArgs -PassThru -WindowStyle Hidden
    Start-Sleep -Milliseconds 700
    try {
      $out = & $load -members localhost:50051 -multitenant -payload-bytes 65536 `
        -concurrency 8 -requests 800 -tail-blocks 2 -seed $seed 2>&1 | Out-String
    } finally {
      Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue
      Start-Sleep -Milliseconds 300
    }
    $sumO += [double]([regex]'block hit rate:\s+([\d.]+)%').Match($out).Groups[1].Value
    $totViol += [int]([regex]'correctness:\s+(\d+) violations').Match($out).Groups[1].Value
    foreach ($m in ([regex]'(?m)^\s+(\S+)\s+reqs=\d+\s+blocks=\d+\s+hit-rate=([\d.]+)%').Matches($out)) {
      switch ($m.Groups[1].Value) {
        "A" { $sumA += [double]$m.Groups[2].Value }
        "B" { $sumB += [double]$m.Groups[2].Value }
        "C" { $sumC += [double]$m.Groups[2].Value }
      }
    }
  }
  $n = $seeds.Count
  $a = $sumA/$n; $b = $sumB/$n; $c = $sumC/$n
  $min = [Math]::Min($a, [Math]::Min($b, $c))
  "{0,-20} overall={1,5:N1}%  viol={2}  A={3,4:N1}%  B={4,4:N1}%  C={5,4:N1}%  min={6,4:N1}%" -f `
    $label, ($sumO/$n), $totViol, $a, $b, $c, $min
}

Write-Host (Run-One "LRU baseline"        "-addr :50051 -eviction lru -max-bytes $max")
Write-Host (Run-One "gdsf static-cap"     "-addr :50051 -eviction gdsf -max-bytes $max -tenant-quota $floors")
foreach ($w in 0, 0.25, 0.5, 0.75, 1.0) {
  $args = "-addr :50051 -eviction gdsf-elastic -max-bytes $max -tenant-quota $floors -fairness-weight $w"
  Write-Host (Run-One ("gdsf-elastic w=$w") $args)
}
