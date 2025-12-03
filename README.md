## Project overview

This repository contains a data production pipeline for flight delays analysis. The pipeline implements two primary processing patterns:

- Bulk (batch) processing: process the full flights archive and write cleaned datasets and daily aggregations.
- Streaming processing: a file-based streaming source (files in a GCS folder) with windowed aggregations for near-real-time insights.

Main locations in the repository:

- `data/raw/` — raw CSV files (airlines, airports, flights.csv).
- `data/tmp/` — temporary samples for streaming tests.
- `src/` — Spark applications:
  - `src/batch_flight_delays.py` (batch job)
  - `src/stream_flight_delays.py` (streaming job)
- `scripts/` — helper scripts for dataset preparation and splitting.

## Relationship between data and jobs

- `src/batch_flight_delays.py` reads full flight files from `gs://<BUCKET>/raw/flights/` and the reference tables `airlines.csv` and `airports.csv`. It performs cleaning, enrichment (joins on airline/airport), and produces two outputs:
  - Silver: cleaned and enriched flight Parquet partitioned by `FL_DATE` (gs://<BUCKET>/silver/flights_clean/).
  - Gold: aggregated daily route statistics (gs://<BUCKET>/gold/delay_stats/) and a write to BigQuery (`{PROJECT}.{DATASET}.daily_delay_stats`).

- `src/stream_flight_delays.py` listens for new CSV files placed in `gs://<BUCKET>/stream/flights_in/`. For each new file a streaming pipeline runs that:
  - reads files with `maxFilesPerTrigger` to throttle bursts,
  - applies similar cleaning logic as the batch job,
  - computes windowed aggregations (15-minute windows, 5-minute slide),
  - writes results to multiple sinks: console (debug), parquet (gs://<BUCKET>/stream/flights_out/) and BigQuery (`{PROJECT}.{DATASET}.live_window_delay_stats`) with checkpointing in GCS.

## Existing PowerShell commands — exact commands preserved

Below are the original PowerShell commands as they appeared in the repository. Each command is presented in its own PowerShell code block. Each block is preceded by a short subtitle that describes the purpose of the command. The commands themselves are unchanged.

### Set local variables used in the examples
```powershell
$PROJECT="jads-de-assignment-2"
$REGION="europe-west4"
$ZONE="europe-west4-a"
$BUCKET="airline-delays-bucket"
$CLUSTER="de-flight-cluster-prod"
$LOCAL_REPO="C:\Users\marti\Documents\Python JADS\DataEngineering\Assignment 2\de-flight-delays"
$BATCH_PY="$LOCAL_REPO\src\batch_flight_delays.py"
$STREAM_PY="$LOCAL_REPO\src\stream_flight_delays.py"
```

### Create a Dataproc cluster (Spark execution environment)
```powershell
gcloud dataproc clusters create $CLUSTER `
  --region=$REGION --zone=$ZONE `
  --project=$PROJECT `
  --num-workers=4 `
  --worker-machine-type=n1-standard-4 `
  --master-machine-type=n1-standard-4 `
  --worker-boot-disk-size=50 `
  --master-boot-disk-size=50 `
  --image-version=2.1-debian11 `
  --quiet
```

### Describe the Dataproc cluster (control / verify)
```powershell
gcloud dataproc clusters describe $CLUSTER --region=$REGION --project=$PROJECT --format="yaml"
```

### Copy batch job script to GCS
```powershell
gsutil cp $BATCH_PY gs://$BUCKET/code/
```

### Copy streaming job script to GCS
```powershell
gsutil cp $STREAM_PY gs://$BUCKET/code/
```

### Upload airports reference CSV to GCS
```powershell
gsutil cp $LOCAL_REPO\data\raw\airports.csv gs://$BUCKET/raw/airports/airports.csv
```

### Upload airlines reference CSV to GCS
```powershell
gsutil cp $LOCAL_REPO\data\raw\airlines.csv gs://$BUCKET/raw/airlines/airlines.csv
```

### Example: (commented) copy many flights CSVs into GCS (for full raw dataset)
```powershell
# For full flights raw (may be large): 
# gsutil -m cp "C:\path\to\flights\*.csv" gs://$BUCKET/raw/flights/
```

### Ensure temporary folder exists and create a small streaming sample
```powershell
if (!(Test-Path -Path "$LOCAL_REPO\data\tmp")) { New-Item -ItemType Directory -Path "$LOCAL_REPO\data\tmp" | Out-Null }
Get-Content -Path "$LOCAL_REPO\data\raw\flights.csv" -TotalCount 51 | Set-Content -Path "$LOCAL_REPO\data\tmp\stream_sample_001.csv"
```

### Upload the streaming sample to the GCS streaming input folder
```powershell
gsutil cp "$LOCAL_REPO\data\tmp\stream_sample_001.csv" "gs://$BUCKET/stream/flights_in/stream_sample_001.csv"
```

### Submit the batch Spark job to Dataproc
```powershell
gcloud dataproc jobs submit pyspark $BATCH_PY `
  --cluster=$CLUSTER --region=$REGION --project=$PROJECT `
  --properties="spark.network.timeout=800s,spark.executor.heartbeatInterval=60s,spark.rpc.askTimeout=600s" `
  --verbosity=info
```

### Wait before starting the streaming job (allow batch to finish or cluster to stabilize)
```powershell
Write-Host "Waiting 180 seconds before starting streaming job..."
```

### Sleep for 180 seconds (pause)
```powershell
Start-Sleep -Seconds 180
```

### Submit the streaming Spark job to Dataproc (async, with max runtime)
```powershell
# Option 1: With async (job continues in background, manual stop needed)
gcloud dataproc jobs submit pyspark $STREAM_PY `
  --cluster=$CLUSTER --region=$REGION --project=$PROJECT `
  --properties="spark.network.timeout=800s,spark.executor.heartbeatInterval=60s,spark.rpc.askTimeout=600s" `
  --async --verbosity=info

# Option 2: Without async (runs for ~5 minutes then you can Ctrl+C to stop gracefully)
# Note: Streaming jobs run indefinitely, so you need to manually stop them
gcloud dataproc jobs submit pyspark $STREAM_PY `
  --cluster=$CLUSTER --region=$REGION --project=$PROJECT `
  --properties="spark.network.timeout=800s,spark.executor.heartbeatInterval=60s,spark.rpc.askTimeout=600s" `
  --verbosity=info
```

### Upload stream chunks sequentially to trigger the streaming job
```powershell
for ($i=1; $i -le 20; $i++) {
  $source = "$LOCAL_REPO\data\stream_chunks\flights_part_$("{0:D3}" -f $i).csv"
  $ts = Get-Date -Format "yyyyMMddHHmmss"
  $dest = "gs://$BUCKET/stream/flights_in/stream_${ts}_part_$("{0:D3}" -f $i).csv"
  Write-Host "Uploading chunk $i of 20: $dest"
  gsutil cp $source $dest
  Start-Sleep -Seconds 10
}
```

### Delete the Dataproc cluster (optional cleanup)
```powershell
gcloud dataproc clusters delete $CLUSTER --region=$REGION --project=$PROJECT --quiet
```

## Explanation summary (what the commands do)

- The variable block defines local PowerShell variables for project, region, bucket and paths used in the examples.
- The `gcloud dataproc clusters create` command provisions a Dataproc cluster used as the Spark execution environment.
- `gcloud dataproc clusters describe` is a quick control-step to inspect the created cluster.
- Multiple `gsutil cp` commands upload job scripts and reference CSVs to GCS so Dataproc jobs can access them.
- A small streaming sample is created locally and uploaded to `gs://<BUCKET>/stream/flights_in/` to feed the streaming job.
- `gcloud dataproc jobs submit pyspark` runs the batch and streaming Spark jobs (`src/batch_flight_delays.py` and `src/stream_flight_delays.py`). The streaming job is started with `--async` so it continues running on the cluster.
- A small loop uploads multiple copies of the sample to create repeated triggers for the streaming job.
- Finally the cluster can be deleted to avoid ongoing costs.

## What the job scripts do (brief)

- `src/batch_flight_delays.py`: reads raw flights CSVs, cleans and enriches with airlines/airports data, writes cleaned Parquet (silver) partitioned by date, computes daily route aggregations (gold), and writes aggregations to BigQuery.
- `src/stream_flight_delays.py`: reads new CSV files from `gs://<BUCKET>/stream/flights_in/` as a streaming source, applies cleaning and enrichment, computes windowed metrics, writes outputs to console, Parquet and BigQuery with checkpointing.
