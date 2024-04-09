#!/usr/bin/env bash
#
#SBATCH -p fasse # partition (queue)
#SBATCH --mem 4000 # memory
#SBATCH -t 5-00:00 # time (D-HH:MM)
#SBATCH -o logs/download_resources_%j_output.out
#SBATCH --mail-type=END # notifications for job done
#SBATCH --account=somerville_lab
set -aeuxo pipefail

# conda activate hcpl
if [ -z "${CONDA_DEFAULT_ENV-}" ]; then
  set +x
  echo "Please run \`conda activate hcpl\` and try again."
  set -x
  exit 1
fi
cd /ncf/hcp/data/intradb

# Add a staggered sleep
#sleep $[ ( $RANDOM % 300 ) + 1 ]s
python /ncf/mclaughlin/users/jflournoy/code/intradb_sync/download_resources.py \
  --project CCF_HCD_STG \
  --collections "Structural_preproc" \
  $@

