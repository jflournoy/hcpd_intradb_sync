#!/usr/bin/env bash
#
#SBATCH -p ncf # partition (queue)
#SBATCH --mem 8000 # memory
#SBATCH --account somerville_lab
#SBATCH -t 3-2:00 # time (D-HH:MM)
#SBATCH -o logs/download_resources_%j_output.out
#SBATCH --mail-type=END # notifications for job done
set -aeuxo pipefail


# conda activate hcpl
if [ -z "${CONDA_DEFAULT_ENV-}" ]; then
  set +x
  echo "Please run \`conda activate hcpl\` and try again."
  set -x
  exit 1
fi

scriptdir=`pwd -P`
#cd /ncf/hcp/data/intradb
targetdir="/net/holynfs01/srv/export/ncf_hcp/share_root/data/intradb_t1w_dicom"

if [ ! -d "${targetdir}" ]; then
	mkdir -pv "${targetdir}"
fi
cd "${targetdir}"

# Add a staggered sleep
sleep $[ ( $RANDOM % 300 ) + 1 ]s
python "${scriptdir}"/download_resources.py -p CCF_HCD_STG -c T2w_setter -t T2w_setter -C NIFTI --like-itk $@

