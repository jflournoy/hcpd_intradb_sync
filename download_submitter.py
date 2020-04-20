#!/usr/bin/env python
# 2019-12-03 EKK
# Split and submit a set of subjects to slurm to download from HCD
import argparse
import csv
import os
# from shlex import join
from random import shuffle
from subprocess import check_output
import sys
import time
# import pandas as pd

HARVARD_SCANNER = 'AWP67056'


def main(subject_csv, scriptloc, nprocs=4, slurm=True, scanners=[HARVARD_SCANNER]):
    """Read an xnat csv, split the subject list, and submit dl to slurm."""

    sublist = read_subjects(subject_csv, scanners=scanners)
    print('Submitting {} subjects for download:'.format(len(sublist)))

    for part in split(sublist, nprocs):
        if slurm:
            cmd = ['sbatch']
        else:
            cmd = []  # Run Locally

        cmd += [
            os.getcwd() + '/download.sh',
            ' '.join(part),
        ]
        print(cmd)
        print(' '.join(cmd))
        print(check_output(cmd))


def read_subjects(subject_csv, scanners=[], do_shuffle=True):
    """Read an xnat exported csv to create a subject list of Harvard Subs."""
    sublist = []
    with open(subject_csv, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if 'Subject' in row:
                subject_key = 'Subject'
                split_base = False
            elif 'MR ID' in row:
                subject_key = 'MR ID'
                split_base = True
            else:
                raise ValueError("Couldn't guess subject key.")

            subject = row[subject_key]
            if split_base:
                subject = subject.split('_')[0]

            # Filter
            if len(scanners):
                for scanner in scanners:
                    if row['Scanner'] == scanner:
                        sublist.append(subject)
            else:
                if not subject.startswith('HC'):
                    raise "Unexpected subject {}".format(subject)
                sublist.append(subject)
    # Remove any possible duplicates
    sublist = list(set(sublist))

    # Shuffle order for re-run to distribute completed subjects
    if do_shuffle:
        shuffle(sublist)

    return sublist


def split(a, n):
    # https://stackoverflow.com/a/2135920
    k, m = divmod(len(a), n)
    return (a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)]
            for i in xrange(n))


def parse_args(args):
    parser = argparse.ArgumentParser(description='Manage Downloads')
    parser.add_argument('--sheet', type=argparse.FileType('r'))
    parser.add_argument('--n-procs', type=int, default=1)
    parser.add_argument('--use-slurm', action='store_true')
    parser.add_argument('--scanners', type=list, help='Limit to only the listed scanner serial numbers')

    return parser.parse_args(args)

if __name__ == '__main__':
    #developed mainly using hcpl environment on the NRG : conda activate hcpl
    # Usage: download_submitter.py xnat_export-kastman_12_3_2019_11_56_1.csv
    args = parse_args(sys.argv)
    # modify script location here
    scriptloc = os.getcwd() + '/download.sh'
    main(sys.argv[1], scriptloc, nprocs=1, slurm=False, scanners=[])
