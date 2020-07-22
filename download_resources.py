#!/usr/bin/env python
# Download Resources for a project

import argparse
from copy import copy
from hashlib import md5
import logging
import os
import re
import sys
import time

import yaxil
from yaxil.exceptions import RestApiError
from requests.exceptions import ConnectionError

logging.basicConfig(level=logging.DEBUG)

logger = logging.getLogger(__name__)
MAX_RETRIES = 3


def main(project, collections, ignore_list=None, subjects=None, sessions=None):
    if not ignore_list:
        ignore_list = list()
    if not subjects:
        subjects = list()
    if not sessions:
        sessions = list()
    auth = yaxil.auth('intradb')  # Requires setup and description
    start_time = time.time()
    with yaxil.session(auth) as sess:
        if not sessions:
            experiments = fetch_experiments(sess, project, subjects)

        for exp_info in [e._asdict() for e in experiments]:
            try:
                fetch_experiment(sess, collections, exp_info, ignore_list)
            except Exception as err:
                logger.error('Error with subject {}'.format(exp_info['label']))
                continue
    elapsed_time = time.time() - start_time
    logger.info('Finished {} experiments in {}'.format(
        len(experiments), time.strftime("%H:%M:%S",
                                        time.gmtime(elapsed_time))))


def fetch_experiments(sess, project, subject_labels):
    """Fetch a list of yaxil.Experiment's (or get all for a project)."""
    logger.info('Fetching list of experiments')
    if len(subject_labels):
        experiments = []
        try:
            for label in subject_labels:
                sub = list(sess.subjects(label=label, project=project))[0]
                experiments.extend(sess.experiments(subject=sub))
        except Exception as err:
            print('Error with subject {}'.format(label))
            print(err)
    else:
        experiments = list(sess.experiments(project=project))
    logger.info('Found {} experiments'.format(len(experiments)))
    return experiments

def fetch_experiment(sess, collections, exp_info, ignore_list):
    logger.info('Syncing experiment {}'.format(exp_info['label']))
    start_time = time.time()

    # auth = yaxil.XnatAuth(url='...', username='...', password='...')

    # url = 'data/experiments/HCPIntradb02_E08367/files'  # ?format=json'
    # exp_info = dict(
    #     project='CCF_HCD_STG',
    #     subject_label='HCD0021614',
    #     label='HCD0021614_V1_MR')  # experiment_label

    try:
        resources = fetch_resources(sess, exp_info, collections)
    except ValueError:
        logger.error('Unrecoverable error in {}'.format(exp_info['label']))
        resources = []

    resource_errors = []

    for resource in resources:
        try:
            fetch_resource(sess, exp_info, resource, always_checksum=True, ignore_list=ignore_list)
        except ValueError as err:
            if 'No JSON object could be decoded' in err.message:
                logger.error(err)
                continue
            else:
                raise
        except ConnectionError as err:
            logger.error(err)
            resource_errors.append('ConnectionError: {}'.format(
                exp_info['label']))
            continue

    with open('errors.log', 'a') as f:
        f.writelines(resource_errors)
    elapsed_time = time.time() - start_time
    logger.info('Finished experiment {} in {}'.format(
        exp_info['label'], time.strftime("%H:%M:%S",
                                         time.gmtime(elapsed_time))))

    # import pdb
    # pdb.set_trace()
    # url_pat = ('data/projects/{project}/subjects/{subject_label}/experiments/'
    #            '{label}/files')
    #
    # base_url = url_pat.format(**exp_info)
    # logger.info('Syncing {}'.format(base_url))

    # import pdb
    # pdb.set_trace()
    # # sess.get(base_url, yaxil.Format.JSON)
    # _, response = yaxil._get(sess._auth, base_url, yaxil.Format.JSON)
    #
    # for collection in collections:
    #     import pdb
    #     pdb.set_trace()
    #     logger.info('Downloading {}'.format(collection))
    #     filelist = [
    #         r for r in response['ResultSet']['Result']
    #         if r['collection'] == collection
    #     ]


def fetch_resources(sess, exp_info, collections=None):
    """Fetch a list of json resources with collection label and id."""

    resources_url_pat = ('data/projects/{project}/subjects/{subject_label}/'
                         'experiments/{label}/resources')
    base_url = resources_url_pat.format(**exp_info)
    _, response = yaxil._get(sess._auth, base_url, yaxil.Format.JSON)

    # Filter only wanted collections or return all
    if collections:
        resources = [
            result for result in response['ResultSet']['Result']
            if result['label'] in collections
        ]
    else:
        resources = response['ResultSet']['Result']

    if not len(resources):
        msg = 'Found no resources '
        if collections:
            msg += 'matching collections {}'.format(collections)
        msg += 'for {}'.format(resources_url_pat)
        logger.warning(msg)

    return resources


def fetch_resource(sess, exp_info, resource_info, always_checksum=False, ignore_list=None):
    if not ignore_list:
        ignore_list = list()
    # Use a cookie to mark a resource as complete
    success_cookie = os.path.join(exp_info['label'], resource_info['label'],
                                  'SUCCESS')
    if os.path.exists(success_cookie) and not always_checksum:
        return

    resource_url_pat = (
        'data/projects/{project}/subjects/{subject_label}/'
        'experiments/{label}/resources/{xnat_abstractresource_id}/files')
    url_info = copy(exp_info)  # Combine resource and experiment
    url_info['xnat_abstractresource_id'] = resource_info[
        'xnat_abstractresource_id']

    base_url = resource_url_pat.format(**url_info)
    _, response = yaxil._get(sess._auth, base_url, yaxil.Format.JSON)

    filelist = response['ResultSet']['Result']
    if not len(filelist):
        raise ValueError('No files could be read from {} in json response: {}'.format(base_url, response))
    logger.info('Syncing {} file (resources) from {}'.format(len(filelist), base_url))

    start_time = time.time()
    for fileinfo in filelist:
        try:
            # Rename URI (python variable case)
            fileinfo['uri'] = fileinfo.pop('URI')
            if ignore_file(fileinfo['uri'], ignore_list):
                logger.debug('Ignoring {}'.format(fileinfo['uri']))
                continue
            download_file(sess, out_dir=exp_info['label'], **fileinfo)
        except RuntimeError:
            logger.info('Digest failed on {}'.format(fileinfo['uri']))
        except RestApiError as err:
            logger.info('Download Error on {}: {}'.format(
                fileinfo['uri'], err))

    elapsed_time = time.time() - start_time
    logger.info('Finished in {}'.format(
        time.strftime("%H:%M:%S", time.gmtime(elapsed_time))))

    # if we got here, mark this collection as completed ("touch" cookie)
    open(success_cookie, 'w').close()


def download_file(sess,
                  uri,
                  digest,
                  collection,
                  out_dir='.',
                  overwrite=False,
                  **kwargs):
    basename = uri.split('files/')[-1]
    fname = os.path.join(out_dir, collection, basename)
    dirname = os.path.dirname(fname)

    if not os.path.exists(dirname):
        os.makedirs(dirname)
    if os.path.exists(fname):
        with open(fname, 'rb') as f:
            disk_digest = md5(f.read()).hexdigest()
        if disk_digest == digest:
            logger.debug('Digest matched - Skipping {}'.format(fname))
            return
        elif overwrite:
            # import pdb; pdb.set_trace()
            logger.info(
                'Digest failed - removing {} and trying again'.format(fname))
            os.remove(fname)
        else:
            raise RuntimeError(
                '{} exisited with incorrect digest '.format(fname) +
                'but cowardly moving on')

    try:
        _, result = yaxil._get(
            sess._auth,
            uri,
            yaxil.Format.JSON,  # Format is ignored for _file_ downloads
            autobox=False)
    except RestApiError as err:
        # Empty responses are acceptable for some scripts and onset files
        if 'response is empty' in err.message:
            result = bytes('')
        else:
            raise

    with open(fname, 'wb') as f:
        f.write(result)

    with open(fname, 'rb') as f:
        disk_digest = md5(f.read()).hexdigest()
    if disk_digest != digest:
        retries = kwargs.get('retries', 0)
        if overwrite:
            os.remove(fname)
        if retries >= MAX_RETRIES:
            raise RuntimeError(
                'Digest failed - ' +
                '{} may need to be re-downloaded.'.format(fname))
        else:
            retries += 1
            download_file(sess,
                          uri,
                          digest,
                          collection,
                          out_dir,
                          overwrite=True,
                          retries=retries)


def ignore_file(uri, ignores_list):
    ignore = False
    for ignore_pat in ignores_list:
        if re.search(ignore_pat, uri):
            ignore = True
    return ignore


def parse_args(args):
    parser = argparse.ArgumentParser(description='Download from Remote XNAT')
    parser.add_argument('--project', '-p', type=str)
    parser.add_argument('--collections', '-c', type=str, nargs='+')
    parser.add_argument('--ignore-list', nargs='+', default=['OTHER_FILES'])
    parser.add_argument('--subjects', '-s', nargs='+', default=[], help='Explicit list of subjects')
    parser.add_argument('--sessions', nargs='+', default=[], help='Explicit list of sessions')

    return parser.parse_args(args)

if __name__ == '__main__':
    # usage: ./download_resources.py CCF_HCD_STG
    args = parse_args(sys.argv[1:])

    # collections = [
    #     #'Diffusion_unproc',
    #     #'mbPCASLhr_unproc',
    #     # 'rfMRI_REST1_AP_unproc',
    #     # 'rfMRI_REST1_PA_unproc',
    #     # 'rfMRI_REST2_AP_unproc',
    #     # 'rfMRI_REST2_PA_unproc',
    #     'Structural_preproc',
    #     #       'T1w_MPR_vNav_4e_RMS_unproc',
    #     #       'T2w_SPC_vNav_unproc',
    #     #       'tfMRI_GUESSING_PA_unproc',
    #     #       'tfMRI_GUESSING_AP_unproc',
    #     #       'tfMRI_CARIT_AP_unproc',
    #     #       'tfMRI_CARIT_PA_unproc',
    #     #       'tfMRI_EMOTION_PA_unproc',
    # ]

    opts = vars(args)
    main(**opts)
