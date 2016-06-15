# -*- coding: utf-8 -*-

"""
Parses md5 file
"""

import os
import logger_utils

logger = logger_utils.get_logger(__name__)
BPA_PREFIX = "102.100.100."


def get_base_metagenomics_data(md5_file):
    """
    Parse md5 file
    d33c76935c343df30572a2f719510eec  Sample_7910_1_PE_550bp_BASE_UNSW_H2TFJBCXX/7910_1_PE_550bp_BASE_UNSW_H2TFJBCXX_GAATTCGT-TATAGCCT_L001_R1_001.fastq.gz
    """

    data = []

    with open(md5_file) as f:
        for line in f.read().splitlines():
            line = line.strip()
            if line == '':
                continue

            file_data = {}
            md5, filepath = line.split()
            file_data['md5'] = md5

            filename = os.path.basename(filepath)
            no_extentions_filename = filename.split('.')[0]
            parts = no_extentions_filename.split('_')

            if len(parts) == 11:
                # UniqueID_extraction_library_insert-size_BASE_facility code_FlowID_Index_Lane_F1/R1
                bpa_id, extraction_id, library, insert_size, _, facility, flowcell, index, lane, run_num, run_id = parts

                file_data['target'] = "metagenomics"
                file_data['filename'] = filename
                file_data['bpa_id'] = BPA_PREFIX + bpa_id
                file_data['extraction_id'] = extraction_id
                file_data['facility'] = facility
                file_data['library'] = library
                file_data['insert_size'] = insert_size
                file_data['flowcell'] = insert_size
                file_data['index'] = index
                file_data['lane'] = lane
                file_data['run'] = run_num
                file_data['run_id'] = run_id
            else:
                logger.error('Ignoring line {} from {} with missing data'.format(filename, md5_file))
                continue

            data.append(file_data)

    return data
