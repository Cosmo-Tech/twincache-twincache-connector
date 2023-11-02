# Copyright (c) Cosmo Tech corporation.
# Licensed under the MIT license.
import logging
import os
import csv

from CosmoTech_Acceleration_Library.Modelops.core.io.model_exporter import ModelExporter
from CosmoTech_Acceleration_Library.Modelops.core.io.model_importer import ModelImporter

logger = logging.getLogger(__name__)

ST_DETECT = [('source', 'target'), ('src', 'dest')]
ID_DETECT = ['id', 'name']


class TwinCacheConnector:
    """
    Connector class to export data from twin cache solution into csv files
    """

    def __init__(self,
                 host: str,
                 port: int,
                 name: str,
                 password: str = None):
        self.host = host
        self.port = port
        self.name = name
        self.password = password

    def extract_to(self, export_path: str, queries: str):
        """
        Extract data from twin cache solution into csv files
        :param export_path: path where to export the data
        :param queries: cypher queries to filter the data to export
        """
        exporter = ModelExporter(self.host,
                                 self.port,
                                 self.name,
                                 self.password,
                                 export_path)
        if queries:
            logger.info(f'Running sub queries extract: {queries}')
            exporter.export_from_queries(queries)
        else:
            logger.info(f"Running full extract")
            exporter.export_all_data()

    def import_from(self, export_path: str):
        """
        Import data from csv files into twin cache solution
        :param export_path: path where to export the data
        """
        twins = []
        rels = []
        errors = []
        for r, d, files in os.walk(export_path):
            for filz in files:
                file_path = os.path.join(r, filz)
                output_file_path = os.path.join(r, 'output', filz)
                os.makedirs(os.path.join(r, 'output'), exist_ok=True)
                with open(file_path) as f, open(output_file_path, 'w') as out:
                    csv_r = csv.DictReader(f)
                    new_header = csv_r.fieldnames

                    st_m = list(map(lambda st: st[0] in new_header and st[1] in new_header, ST_DETECT))
                    if any(st_m):
                        print(f'{filz} is rel')
                        if sum(st_m) > 1:
                            errors.append(f'{filz} detected as relationship has more than one pair of (source, target)')
                            continue
                        _src, _dest = [val for use, val in zip(st_m, ST_DETECT) if use][0]
                        new_header.insert(0, new_header.pop(new_header.index(_src)))
                        new_header.insert(1, new_header.pop(new_header.index(_dest)))
                        rels.append(output_file_path)
                    else:
                        print(f'{filz} is twin')
                        id_m = list(map(lambda i: i in new_header, ID_DETECT))
                        if sum(id_m) > 1:
                            errors.append(f'{filz} detected as twin has more than one id')
                            continue
                        _id = [val for use, val in zip(id_m, ID_DETECT) if use][0]
                        new_header.insert(0, new_header.pop(new_header.index(_id)))
                        twins.append(output_file_path)

                    csv_w = csv.DictWriter(out, new_header)
                    csv_w.writeheader()
                    csv_w.writerows(csv_r)

        if len(errors) > 0:
            print('Following errors has been detected:')
            for e in errors:
                print(e)
            raise Exception('Errors detected in source files. import has been canceled.')
        importer = ModelImporter(host=self.host, port=self.port,
                                 name=self.name,
                                 password=self.password)
        importer.bulk_import(twins, rels)
