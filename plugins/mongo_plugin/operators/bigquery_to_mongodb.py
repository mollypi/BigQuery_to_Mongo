#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from typing import Any, Optional, List

from airflow.models import BaseOperator, Variable
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

class BigQueryToMongoDB(BaseOperator):
    """Operator to move data incrementaly or not from bigquery to mongodb.

    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :type gcp_conn_id: str
    :param bq_dataset_id: The dataset ID of the requested table.
    :type bq_dataset_id: str
    :param bq_table_id: The table ID of the requested table. (templated)
    :type bq_table_id: str
    :param bq_selected_fields: *(optional)* List of fields to return (comma-separated). 
        If unspecified, all fields are returned.
    :type bq_selected_fields: str
    :param max_results: The maximum number of records (rows) to be fetched
    :type max_results: int
    :param mongo_conn_id: The connection ID used to connect mongodb
    :type mongo_conn_id: str
    :param mongo_db: *(optional)* Reference to a specific mongo database,
        if not None, its value is getted from the configured connection schema
    :type mongo_db: str
    :param mongo_collection: Reference to a specific collection in your mongo db
    :type mongo_collection: str
    :param mongo_method: The method to push records into mongo. 
        Possible values for this include:
            - insert
            - replace
    :type mongo_method: str
    :param mongo_replacement_filter: *(optional)* If choosing the replace method, 
        this indicates the filter to determine which records to replace. This may be set 
        as either a string or dictionary. If set as a string, the operator will view that 
        as a key and replace the record in Mongo where the value of the key in the existing 
        collection matches the incomingvalue of the key in the incoming record. If set as a 
        dictionary, the dictionary will be passed in normally as a filter.
    :type mongo_replacement_filter: str/dict
    :param mongo_upsert: *(optional)* If choosing the replace method,
        this is used to indicate whether records should be upserted.
    :type mongo_upsert: bool
    """

    template_fields = (
        'bq_dataset_id',
        'bq_table_id',
        'max_results',
        'mongo_collection',
        )
    ui_color = '#589636'
    template_fields_renderers = {}

    def __init__(self, *, 
        gcp_conn_id: str = 'google_cloud_default',
        bq_dataset_id: str,
        bq_table_id: str,
        bq_selected_fields: Optional[str] = None,
        bq_next_index_variable: Optional[str] = None,
        max_results: int = 100,
        mongo_conn_id: str = 'mongo_default', 
        mongo_db: Optional[str] = None, 
        mongo_collection: str, 
        mongo_method: str,
        mongo_replacement_filter: Optional[Any] = None,
        mongo_upsert: Optional[bool] = False,
        **kwargs,
        ):

        super().__init__(**kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.bq_dataset_id = bq_dataset_id
        self.bq_table_id = bq_table_id
        self.bq_selected_fields = bq_selected_fields
        self.bq_next_index_variable = bq_next_index_variable
        self.bq_next_index = int(Variable.get(self.bq_next_index_variable), 0)
        self.max_results = int(max_results)
        self.mongo_conn_id = mongo_conn_id
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection
        self.mongo_method = mongo_method
        self.mongo_replacement_filter = mongo_replacement_filter
        self.mongo_upsert = mongo_upsert

        if self.mongo_method not in ('insert', 'replace'):
            raise Exception('Please specify either "insert" or "replace" for Mongo method.')

        if (self.mongo_upsert is not True and self.mongo_upsert is not False):
            raise Exception('Upsert must be specified as a boolean. Please choose True/False.')
        
        if self.mongo_method == 'replace' and not self.mongo_replacement_filter:
            raise Exception('Please specify the replacement filter when using replace.')
        
        self.bq_hook = BigQueryHook(bigquery_conn_id=self.gcp_conn_id)
        self.mongo_hook = MongoHook(self.mongo_conn_id)

    def execute(self, context) -> bool:
        """Is written to depend on transform method"""

        big_query_data = self.get_big_query_data()
        mongo_doc = self.transform(big_query_data)
        self.method_mapper(mongo_doc)

    def get_big_query_data(self):
        self.log.info(
            'Fetching %s rows from %s.%s starting at row %s', 
            self.max_results,
            self.bq_dataset_id, 
            self.bq_table_id, 
            self.bq_next_index
        )

        rows = self.bq_hook.list_rows(
            start_index=self.bq_next_index,
            dataset_id=self.bq_dataset_id,
            table_id=self.bq_table_id,
            selected_fields=self.bq_selected_fields,
            max_results=self.max_results,
        )

        n_rows = len(rows)
        next_index = self.bq_next_index + n_rows
        self.log.info('Total extracted rows: %s', n_rows)
        Variable.set(self.bq_next_index_variable, next_index)

        return [dict(row) for row in rows]

    def method_mapper(self, docs:List[dict]):
        if self.mongo_method == 'insert':
            self.insert_into_mongo(docs)
        else:
            self.replace_into_mongo(docs)

    def insert_into_mongo(self, docs:List[dict]):
    
        self.log.info('Inserting %s docs with filter', 
            len(docs)
        )

        self.mongo_hook.insert_many(
            mongo_collection=self.mongo_collection,
            docs=docs,
            mongo_db=self.mongo_db
        )

    def replace_into_mongo(self, docs:List[dict]):
        replacement_filter = []
        if isinstance(self.mongo_replacement_filter, str):
            replacement_filter = [{self.mongo_replacement_filter: doc.get(self.mongo_replacement_filter, False)} for doc in docs]
        elif isinstance(self.mongo_replacement_filter, dict):
            replacement_filter = [self.mongo_replacement_filter for doc in docs]

        if self.mongo_upsert:
            self.log.info('Upserting %s docs with filter: %s', 
                    len(replacement_filter),
                    replacement_filter[0]
                )
        else:
            self.log.info('Replacing %s docs with filter: %s', 
                len(replacement_filter),
                replacement_filter[0]
            )

        self.mongo_hook.replace_many(
            mongo_collection=self.mongo_collection,
            docs=docs,
            mongo_db=self.mongo_db,
            filter_docs=replacement_filter,
            upsert=self.mongo_upsert
        )

    @staticmethod
    def transform(docs: list) -> list:
        """This method is meant to be extended by child classes
        to perform transformations unique to those operators needs.
        Override this method for custom transformations between the databases

        :param docs: list of objects (rows) fetched from bigquery
        :type docs: [{}]
        """
        return docs
