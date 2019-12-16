# -*- coding: utf-8 -*-
"""
Created on Sat Dec 14 21:15:31 2019

@author: klahrichi
"""

from pipeline.big_query_pipeline import BigQueryPipeline
import apache_beam as beam


class TestPipeline(BigQueryPipeline):

    def __init__(self):
        super(TestPipeline, self).__init__(job_name='job_name',
                                           project='bigquery_project',
                                           temp_location='gs://temp_location',
                                           input_table='dataset.input_table',
                                           output_table='dataset.output_table',
                                           output_schema='col1:STRING, col2:INTEGER, col3:INTEGER')

    def do(self, p):
        return (p | 'processing' >> beam.Map(lambda d: dict(d, col3=d['col1']+1)))


if __name__ == '__main__':
    p = TestPipeline()
    p.run()


