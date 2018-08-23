
"""
Docstring.
"""
import synapseclient
import pandas as pd
import validation_proccessing as vp
from pandas.io import gbq
from functools import partial

input_synapse_id = "syn13006337"


syn = synapseclient.Synapse()
syn.login('andrew.lamb@sagebase.org', 'sageBlam1979')


def insert_validation_table(input_synapse_id, table, output_columns,
                            input_columns=None, dataset="test",
                            project="neoepitopes"):
    """Download synapse file, process, and insert into bigquery table."""
    if input_columns is None:
        input_columns = output_columns
    entity = syn.get(input_synapse_id)
    df = pd.read_csv(entity.path)
    df = vp.process_validation_table(df, input_columns, output_columns)
    table_name = ".".join([dataset, table])
    gbq.to_gbq(df, table_name, project, if_exists='append')


insert_binding_assay_results_table = partial(
    insert_validation_table,
    output_columns=[
        "PATIENT", "ALT_EPI_SEQ", "HLA_ALLELE", "BINDING_ASSAY_RESULT"],
    table="Binding_assay_results"
)


insert_binding_assay_results_table(
    input_synapse_id,
    input_columns=["PATIENT", "ALT_EPI_SEQ", "HLA_ALLELE", "LJI_BINDING"])
