from google.cloud import bigquery
from google.oauth2 import service_account
credentials = service_account.Credentials.from_service_account_file(
    '/home/aelamb/Documents/keys/neoepitopes-14aeae457a9d.json')
project_id = "neoepitopes"
client = bigquery.Client(credentials=credentials, project=project_id)


query_job = client.query("SELECT * FROM test.MHC_Binding_Assay LIMIT 10")
results = query_job.result()  # Waits for job to complete.
print(results)

'''
ALL_TABLES = [
    "MHC_Binding_Assay",
    "TCR_Binding_by_Flow_I",
    "TCR_Binding_by_Flow_II",
    "TCR_Binding_by_Microfluidics",
    "MS_Peptide_IDs",
    "T_Cell_Reactivity_Screen"]

sql = "SELECT * FROM test.MHC_Binding_Assay LIMIT 10"

df = pd.read_gbq(sql, "neoepitopes")

print(df)
'''
