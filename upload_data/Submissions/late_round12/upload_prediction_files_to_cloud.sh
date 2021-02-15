gsutil mv late_12*.csv  gs://version_4
bq load --null_marker=NA --skip_leading_rows=1 --source_format=CSV Version_4.Protein_Positions gs://version_4/late_12_protein_position_df.csv
bq load --null_marker=NA --skip_leading_rows=1 --source_format=CSV Version_4.Predictions gs://version_4/late_12_prediction_df.csv
bq load --null_marker=NA --skip_leading_rows=1 --source_format=CSV Version_4.Bad_Predictions gs://version_4/late_12_bad_prediction_df.csv
bq load --null_marker=NA --skip_leading_rows=1 --source_format=CSV Version_4.Prediction_Variants gs://version_4/late_12_variant_prediction_df.csv
