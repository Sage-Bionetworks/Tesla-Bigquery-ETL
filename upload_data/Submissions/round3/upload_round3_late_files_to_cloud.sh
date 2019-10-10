gsutil mv round3*.csv  gs://version_3
bq load --null_marker=NA --skip_leading_rows=1 --source_format=CSV Version_3.Protein_Positions gs://version_3/round3_late_protein_positions.csv
bq load --null_marker=NA --skip_leading_rows=1 --source_format=CSV Version_3.Predictions gs://version_3/round3_late_predictions.csv
bq load --null_marker=NA --skip_leading_rows=1 --source_format=CSV Version_3.Bad_Predictions gs://version_3/round3_late_bad_predictions.csv
bq load --null_marker=NA --skip_leading_rows=1 --source_format=CSV Version_3.Prediction_Variants gs://version_3/round3_late_prediction_variants.csv
bq load --null_marker=NA --skip_leading_rows=1 --source_format=CSV Version_3.Variants gs://version_3/round3_late_variants.csv
