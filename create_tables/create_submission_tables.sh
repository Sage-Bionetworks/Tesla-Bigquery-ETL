bq mk --table Version_3.Protein_Positions json/Protein_Positions.json
bq mk --table Version_3.Prediction_Variants json/Prediction_Variants.json
bq mk --table Version_3.Predictions json/Predictions.json
bq mk --table Version_3.Bad_Predictions json/Predictions.json
bq mk --table Version_3.Steps json/Steps.json
bq mk --table Version_3.Parameters json/Parameters.json
bq mk --table Version_3.Variants json/Variants.json
bq mk --table Version_3.Submissions json/Submissions.json

bq mk --table Version_3.Teams json/Teams.json
bq mk --table Version_3.Survey_Answers json/Survey_Answers.json
bq mk --table Version_3.Survey_Questions json/Survey_Questions.json
