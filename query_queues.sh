challengeutils query \
"select objectId, auprc, team, createdOn,patientId from evaluation_9614044 where status == 'SCORED'" \
| tail -n +3 | head -n -1 > roundx_validation.csv

challengeutils query \
"select objectId, auprc, team, createdOn,patientId from evaluation_9614043 where status == 'SCORED'" \
| tail -n +3 | head -n -1 > roundx_testing.csv

challengeutils query \
"select objectId, team, createdOn, patientId from evaluation_9614042 where status == 'VALIDATED'"\
| tail -n +3 | head -n -1 > roundx_training.csv

challengeutils query \
"select objectId, team, createdOn, patientId, round, submissionName from evaluation_8116290 where status == 'VALIDATED'"\
| tail -n +3 | head -n -1 > round12.csv



