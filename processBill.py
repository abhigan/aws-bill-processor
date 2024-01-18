import json, tempfile, gzip
import boto3, pandas as pd, numpy as np
from tabulate import tabulate
import io, sys
from datetime import datetime, timedelta

old_stdout = sys.stdout
sys.stdout = io.StringIO()

s3 = boto3.client('s3')
sns = boto3.client('sns')
BUCKET_NAME= "gw.ops"

def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))

    endDate = datetime.utcnow().replace(day=1)
    startDate = (endDate - timedelta(days=1)).replace(day=1)    

    dateformat = "%Y%m%d"
    # daterange = "20231201-20240101"
    daterange = f"{startDate.strftime(dateformat)}-{endDate.strftime(dateformat)}"
    filePath = f"costandusagereports/MyCostAndUsageReport/{daterange}/"
    reportFileKey = ""

    with tempfile.TemporaryFile() as f_manifest:
        print("Downloading manifest file", file=sys.stderr)
        s3.download_fileobj(BUCKET_NAME, filePath + 'MyCostAndUsageReport-Manifest.json', f_manifest)
        print(f'{f_manifest.tell()} bytes received', file=sys.stderr)
        f_manifest.seek(0)
        h_manifest = json.load(f_manifest)

        reportFileKeys = h_manifest["reportKeys"]

        if len(reportFileKeys) > 1:
            raise "UNSUPPORTED: Multiple report files found"
        else:
            reportFileKey = reportFileKeys[0]
    
    with tempfile.TemporaryFile() as f_gzip:
        print(f"Downloading report file {reportFileKey}", file=sys.stderr)
        s3.download_fileobj(BUCKET_NAME, reportFileKey, f_gzip)        
        print(f'{f_gzip.tell()} bytes received', file=sys.stderr)
        f_gzip.seek(0)

        with gzip.open(f_gzip,'rt') as f:
            print("Reading into pivot table", file=sys.stderr)
            df = pd.read_csv(f, index_col="identity/LineItemId").pivot_table(
                index='product/ProductName', columns='resourceTags/user:PID', 
                values='lineItem/UnblendedCost', fill_value=0, aggfunc='sum',
                margins=True, dropna=False)

    # rename nan -to-> (blank)   This is the column where the PID is not tagged
    df.rename(columns = {np.nan:'(blank)'}, inplace = True)

    # manually add Sum for (blank) column. pivot does not do that automatically
    df['(blank)']['All'] = df['(blank)'].sum()

    for col in df.columns:
        if col == "product/ProductName" or col == "All":
            continue # with next column

        df1 = df[[col]].copy().round(2)
        df1.drop(df1[df1[col] == 0].index, inplace=True)
        if df1.shape[0] > 0:
            print(tabulate(df1, headers='keys', tablefmt="simple"))
        else:
            print(f"{col} Insignificant")
        print()
    
    print("Pushing to SNS", file=sys.stderr)
    reportTxt = sys.stdout.getvalue()
    arn = "arn:aws:sns:ap-south-1:975795997058:sns_MonthlyBillBreakup"
    response = sns.publish(TopicArn=arn, Message=reportTxt, Subject=f"AWS Bill Breakup {daterange}")
    print(response, file=sys.stderr)