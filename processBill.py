import io, sys, json, tempfile, gzip
from datetime import datetime, timedelta
import boto3, pandas as pd, numpy as np
from tabulate import tabulate

ISLOCAL = True

if not ISLOCAL:
    old_stdout = sys.stdout
    sys.stdout = io.StringIO()

s3 = boto3.client('s3')
sns = boto3.client('sns')

DATEFORMAT = "%Y%m%d"
BUCKET_NAME= "gw.ops"
SNS_ARN = "arn:aws:sns:ap-south-1:975795997058:sns_MonthlyBillBreakup"
# costandusagereports/MyCostAndUsageReport/20231201-20240101/
REPORTPATHPREFIX = "costandusagereports/MyCostAndUsageReport"
MANIFESTFILENAME = 'MyCostAndUsageReport-Manifest.json'
COL_PRODUCTNAME = 'product/ProductName'
COL_PRODUCTFAMILY = 'product/productFamily'
COL_TAGPID = 'resourceTags/user:PID'
COL_LINEITEMID = "identity/LineItemId"
COL_COST = 'lineItem/UnblendedCost'

def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))

    endDate = datetime.utcnow().replace(day=1)
    startDate = (endDate - timedelta(days=1)).replace(day=1)    

    daterange = f"{startDate.strftime(DATEFORMAT)}-{endDate.strftime(DATEFORMAT)}"
    filePath = f"{REPORTPATHPREFIX}/{daterange}/"
    
    reportFileKey = ""; df = ""
    with tempfile.TemporaryFile() as f_manifest:
        manifestFileKey = filePath + MANIFESTFILENAME
        print(f"Downloading manifest file {manifestFileKey}", file=sys.stderr)        
        s3.download_fileobj(BUCKET_NAME, manifestFileKey, f_manifest)
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
            df = pd.read_csv(f, index_col=COL_LINEITEMID, low_memory=False).pivot_table(
                index=[COL_PRODUCTNAME, COL_PRODUCTFAMILY], columns=COL_TAGPID, 
                values=COL_COST, fill_value=0, aggfunc='sum',
                margins=True, dropna=False)

    # rename nan -to-> (blank)   This is the column where the PID is not tagged
    df.rename(columns = {np.nan:'(blank)'}, inplace = True)

    # manually add Sum for (blank) column. pivot does not do that automatically
    df['(blank)']['All'] = df['(blank)'].sum()

    for col in df.columns:
        if col == COL_PRODUCTNAME or col == "All":
            continue # with next column

        df1 = df[[col]].copy().round(2)
        df1.drop(df1[df1[col] == 0].index, inplace=True)
        if df1.shape[0] > 0:
            print("="*50)
            print(tabulate(df1, headers='keys', tablefmt="simple"))
        
        else:
            print(f"# {col} Insignificant", file=sys.stderr)
            print(f"{col} Insignificant")
        print()
    
    if not ISLOCAL:
        print("Pushing to SNS", file=sys.stderr)
        reportTxt = sys.stdout.getvalue()    
        response = sns.publish(TopicArn=SNS_ARN, Message=reportTxt, Subject=f"AWS Bill Breakup {daterange}")
        print(response, file=sys.stderr)