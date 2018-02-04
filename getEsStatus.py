#Add the following as a crontab once this script is available in $PATH 
#*/30 * * * * python /usr/local/bin/getESDataStatus.py >> /var/log/getESDataStatus.log
import json
import requests
import smtplib
import email.utils
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import time
import datetime
from datetime import datetime
import boto3
import urllib2

thresholdTimeSeconds = 20
thresholdTimeDays = 1
AWS_ACCESS_KEY_ID = "fill-in-your-aws-access-key-id"
AWS_SECRET_ACCESS_KEY = "fill-in-your-aws-secret-access-key"
domainName='name-of-the-aws-ES-domain'
region_response = requests.get('http://169.254.169.254/latest/meta-data/placement/availability-zone')
region = region_response.content[:-1]

def getESClsutersEndPoint():
    #region = 'us-east-1'
    ec2client = boto3.client('es',
                             aws_access_key_id=AWS_ACCESS_KEY_ID,
                             aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                             region_name=region
                             )
    response = ec2client.describe_elasticsearch_domains(
        DomainNames=[
            domainName,
        ]
    )
    return  response['DomainStatusList'][0].get("Endpoint")

def getESDataStatus():
    req_data = {"size": 1,"sort": { "time": "desc"},"_source": {"includes": "time"},"query": {"match_all": {}}}
    req_data_json = json.dumps(req_data)
    now_date = datetime.utcnow().strftime("%Y.%m.%d")
    endPoint = getESClsutersEndPoint()
    protocol = 'https://'
    es_url='%s%s' % (protocol,endPoint)
    url = '%s/kube-system-%s/_search' % (es_url,now_date)

    request = urllib2.Request(url,req_data_json)
    response = urllib2.urlopen(request)
    response_string = response.read()
    response_lines = response_string.split(",")
    for response_lines_time in response_lines:
        if 'time' in response_lines_time and 'timed_out' not in response_lines_time:
            time_string = response_lines_time[19:38]
            time_obj = datetime.strptime(time_string, '%Y-%m-%dT%H:%M:%S')
            print("Last updated time:%s" % (time_obj))
            if timeDiff(time_obj):
                print("Data error")
                sendEmail("Elastic serach cluster data not up to date, region:%s" % (region))
            else:
                print("Data up to date, region:%s" % (region))


def timeDiff(lastInsertedTime):
    nowtime = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    print("Time now:%s" % (nowtime))
    lastModifiedTime = lastInsertedTime.strftime("%Y-%m-%dT%H:%M:%S")
    FMT = '%Y-%m-%dT%H:%M:%S'
    different = datetime.strptime(nowtime, FMT) - datetime.strptime(lastModifiedTime, FMT)
    print("Time different between last inserted raw with now in seconds:%s" % (different.seconds))
    if (int(different.seconds)) > thresholdTimeSeconds:
        return 1;
    else:
        return 0;


def sendEmail(content):

    sender = 'roshane.ishara@gmail.com'
    sendername = 'AWS Elastic Search Data Monitor'

    recipients  = ['place-the-email-of-the-recepient']


    smtp_username = "aws-ses-username"
    smtp_password = "aws-ses-password"
    host = "email-smtp.us-east-1.amazonaws.com"
    port = 465

    subject = 'ES Data Monitoring ('+ time.strftime("%c")+")"
    text = '\r\n'.join([
        "ES Cluster Alerts",
        """This email was sent through the Amazon SES SMTP Interface using the Python smtplib package."""
    ])

    html = '\n'.join([
        "<html>",
        "<head></head>",
        "<body>",
        content
        ,
        "</body>",
        "</html>"
    ])

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = email.utils.formataddr((sendername, sender))
    msg['To'] = ','.join(recipients)

    part1 = MIMEText(text, 'plain')
    part2 = MIMEText(html, 'html')

    msg.attach(part1)
    msg.attach(part2)

    try:
        server = smtplib.SMTP_SSL(host, port)
        server.ehlo()
        server.login(smtp_username, smtp_password)
        server.sendmail(sender, recipients, msg.as_string())
        server.close()
    except Exception as e:
        print ("Error in sending email: ", e)
    else:
        print ("Email sent!")

getESDataStatus()

