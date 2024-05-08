from flask import Flask,request,jsonify
import json
import datetime
# import uuid
import psycopg2
from psycopg2 import sql

dbname = "postgres"
user = "postgres"
password = "4lQ}EI|2<7lEfjbP+[3Bvy|7vYU5"
host = "synergi-db-instance-1.cegvievfjkqm.us-east-1.rds.amazonaws.com"
port = "5432"
table = "email_reporting"

# Function to check if a table exists
def table_exists(table_name):
    cur.execute(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = %s
        );
        """,
        (table_name,)
    )
    return cur.fetchone()[0]

# Function to create the table if it doesn't exist
def create_table_if_not_exists(table_name):
    if not table_exists(table_name):
        cur.execute(
            sql.SQL(
                """
                CREATE TABLE {} (
                    event_type VARCHAR(255),
                    brand_id VARCHAR(255),
                    campaign_id VARCHAR(255),
                    lead_id VARCHAR(255),
                    email_from VARCHAR(255),
                    email_to VARCHAR(255),
                    email_reply_to VARCHAR(255),
                    email_subject VARCHAR(255),
                    email_date DATE,
                    email_timestamp TIMESTAMP,
                    email_message_id VARCHAR(255),
                    message_id VARCHAR(255),
                    destination VARCHAR(255),
                    source VARCHAR(255),
                    sending_account_id VARCHAR(255),
                    recipient_isp VARCHAR(255),
                    configuration_set VARCHAR(255),
                    ses_operation VARCHAR(255),
                    ses_from_domain VARCHAR(255),
                    ses_caller_identity VARCHAR(255),
                    ses_ip_address VARCHAR(255),
                    delivery_processing_time VARCHAR(255),
                    delivery_recipients VARCHAR(255),
                    delivery_time TIMESTAMP,
                    open_ip VARCHAR(255),
                    open_agent VARCHAR(255),
                    open_time TIMESTAMP,
                    click_ip VARCHAR(255),
                    click_link VARCHAR(255),
                    click_tags VARCHAR(255),
                    click_agent VARCHAR(255),
                    click_time TIMESTAMP,
                    complaint_feedback_id VARCHAR(255),
                    complaint_feedback_type VARCHAR(255),
                    complaint_arrival_date TIMESTAMP,
                    complaint_agent VARCHAR(255),
                    complaint_time VARCHAR(255),
                    bounce_feedback_id VARCHAR(255),
                    bounce_type VARCHAR(255),
                    bounce_sub_type VARCHAR(255),
                    bounce_time TIMESTAMP,
                    bounce_action VARCHAR(255),
                    bounce_diagnostic_code VARCHAR(255),
                    reject_reason VARCHAR(255)
                );
                """
            ).format(sql.Identifier(table_name))
        )
        conn.commit()
        print(f"Table '{table_name}' created successfully.")
    else:
        print(f"Table '{table_name}' already exists.")

# Create the table if it doesn't exist
# create_table_if_not_exists(table)

# def write_data_to_files(data):
#     # Generate unique filenames using timestamps
#     timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
#     uniq = uuid.uuid4()
#     filenames = f'{uniq}_{timestamp}.json'
#     with open(filenames, 'w') as file:
#         file.write(data)
#         print(f'Written data to {filenames}')
app = Flask(__name__)

# Connection to PostgreSQL database
# Connection string
conn_string = f"dbname='{dbname}' user='{user}' password='{password}' host='{host}' port='{port}'"

# Connect to the PostgreSQL database
conn = psycopg2.connect(conn_string)
cur = conn.cursor()

def execute_query(query, params):
    cur.execute(query, params)
    conn.commit()

def create_insert_query(table_name,event_type,data):
    base_query = "INSERT INTO {table_name} ({fields}) VALUES ({values});"
    fields = []
    values = []
    placeholders = []

    # Dynamically create the list of fields, placeholders for values, and the values themselves
    for key, value in data.items():
        fields.append(key)
        placeholders.append('%s')
        values.append(value)

    # Construct the query string
    formatted_fields = ', '.join(fields)
    formatted_placeholders = ', '.join(placeholders)
    sql_query = base_query.format(table_name=table_name,fields=formatted_fields, values=formatted_placeholders)
    print(sql_query)
    print(values)
    # Execute the query
    execute_query(sql_query, values)

def get_header_value(headers, header_name):
    for header in headers:
        if header['name'] == header_name:
            return header['value']
    return None

def insert_webhook_data(table,webhook_data):
    event_type = webhook_data.get('eventType', 'Unknown')
    mail_info = webhook_data.get('mail', {})

    mail_tags = mail_info.get('tags', {})
    brand_id = mail_tags.get('brand_id', [None])[0]
    campaign_id = mail_tags.get('campaign_id', [None])[0]
    lead_id = mail_tags.get('lead_id', [None])[0]
    recipient_isp = mail_tags.get('ses:recipient-isp', [None])[0]
    configuration_set = mail_tags.get('ses:configuration-set', [None])[0]
    ses_operation = mail_tags.get('ses:operation', [None])[0]
    ses_from_domain = mail_tags.get('ses:from-domain', [None])[0]
    ses_caller_identity = mail_tags.get('ses:caller-identity', [None])[0]
    ses_ip_address = mail_tags.get('ses:source-ip', [None])[0]

    headers = mail_info.get('headers', [])
    email_from = get_header_value(headers, 'From')
    email_to = get_header_value(headers, 'To')
    email_reply_to = get_header_value(headers, 'Reply-To')
    email_subject = get_header_value(headers, 'Subject')
    email_date = get_header_value(headers, 'Date')
    email_message_id = get_header_value(headers, 'Message-ID')

    destination = mail_info.get('destination', [None])[0]
    email_timestamp = mail_info.get('timestamp', None)
    source = mail_info.get('source', None)
    sending_account_id = mail_info.get('sendingAccountId', None)
    message_id = mail_info.get('messageId', None)

    event_data = {
        "event_type": event_type.lower(),
        "brand_id": brand_id,
        "campaign_id": campaign_id,
        "lead_id": lead_id,
        "email_from": email_from,
        "email_to": email_to,
        "email_reply_to": email_reply_to,
        "email_subject": email_subject,
        "email_date": email_date,
        "email_timestamp": email_timestamp,
        "email_message_id": email_message_id,
        "message_id": message_id,
        "destination": destination,
        "source": source,
        "sending_account_id": sending_account_id,
        "recipient_isp": recipient_isp,
        "configuration_set": configuration_set,
        "ses_operation": ses_operation,
        "ses_from_domain": ses_from_domain,
        "ses_caller_identity": ses_caller_identity,
        "ses_ip_address": ses_ip_address
    }

    # Event specific data
    event_details = {}
    if event_type == "Delivery":
        event_details = webhook_data.get('delivery', {})
        delivery_processing_time = event_details.get('processingTimeMillis', None)
        delivery_recipients = event_details.get('recipients', [None])[0]
        delivery_time = event_details.get('timestamp', None)\
        
        event_data.update({
            "delivery_processing_time": str(delivery_processing_time),
            "delivery_recipients": delivery_recipients,
            "delivery_time": delivery_time
        })
    elif event_type == "Open":
        event_details = webhook_data.get('open', {})
        open_ip = event_details.get('ipAddress', None)
        open_agent = event_details.get('userAgent', None)
        open_time = event_details.get('timestamp', None)

        event_data.update({
            "open_ip": open_ip,
            "open_agent": open_agent,
            "open_time": open_time
        })
    elif event_type == "Click":
        event_details = webhook_data.get('click', {})
        click_ip = event_details.get('ipAddress', None)
        click_link = event_details.get('link', None)
        click_tags = event_details.get('linkTags', None)
        if click_tags:
            click_tags = ', '.join(f"{key}: {'/'.join(values)}" for key, values in click_tags.items())
        click_agent = event_details.get('userAgent', None)
        click_time = event_details.get('timestamp', None)

        event_data.update({
            "click_ip": click_ip,
            "click_link": click_link,
            "click_tags": click_tags,
            "click_agent": click_agent,
            "click_time": click_time
        })
    elif event_type == "Complaint":
        event_details = webhook_data.get('complaint', {})
        complaint_feedbackid = event_details.get('feedbackId', None)
        complaint_feedback_type = event_details.get('complaintFeedbackType', None)
        complaint_arrival_date = event_details.get('arrivalDate', None)
        complaint_agent = event_details.get('userAgent', None)
        complaint_time = event_details.get('timestamp', None)

        event_data.update({
            "complaint_feedback_id": complaint_feedbackid,
            "complaint_feedback_type": complaint_feedback_type,
            "complaint_arrival_date": complaint_arrival_date,
            "complaint_agent": complaint_agent,
            "complaint_time": complaint_time
        })
    elif event_type == "Bounce":
        event_details = webhook_data.get('bounce', {})
        bounce_feedbackid = event_details.get('feedbackId', None)
        bounce_type = event_details.get('bounceType', None)
        bounce_sub_type = event_details.get('bounceSubType', None)
        bounce_time = event_details.get('timestamp', None)
        bounced = event_details.get('bouncedRecipients', {})
        bounce_action = bounced.get('action', None)
        bounce_diagnostic_code = bounced.get('diagnosticCode', None)

        event_data.update({
            "bounce_feedback_id": bounce_feedbackid,
            "bounce_type": bounce_type,
            "bounce_sub_type": bounce_sub_type,
            "bounce_time": bounce_time,
            "bounce_action": bounce_action,
            "bounce_diagnostic_code": bounce_diagnostic_code
        })
    elif event_type == "Reject":
        event_details = webhook_data.get('reject', {})
        reject_reason = event_details.get('reason', None)
        event_data.update({
            "reject_reason": reject_reason
        })

    print(table)
    print(event_data)
    # Call the function to execute the query
    create_insert_query(table,event_type, event_data)
    return True

@app.route("/")
def index():
    print("Hello from HB!")
    return "Hello from HB!"

@app.route('/webhook', methods=['POST'])
def webhook():
    # Log the data received for inspection
    print("Received webhook data:")
    # print(request.is_json)
    print(request.data)
    # Decode the bytes to string
    decoded_data = request.data.decode('utf-8')
    # Load the JSON data
    json_data = json.loads(decoded_data)
    # Extract the 'Message' field and parse it as JSON
    inner_message = json.loads(json_data['Message'])
    email_data = json.dumps(inner_message, indent=4)
    webhook_data = json.loads(email_data)
    print(webhook_data)
    result = insert_webhook_data(table,webhook_data)
    print(result)
    # write_data_to_files(final_data)
    return jsonify(success=True)

if __name__ == '__main__':
    app.run(host='0.0.0.0',port=5000)
