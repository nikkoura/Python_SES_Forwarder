import boto3
import logging
import email

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamo_client = boto3.client('dynamodb')
s3_client = boto3.client('s3')
ses_client = boto3.client('ses')

configuration = dict(
    dynamodb_table='SesForwarder.mapping',
    s3_bucket='nikkoura.ses.mailstore',
    s3_folder='nikkoura-eu/')


def event_sanity_check(event, context):
    if not 'Records' in event or not 'ses' in event['Records'][0]:
        logger.error('Invalid event format')
        raise Exception('Invalid event format')


def recipients_mapper(event, context):
    logger.info('Original recipients: {}'.format(event['Records'][0]['ses']['receipt']['recipients']))

    new_recipients = []
    last_mapped_original_recipient = ''

    for current_original_recipient in event['Records'][0]['ses']['receipt']['recipients']:
        dynamo_result = dynamo_client.get_item(
            Key={'email': {'S': current_original_recipient}},
            TableName=configuration['dynamodb_table'],
            ProjectionExpression='destinations')

        if 'Item' in dynamo_result:
            for current_new_recipient in dynamo_result['Item']['destinations']['SS']:
                logger.debug(current_new_recipient)
                new_recipients.append(current_new_recipient)
            last_mapped_original_recipient = current_original_recipient

    if len(new_recipients) == 0:
        for current_original_recipient in event['Records'][0]['ses']['receipt']['recipients']:
            address_user, address_domain = current_original_recipient.split('@')
            dynamo_result = dynamo_client.get_item(
                Key={'email': {'S': '*@' + address_domain}},
                TableName=configuration['dynamodb_table'],
                ProjectionExpression='destinations')

            if 'Item' in dynamo_result:
                for current_new_recipient in dynamo_result['Item']['destinations']['SS']:
                    logger.info('No mapped destination found, using generic domain mapping')
                    new_recipients.append(current_new_recipient)
                last_mapped_original_recipient = current_original_recipient
            else:
                raise Exception('No match found in mapping table for recipients {}'.format(
                    event['Records'][0]['ses']['receipt']['recipients']))

    logger.info('Mapped recipients: {}'.format(new_recipients))
    return [new_recipients, last_mapped_original_recipient]


def load_message(event, context):
    message_id = event['Records'][0]['ses']['mail']['messageId']

    logger.info(
        'Loading raw mail from S3://{}/{}{}'.format(configuration['s3_bucket'], configuration['s3_folder'], message_id))
    raw_email = s3_client.get_object(
        Bucket=configuration['s3_bucket'],
        Key='{}{}'.format(configuration['s3_folder'], message_id))['Body'].read()

    return email.message_from_bytes(raw_email)


def process_headers(mail_contents, original_recipient, new_recipients):
    # Add 'Reply-To' header, if necessary
    if not 'Reply-To' in mail_contents:
        mail_contents['Reply-To'] = mail_contents['From']

    # Replace 'From' header, so that the message originates from an SES-sanctionned address
    original_from = mail_contents['From']
    del mail_contents['From']
    mail_contents['From'] = '{} <{}>'.format(
        original_from.replace('<', '-').replace('>', '-'),
        original_recipient)

    # Replace 'To' header
    del mail_contents['To']
    mail_contents['To'] = ', '.join(new_recipients)

    # Remove headers that will be invalid once the message is forwarded
    del mail_contents['Return-Path']
    del mail_contents['Sender']
    del mail_contents['Message-ID']
    del mail_contents['DKIM-Signature']

    return mail_contents


def send_ses_mail(mail_contents, new_recipients):
    new_message_id = ses_client.send_raw_email(RawMessage={'Data': mail_contents.as_string()},
                                               Destinations=new_recipients)
    logger.info('Message sent. ID={}'.format(new_message_id))

    return new_message_id


def lambda_handler(event, context):
    # Sanity check: is the received event an SES one?
    event_sanity_check(event, context)

    logger.info('Processing message: {}'.format(event['Records'][0]['ses']['mail']['messageId']))

    # Map recipients
    new_recipients, original_recipient = recipients_mapper(event, context)

    # Fetch S3-stored message
    mail_contents = load_message(event, context)

    # Process mail headers
    mail_contents = process_headers(mail_contents, original_recipient, new_recipients)

    # Send mail
    send_ses_mail(mail_contents, new_recipients)

    return 0
