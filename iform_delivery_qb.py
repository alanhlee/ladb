import os
import qb_connect
import iform_connect
from qb_table_enums import DELIVERIES
import email_alert
import base64
import requests
import json
import logs

# we have to give the emails a list to send to
send_emails_to = ['operations@ladb.com', 'it@ladb.com']

def parse_deliveries(delivery_array, qb_deliveries_table_drns, delivery_receipts_table, deliveries_table, qb_deliveries_table_delivery_ids):
    # this is a list, this is how you declare a list
    iform_movement_numbers = []
    data = []
    iform_invalid_drns = []
    for delivery in delivery_array:
        # setting variables for delivery object
        record = delivery["record"]
        modified_date = record['MODIFIED_DATE']
        created_date = record['CREATED_DATE']
        create_device_id = record['CREATED_DEVICE_ID']
        created_by = record['CREATED_BY']
        delivery_id = record['ID']
        receipt_numbers = record['movement_numbers']
        signature = record['Signature']
        recipient = record['Recipient']
        notes = record['QuickNotes']

        for receipt_number in receipt_numbers.split(','):
            try:
                if receipt_number in qb_deliveries_table_drns or int(receipt_number) in qb_deliveries_table_drns:
                    qb_deliveries_table_delivery_ids.append(int(delivery_id))
                    logs.logger_info.info('Duplicate movement number found, skipping record ingest: ' + str(receipt_number))
                    email_alert.send_email(send_emails_to,
                                           'Duplicate movement number',
                                           'There is a duplicate movement number in Quickbase DELIVERIES table and iForm. '
                                           'Movement number: {}'.format(receipt_number))

                    logs.logger_info.info('Email notification sent to {} for duplicate movement number.'
                          .format(send_emails_to))
                    continue
                # base64 encoding the images to be stored
                try:
                    signature_b64 = base64.b64encode(requests.get(signature).content)
                except Exception as e:
                     logs.logger_error.error(str(e))
                file_name = delivery_id + created_by + '.png'
            except Exception as e:
                logs.logger_error.error(str(e))
                if receipt_number in qb_deliveries_table_drns or str(receipt_number) in qb_deliveries_table_drns:
                    qb_deliveries_table_delivery_ids.append(int(delivery_id))
                    logs.logger_info.info('Duplicate movement number found, skipping record ingest: ' + str(receipt_number))
                    email_alert.send_email(send_emails_to,
                                           'Duplicate movement number',
                                           'There is a duplicate movement number in Quickbase DELIVERIES table and iForm. '
                                           'Movement number: {}'.format(receipt_number))
                    logs.logger_info.info('Email notification sent to {} for duplicate movement number'
                          .format(send_emails_to))
                    continue
                # base64 encoding the images to be stored
                try:
                    signature_b64 = base64.b64encode(requests.get(signature).content)
                except Exception as e:
                    logs.logger_error.error(str(e))
                file_name = delivery_id + created_by + '.png'
                qb_deliveries_table_drns += [str(receipt_number)]
            if int(delivery_id) in qb_deliveries_table_delivery_ids:
                continue
            # creating the JSON payload, to make the List of Objects to insert into Quickbase,
            # because that's how QB wants it, read the developer docs for insertion
            # https://developer.quickbase.com/
            qb_record = {}
            qb_record[str(DELIVERIES.delivery_id.value)] = {"value": delivery_id}
            qb_record[str(DELIVERIES.recipient.value)] = {"value": recipient}
            qb_record[str(DELIVERIES.quick_notes.value)] = {"value": notes}
            qb_record[str(DELIVERIES.signature.value)] = {'value': {'fileName': file_name, 'data': bytes.decode(signature_b64)}}
            # try catch block for invalid movement numbers
            try:
                int(receipt_number)
                qb_record[str(DELIVERIES.receipt_numbers.value)] = {"value": receipt_number}
                qb_deliveries_table_drns += [int(receipt_number)]
                iform_movement_numbers += [int(receipt_number)]
            except Exception as e:
                logs.logger_error.error(str(e))
                logs.logger_info.info('iForm - invalid delivery receipt number, casting as string, continuing ingest. DRN: ' + str(receipt_number))
                string_drn = str(receipt_number)
                qb_record[str(DELIVERIES.receipt_numbers.value)] = {"value": string_drn}
                iform_invalid_drns += [string_drn]
                qb_deliveries_table_drns += [string_drn]
                iform_movement_numbers += [string_drn]
            # appending to the list object, this is how you do it
            data.append(qb_record)
    # email alert for invalid movement numbers compiled in iform_invalid_drns array
    if len(iform_invalid_drns) != 0:
        email_alert.send_email(send_emails_to,
                               'Invalid delivery receipt number(s)',
                               'Invalid movement number(s). '
                               'Movement number: {}'.format(iform_invalid_drns))
        logs.logger_info.info('Email notification sent to {} for invalid movement number(s)'.format(send_emails_to))
    if len(data) == 0:
        logs.logger_info.info('All deliveries are up to date.')
    else:
        # api call to ingest into QB
        qb_connect.insert_record(os.environ.get('p_deliveries_id'), data)
        logs.logger_info.info(str(len(data)) + ' delivery record(s) have been ingested.')

        check_receipt_numbers(delivery_receipts_table, deliveries_table, iform_movement_numbers)
        delete_iform_records(delivery_array, qb_deliveries_table_delivery_ids)


# checking QB DELIVERY_RECEIPTS: if movement number does not exist email will be sent to recipient specified.
def check_receipt_numbers(delivery_receipts_table, deliveries_table, iform_movement_numbers):
    receipt_numbers = []
    missing_movement_numbers = []

    # compiling movement numbers from delivery receipts table try/catch block for invalids
    for entry in delivery_receipts_table['data']:
        try:
            isinstance(int(entry['16']['value']), int)
            receipt_numbers += [int(entry['16']['value'])]
        except Exception as e:
            logs.logger_error.error(e)
            receipt_numbers += [str(entry['16']['value'])]
    # DELIVERIES table drns compiled to check against DELIVERY_RECEIPTS appending to missing_movement_numbers
    for entry2 in deliveries_table['data']:
        try:
            isinstance(int(entry2['7']['value']), int)
            drn2 = int(entry2['7']['value'])
            if drn2 not in receipt_numbers:
                missing_movement_numbers += [drn2]
        except Exception as e:
            logs.logger_error.error(e)
            drn2 = str(entry2['7']['value'])
            if drn2 not in receipt_numbers:
                missing_movement_numbers += [drn2]
    # appending to missing_movement_numbers from iform movement numbers
    for movement_number in iform_movement_numbers:
        if movement_number not in receipt_numbers:
            missing_movement_numbers += [movement_number]

    if len(missing_movement_numbers) != 0:
        email_alert.send_email(send_emails_to,
                               'Missing Movement Numbers',
                               'These movement numbers do not exist in DELIVERY_RECEIPTS. Please check them: {}'.format(
                                   set(missing_movement_numbers)))
        logs.logger_info.info('Email alert sent to {} for missing movement numbers within DELIVERY_RECEIPTS'.format(send_emails_to))


# remove records from iForm after ingest
def delete_iform_records(delivery_array, qb_deliveries_table_delivery_ids):
    values = []
    for delivery in delivery_array:
        obj = {}
        record = delivery["record"]
        delivery_id = record['ID']
        obj["id"] = int(delivery_id)
        if int(delivery_id) not in qb_deliveries_table_delivery_ids:
            values.append(obj)

    if len(values) != 0:
        logs.logger_info.info(str(len(values)) + " record(s) will be removed from iForm.")
    else:
        logs.logger_info.info('There are no records to remove from iForm.')
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer {}'.format(
            iform_connect.get_access_token(os.environ.get('iform_client_key'), os.environ.get('iform_client_secret')))

    }
    values = json.dumps(values)
    try:

        r = requests.delete(
        'https://api.iformbuilder.com/exzact/api/v81/app/profiles/11298/pages/3875164/records', data=values,
        headers=headers)
    except Exception as e:
        logs.logger_error.warning(str(e))

    if r.status_code == 200 and len(r.text) - 2 != 0:
        logs.logger_info.info("Records successfully removed from iForm. " + str(r.text))


def main():
    logs.logger_info.info("Delivery check started")
    delivery_array = iform_connect.get_form_json_results(os.environ.get('p_iform_delivery_url'),
                                                         os.environ.get('iform_username'),
                                                         os.environ.get('iform_password'))

    # here we are getting all the current receipt_numbers from delivery_receipts/deliveries tables for us to check against
    delivery_receipts_table = qb_connect.get_data_set(os.environ.get("p_delivery_receipts_id"), [16], '{1.GT.01-01-1900}')
    deliveries_table = qb_connect.get_data_set(os.environ.get("p_deliveries_id"), [6, 7], '{1.GT.01-01-1900}')

    qb_deliveries_table_drns = []
    qb_deliveries_table_delivery_ids = []
    count = 0
    logs.logger_info.info("Processing deliveries")
    for delivery in deliveries_table['data']:
        count += 1
        if count % 5 == 0 and count > 0:
            logs.logger_info.info(str(count) + " entries have been obtained from DELIVERIES.")
        try:
            int(delivery['7']['value'])
            movement_numbers = int(delivery['7']['value'])
            qb_deliveries_table_drns += [movement_numbers]
        except Exception as e:
            logs.logger_error.error(str(e))
            movement_numbers = str(delivery['7']['value'])
            qb_deliveries_table_drns += [movement_numbers]
        skip_delivery_id = int(delivery['6']['value'])
        qb_deliveries_table_delivery_ids += [skip_delivery_id]
    logs.logger_info.info(str(count) + ' movement numbers and ids compiled.')
    parse_deliveries(delivery_array, qb_deliveries_table_drns, delivery_receipts_table, deliveries_table, qb_deliveries_table_delivery_ids)

    logs.logger_info.info("Delivery check ended")


if __name__ == '__main__':
    main()




