import requests
import json
import time
import jwt
import os
# from collections import defaultdict
# import urllib.request
from dotenv import load_dotenv

import mysql_connect as mysql_connect
import qb_connect as qb_connect
# from qb_connect import update_record, get_data_set
from qb_table_enums import DELIVERY_RECEIPTS, VAULT
# from mysql_connect import obtain_delivery, update_many_rows
import base64
import re

load_dotenv()  # need this to load the variables in .env


# returns a list of dictionary objects
def get_form_json_results(iform_url, username, password):
    auth = {'USERNAME': username, 'PASSWORD': password}
    r = requests.post(iform_url, params=auth)

    # load the array of JSON into a list object
    record_array = json.loads(r.text)

    # returns a list of dictionary objects
    return record_array


# build the JWT send in the request for the token, and then return a token to use
def get_access_token(client_key, client_secret):
    headers = {
        'alg': 'HS256',
        'typ': 'JWT'
    }

    claim_obj = {
        "iss": client_key,
        "aud": "https://app.iformbuilder.com/exzact/api/oauth/token",
        "exp": time.time() + 90,
        "iat": time.time()
    }

    jwt_encoded = jwt.encode(claim_obj, client_secret, headers=headers)

    access_request = {
        "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
        "assertion": jwt_encoded
    }

    r = requests.post('https://app.iformbuilder.com/exzact/api/oauth/token', data=access_request)

    response_body = json.loads(r.text)
    print(response_body)

    access_token = response_body['access_token']

    return access_token


# def deleteFormRecords(records_list):
#     headers = {
#         'Content-Type': 'application/json',
#         'Authorization': getAccessToken(os.environ.get('iform_client_key'), os.environ.get('iform_client_secret'))
#     }
#
#     request = requests.post(
#         'https://servername.iformbuilder.com/exzact/api/v60/profiles/profile_id/pages/page_id/records',
#         data=values, headers=headers)
#     request.get_method = lambda: 'DELETE'


# takes a json object and adds it to the JSON as an entry, all this is string handled since they use duplicate keys
def add_delete_record(add_record):
    global the_list
    x = the_list
    if the_list == '':
        the_list = add_record
    else:
        the_list = x + ',' + add_record

    # delete_records_list[0]
    # print(json.dumps(delete_records_list[0]))
    print(the_list)


# I need to add these braces to the beginning and end of the JSON object,
# because the body of the delete needs it to be expressed this way
def complete_delete_list(delete_list):
    global delete_records_list
    delete_records_list = '[ { ' + delete_list + ' } ]'
    print(delete_records_list)


def signature_as_base64(url, username, password):
    params = {'USERNAME': username, 'PASSWORD': password}
    return base64.b64encode(requests.get(url, params=params).content)


# adds the signatures from iform to quickbase
# make sure you have an updated sql database
def add_signatures():
    url = os.environ.get("iform_delivery_url")
    username = os.environ.get("iform_username")
    password = os.environ.get("iform_password")
    iform_records = get_form_json_results(url, username, password)
    data = []
    ids = []
    count = 0
    for obj in iform_records:
        count += 1
        record = obj["record"]
        move_num = record["movement_numbers"]

        for num in re.findall("[0-9]*", move_num):
            if num == "":
                continue
            rows = mysql_connect.obtain_delivery(num)
            for row in rows:
                sign = signature_as_base64(record["Signature"], username, password)
                rec = row["Record_ID"]
                ids += [rec]
                print("yay match found for ", rec)
                data += [
                    {
                        DELIVERY_RECEIPTS.Record_ID.value: {"value": rec},
                        DELIVERY_RECEIPTS.signature.value: {"value": {"data": sign,
                                                                      "fileName": str(num + "_sign.jpg")}}
                    }
                ]
        if count % 1000 == 0:
            print(count, "iform records have been looked at")

    response = qb_connect.update_record(os.environ.get("delivery_receipts_id"), data)
    if response is not None:
        update_data = response["data"]
        fields_dict = {
            "1": "Date_Created",
            "2": "Date_Modified",
            "3": "Record_ID",
            "4": "Record_Owner",
            "5": "Last_Modified_By"
        }
        mysql_connect.update_many_rows("DELIVERY_RECEIPTS", fields_dict, update_data,
                                       os.environ.get("delivery_receipts_id"), ids)


# changes the status and location of the assets to Out
def batch_check_out():
    item = 'vault_check_out_last_updated'
    last_updated = mysql_connect.get_global_var(item)
    iform_records = get_form_json_results(os.environ.get("iform_vault_check_out_url"),
                                          os.environ.get("iform_username"), os.environ.get("iform_password"))
    updated_date = last_updated
    # loops through all of the check out forms
    for check_out in iform_records:
        record = check_out['record']
        created = record['CREATED_DATE']
        # checks against the global variable so only forms that have not been processed are dealt with
        if created <= last_updated:
            break
        # this is so that the global variable can be changed later on
        if created > updated_date:
            updated_date = created
        # finds all of the barcodes in the form
        barcodes = re.findall("[a-zA-Z0-9]{5,20}", record["barcodes"])

        # creates a query so that we can find all of the record_ids of the assets that match the barcodes
        # have to do in batches of 100, quickbase is limited to 100 filters grouped by AND or OR
        # I did batches of 95 just to be safe.
        assets = []
        barcode_length = len(barcodes) - 1
        query = ""
        # get all records that relate to barcode
        for i, code in enumerate(barcodes):
            if query != "":
                query += "OR"
            query += "{'" + str(VAULT.ladb_barcode.value) + "'.EX.'" + str(code) + "'}"

            if i > 0 and i % 95 == 0 or i == barcode_length:
                temp = qb_connect.get_data_set(os.environ.get('vault_id'), [VAULT.Record_ID.value, VAULT.status.value,
                                                                            VAULT.ladb_barcode.value,
                                                                            VAULT.location.value], query)
                if temp and temp['data']:
                    assets += temp['data']
                query = ""
                barcode_length -= 95

        if assets is not None:
            # sets up variables for adding records to vault_delivery_receipts and work_orders_vault
            delivery = qb_connect.get_data_set(os.environ.get("delivery_receipts_id"), [3],
                                               "{" + str(DELIVERY_RECEIPTS.receipt_number.value) + ".CT." +
                                               str(record['delivery_receipt']) + "}")
            work_order = None
            if record['work_order']:
                work_order = qb_connect.get_data_set(os.environ.get('work_orders_id'), [3], '{121.EX.' +
                                                     record['work_order'] + "}")
            if work_order is not None and work_order['data']:
                work_order = work_order['data'][0]['3']['value']
            work_vault = []

            if delivery is not None and delivery['data']:
                del_id = delivery['data'][0]['3']['value']
            else:
                del_id = None
            body = []
            # loops through each asset that needs to be updated
            for asset in assets:
                # does a work_order_vault record need to be made?
                if work_order is not None and type(work_order) == int:
                    work_vault += [{'6': {'value': asset['3']['value']}, '8': {'value': work_order}}]
                # does a vault_delivery_receipt record need to be made?
                if del_id is not None:
                    body += [{'6': {'value': asset['3']['value']}, '10': {'value': del_id}}]

            # updates the tables; pipeline updates the vault records
            if body:
                qb_connect.update_record(os.environ.get('vault_delivery_receipts_id'), body)
            if work_vault:
                qb_connect.update_record(os.environ.get('work_orders_vault_id'), work_vault)
    # updates the global variable
    mysql_connect.update_global(updated_date, item)


def batch_check_in():
    item = 'vault_check_in_last_updated'
    last_updated = mysql_connect.get_global_var(item)
    iform_records = get_form_json_results(os.environ.get("iform_vault_check_in_url"),
                                          os.environ.get("iform_username"),
                                          os.environ.get("iform_password"))
    updated_date = last_updated
    # loops through all of the records, starting from the newest going to the oldest form
    for check_in in iform_records:
        record = check_in['record']
        created = record['CREATED_DATE']

        staff = record['vault_user']
        # check against global variable to see if this form has already been added into qb
        if created <= last_updated:
            break
        # this is so we can update the global variable at the end
        if created > updated_date:
            updated_date = created

        # gets all the barcodes from the form
        barcodes = re.findall("[A-Za-z0-9]+", record["barcodes"])

        # creates a query so that we can find all of the record_ids of the assets that match the barcodes
        # have to do in batches of 100, quickbase is limited to 100 filters grouped by AND or OR
        # I did batches of 95 just to be safe.
        assets = []
        barcode_length = len(barcodes) - 1
        query = ""
        # get all records that relate to barcode
        for i, code in enumerate(barcodes):
            if query != "":
                query += "OR"
            query += "{'" + str(VAULT.ladb_barcode.value) + "'.EX.'" + str(code) + "'}"
            if i > 0 and i % 95 == 0 or i == barcode_length:
                # 1 API call for each 95 records
                temp = qb_connect.get_data_set(os.environ.get('vault_id'),
                                               [VAULT.Record_ID.value, VAULT.ladb_barcode.value,
                                                VAULT.location.value], query)
                if temp and temp['data']:
                    assets += temp['data']
                query = ""
                barcode_length -= 95
        body = []

        for rec in assets:
            # rec is the information used to insert or update a record in the VAULT table
            rec[str(VAULT.status.value)] = {'value': 'In'}
            rec[str(VAULT.last_location.value)] = rec[str(VAULT.location.value)]

            if record['shelf']:
                rec[str(VAULT.location.value)] = {'value': record['shelf']}
            else:
                rec[str(VAULT.location.value)] = {'value': ''}

            body += [rec]
        vault = None
        if body:
            vault = qb_connect.insert_record(os.environ.get('vault_id'), body)

        if vault:
            for i in vault['data']:
                requests.post(
                    'https://ladb.quickbase.com/db/' + os.environ.get('vault_id') + '?a=API_ChangeRecordOwner&rid=' +
                    str(i['3']['value']) + '&newowner=' + staff + '&usertoken=' + os.environ.get('user_token') +
                    '&apptoken=' + os.environ.get("app_token")
                )

    mysql_connect.update_global(updated_date, item)


def fastapi_test():
    return "hello"


def test():
    while True:
        batch_check_out()
        batch_check_in()


# set the globals for creation of deletion lists
delete_records_list = ''
the_list = ''


def main():
    test()


# examples for adding to the deletion list to be sent in the body of the delete request
#     temp_add = '"id": 1'
#     temp_add2 = '"id": 2'
#     temp_add3 = '"id": 3'
#     addDeleteRecord(temp_add)
#     addDeleteRecord(temp_add2)
#     completeDeleteList(the_list)


if __name__ == '__main__':
    main()
