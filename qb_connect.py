import requests
import os
from scripts.al import mysql_connect as mysql_connect
from qb_table_enums import OPERATIONS, DELIVERY_RECEIPTS
from dotenv import load_dotenv
import json


# need this to load the variables in .env
load_dotenv()

# needed for working with qb
headers = {
    'QB-Realm-Hostname': 'ladb.quickbase.com',
    'User-Agent': '{User-Agent}',
    'Authorization': 'QB-USER-TOKEN ' + os.environ.get("user_token")
}


# returns an array of objects of the query results
# get_fields is a comma delimited List of field numbers you want to return,
# empty means we just return all the fields, to be a list needs to be in Brackets []
def get_data_set(table_id, get_fields, get_query, get_sort_by=False):
    body = {"from": table_id, "select": get_fields, "where": get_query, "sortBy": get_sort_by, "options": {}}
    total, goal = 0, -1
    to_ret = None
    while goal != total:
        body["options"]["skip"] = total
        r = requests.post(
            'https://api.quickbase.com/v1/records/query',
            headers=headers,
            json=body
        )

        # this returns the json as a dict, data is the array that has all the objects
        temp = r.json()
        if r.status_code != 200:
            print("Error while fetching data: " + str(temp))
            return
        if to_ret is None:
            to_ret = temp
        else:
            to_ret["data"] += temp["data"]
        total += temp["metadata"]["numRecords"]
        goal = temp["metadata"]["totalRecords"]
    to_ret["metadata"]["numRecords"] = total

    return to_ret


# def update_single_field(table_id, field_id, field_value, record_id):
#     headers = {
#         'QB-Realm-Hostname': 'ladb.quickbase.com',
#         'User-Agent': 'qb_connector',
#         'Authorization': 'QB-USER-TOKEN ' + os.environ.get("user_token")
#     }
#
#     body = {
#               "to": table_id,
#               "data": [
#                 {
#                   field_id: {
#                     "value": field_value
#                   }
#                 }
#               ],
#               "mergeFieldId" : record_id,
#               "fieldsToReturn": [
#                 3,
#                 4,
#                 5,
#                 9,
#                 10,
#                 11,
#                 12,
#                 13
#               ]
#             }
#
#     r = requests.post(
#         'https://api.quickbase.com/v1/fields/117',
#         params=params,
#         headers=headers,
#         json=body
#     )


# def addContact():
#     ticket = getTicket('https://ladb.quickbase.com/db/main',
#     os.environ.get("qb_user"), os.environ.get("qb_pass"), '6')
#     contact = {'first_name': 'Kyle',
#                'last_name': 'Kuzma',
#                'contact_type': 'Primary Contact',
#                'email': 'kkuzma@gmail.com',
#                'phone': '8185251861',
#                'mobile': '8185251862'
#     }
#
#     the_res = add_record(os.environ.get('contacts_url'), contact, ticket)
#     res = str(the_res)
#
#     if res.startswith("Error"):
#         print("Cannot add contact Error: ")
#         return
#
#     print("Added Contact: " + res)

# def addDeliverySignature():
#     ticket = getTicket('https://ladb.quickbase.com/db/main',
#     os.environ.get("qb_user"), os.environ.get("qb_pass"), '6')
#     contact = {'first_name': 'Kyle',
#                'last_name': 'Kuzma',
#                'contact_type': 'Primary Contact',
#                'email': 'kkuzma@gmail.com',
#                'phone': '8185251861',
#                'mobile': '8185251862'
#     }
#
#     the_res = add_record(os.environ.get('contacts_url'), contact, ticket)
#     res = str(the_res)
#
#     if res.startswith("Error"):
#         print("Cannot add contact Error: ")
#         return
#
#     print("Added Contact: " + res)


# returns Record ID# of the delivery receipt with receipt number receipt_id
def get_delivery(receipt_id):
    # receipt_number is field 85
    # phat_data = getDataSet(os.environ.get('delivery_receipts_id'), "{'85'.CT." + delivery_id + "}", '', '')
    phat_data = get_data_set(os.environ.get('delivery_receipt_id'), [3], "{ '" +
                             str(DELIVERY_RECEIPTS.receipt_number.value) + "'." + OPERATIONS.Equals.value + "." +
                             str(receipt_id) + "}", '')
    if phat_data is None:
        print("something went wrong with the request")
        return
    if phat_data['metadata']["numRecords"] == 0:
        # print("nothing matches this request")
        return
    return phat_data["data"][0]['3']['value']


# returns an array of tables for an app given by app_id
def get_all_tables(app_id):
    params = {
        'appId': app_id
    }
    r = requests.get(
        'https://api.quickbase.com/v1/tables',
        params=params,
        headers=headers
    )

    if r.status_code != 200:
        print("retrieving tables was unsuccessful")
        return

    return r.json()


# returns an array of fields for table given by table_id
def get_all_fields(table_id):
    params = {
        "tableId": table_id,
        "includeFieldPerms": False
    }

    r = requests.get("https://api.quickbase.com/v1/fields",
                     params=params,
                     headers=headers
                     )
    if r.status_code != 200:
        print("retrieving fields from table " + str(table_id) + " was unsuccessful")
        return

    return r.json()


# returns a correctly formatted body for inserting into quickbase from data in sql
def get_insert_body(table_name, fields, record_id, for_vault_transfer=False, unique=()):
    data = {}

    temp = mysql_connect.obtain_row(table_name, record_id)
    if temp is None:
        print("row with " + str(record_id) + " record id in table " + table_name + " does not exist")
        return
    try:
        row = list(temp)[0]
        temp.close()
    except IndexError:
        print("row with " + str(record_id) + " record id in table " + table_name + " does not exist")
        return

    for field in fields:
        field_id, type_field = fields[field]

        if (type_field == 2 and (row[field + "_filename"] is None or row[field + "_file_data"] is None)) or \
                (type_field != 2 and (row[field] is None or field_id in [1, 2, 3, 4, 5])):
            if field_id in unique:
                value = {"value": 0}
            else:
                continue
        elif field == "last_modified_by" or field == "record_owner":
            value = {"email": row[field + "_1"]}
        elif type_field == 1:
            value = {"email": row[field]}
        elif type_field == 2:
            value = {"fileName": row[field + "_filename"], "data": row[field + "_file_data"]}
        else:
            if type(row[field]) == str:
                value = row[field].replace("\r", " ").replace("\u2019", "'").replace("\u201C", '"')\
                    .replace('\u201D', '"').replace("\u2018", "'").replace("\x1e", "30")
            else:
                value = row[field]
        if for_vault_transfer:
            data[str(field_id)] = str({"value": value})
        else:
            data[str(field_id)] = {"value": value}
    return [data]


# updates an existing record in Quickbase based on the respective entry data in mySQL
def update_record_from_sql(table_id, table_name, fields, record_id, fields_dict, fields_to_return=(1, 2, 3)):
    temp_dict = {field["label"].replace("#", "").replace(" ", "_"): (field["id"], field_type(field))
                 for field in fields}
    data = get_insert_body(table_name, temp_dict, record_id)
    if data:
        data[0]["3"] = {"value": record_id}
    else:
        return
    to_ret = update_record(table_id, data, fields_to_return)
    if to_ret:
        try:
            to_ret["message"]
        except KeyError:
            mysql_connect.update_row(table_name, table_id, record_id, fields_dict)
        return to_ret
    return


# inserts a new record into Quickbase bases on an already existing entry in the mySQL database
def insert_record_from_sql(table_id, table_name, fields, record_id, fields_dict, fields_to_return=(1, 2, 3)):
    temp_dict = {field["label"].replace("#", "").replace(" ", "_"): (field["id"], field_type(field))
                 for field in fields}
    data = get_insert_body(table_name, temp_dict, record_id)
    if data:
        response = insert_record(table_id, data, fields_to_return)
    else:
        return
    if response:
        try:
            response["message"]
        except KeyError:
            mysql_connect.update_row(table_name, table_id, record_id, fields_dict, response["data"][0]['3'])
        return response
    else:
        return


# updates a record in Quickbase based on the data passed into the function
def update_record(table_id, data, fields_to_return=(1, 2, 3, 4, 5)):
    body = {
        "to": table_id,
        "data": data,
        "fieldsToReturn": list(fields_to_return)
    }

    r = requests.post(
        'https://api.quickbase.com/v1/records',
        headers=headers,
        json=body
    )
    try:
        to_ret = r.json()
    except json.decoder.JSONDecodeError:
        print(r)
        return
    if r.status_code == 404:
        print(to_ret['message'])
        return
    elif r.status_code == 400:
        print("bad request")
        print(body)
        return
    elif r.status_code == 403:
        print("you do not have access to the table " + table_id)
        return
    elif r.status_code == 207:
        for k, v in to_ret["metadata"]["lineErrors"].items():
            print('key:', k, 'value:', v)
        for i, dat in enumerate(data):
            print(i, 'for', dat)
        return to_ret
    return to_ret


# inserts a new record into Quickbase based on the data passed into this function
def insert_record(table_id, data, fields_to_return=(1, 2, 3)):
    fields_to_return += (3,)
    return update_record(table_id, data, fields_to_return)


# returns 1 if the type is a user, 2 if the type is a file, and 0 otherwise
# this is needed to handle the data that is returned from Quickbase queries without using a bunch of try except
def field_type(field):
    if field["fieldType"] == "user":
        return 1
    elif 'file' in field['fieldType']:
        return 2
    else:
        return 0


# returns the metadata about the table with table ID table_id
def get_table_data(table_id):
    params = {
        'appId': os.environ.get('app_id')
    }

    r = requests.get(
        'https://api.quickbase.com/v1/tables/' + table_id,
        params=params,
        headers=headers
    )
    if r.status_code != 200:
        print("could not get table with table_id: ", table_id)
        return
    return r.json()


# finds the table id given the table_name, the table must exist in the app defined in the .env file
# O(n) where n is the number of tables in the app, so not really ideal to call this a lot
def get_table_id(table_name):
    tables = get_all_tables(os.environ.get("app_id"))
    for table in tables:
        if table["name"] == table_name:
            return table["id"]


# gets file data from Quickbase
# returns a Base64 encoded string
# apparently this is slow because any table that needs this takes forever to fill up
def get_file(table_id, record_id, field_id, version):
    r = requests.get(
        'https://api.quickbase.com/v1/files/' + str(table_id) + '/' + str(record_id) + '/' + str(field_id) + '/' + str(
            version),
        headers=headers
    )
    if r.status_code != 200:
        print("Unable to get that file")
        return None

    return r.content


# returns the name of a table with the given table_id
# O(1) so this is much faster than finding the id from the name AKA it is more handy to have the table_id
def get_table_name(table_id):
    table_data = get_table_data(table_id)
    if table_data is None:
        return
    return table_data["name"]


# updates a whole table based on an existing table in SQL
# the table in Quickbase must already be defined
# inserts new records, and updates records that already exist based on record_id
# does not add duplicates at least from what I know...
def update_table_from_sql(table_id, unique_key_field_ids=()):
    table_name = get_table_name(table_id)
    rec_ids = mysql_connect.get_record_ids(table_name)
    fields = get_all_fields(table_id)
    full_data = get_data_set(table_id, unique_key_field_ids + (3,), "{3.GT.-1}")

    # key: field_name value: tuple of field_id and field_type
    valid_fields = {}
    # key: field_id, value: field name(what it will be in sql)
    field_dict = {}
    unique = []
    ids = []
    if full_data is not None:
        full_data = full_data["data"]
        for data in full_data:
            ids += [data["3"]["value"]]
            if unique_key_field_ids:
                unique += [tuple(data[str(i)]["value"] for i in unique_key_field_ids)]

    for field in fields:
        if mysql_connect.valid_field(field) and field['label'] != "CONTACT - COMPANY - name" \
                and field["mode"] != "summary" and field['label'] != "acquired_from":
            valid_fields[field["label"].replace("#", "").replace(" ", "_")] = (field["id"], field_type(field))
            if field["label"] == "last_modified_by" or field["label"] == "record_owner":
                field_dict[str(field["id"])] = field['label'] + "_1"
            else:
                field_dict[str(field["id"])] = field["label"].replace("#", "").replace(" ", "_")

    to_update = []
    data = []
    print("creating json body")
    for rec in rec_ids:
        body = get_insert_body(table_name, valid_fields, rec, unique=unique_key_field_ids)
        # You cannot include the record ID if it is not the key field.
        if rec in ids and table_name != "CONTACTS" and unique_key_field_ids:
            body[0]['3'] = {"value": rec}
        if unique_key_field_ids:
            # print(body[0])
            try:
                temp = tuple(body[0][str(i)]["value"] for i in unique_key_field_ids)
                if temp in unique:
                    continue
            except KeyError:
                print("There was a key error for " + str(body[0]))

        to_update += body
        if len(to_update) > 0 and ((len(to_update) % 500 == 0 and table_name != "VAULT_MOVEMENTS")
                                   or len(to_update) % 1500 == 0):
            response = update_record(table_id, to_update, fields_to_return=list(field_dict.keys()))
            if response is None:
                continue
            try:
                data += response['data']
            except KeyError:
                print("bro idk", response)
            print("updated up to", len(data))
            to_update = []
    if not to_update and len(data) == 0:
        print("there is nothing to update")
        return
    if to_update:
        response = update_record(table_id, to_update, fields_to_return=list(field_dict.keys()))
        if response is not None:
            data += response['data']
        print("updated up to", len(data))
    print("updating sql")

    # mysql_connect.update_many_rows(table_name, field_dict, data, table_id, rec_ids)
    return


# THIS IS TO BE USED PURELY FOR CLEARING OUT TABLES FOR TESTING
def delete_all_records_in_table(table_id):
    body = {
        "from": table_id,
        "where": "{6.CT.'Amy'}"
    }
    r = requests.delete(
        "https://api.quickbase.com/v1/records",
        headers=headers,
        json=body
    )
    print(r.json())


def test():
    print("start")
    # delete_all_records_in_table(os.environ.get("full_contacts_id"))
    # delete_all_records_in_table(os.environ.get("work_orders_full_contacts_id"))
    # delete_all_records_in_table(os.environ.get("delivery_receipts_full_contacts_id"))
    # delete_all_records_in_table(os.environ.get("addresses_companies_id"))
    # delete_all_records_in_table(os.environ.get("vault_full_contacts_id"))
    # delete_all_records_in_table(os.environ.get("vault_id"))
    # delete_all_records_in_table(os.environ.get("vault_movements_id"))
    # insert_new_records_from_sql(os.environ.get("full_contacts_id"), ("6", "15"))
    # print(update_table_from_sql(os.environ.get("contacts_id")))
    # mysql_connect.create_qb_vault_table("vault_movements_for_quickbase", os.environ.get("vault_movements_id"))
    # mysql_connect.copy_to_qb_sql_table(os.environ.get("vault_movements_id"), "vault_movements_for_quickbase")
    #delete_all_records_in_table(os.environ.get("vault_movements_id"))


def main():
    test()


if __name__ == '__main__':
    main()
