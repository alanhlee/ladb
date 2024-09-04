import requests
import os
from dotenv import load_dotenv
from qb_table_enums import WORK_ORDERS_CONTACTS_AND_ADRESSES
import qb_connect



load_dotenv()



headers = {
    'QB-Realm-Hostname': 'ladb.quickbase.com',
    'User-Agent': '{User-Agent}',
    'Authorization': 'QB-USER-TOKEN ' + os.environ.get("user_token")
}

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
            print("Error while fetching data: " + str(temp) + " status: " + str(r.status_code))
            return
        if to_ret is None:
            to_ret = temp
        else:
            to_ret["data"] += temp["data"]
        total += temp["metadata"]["numRecords"]
        goal = temp["metadata"]["totalRecords"]
    to_ret["metadata"]["numRecords"] = total

    return to_ret



def backfill():
    work_orders_contacts_and_addresses = get_data_set(os.getenv("p_work_orders_contacts_and_addresses_id"), [3, 40, 60, 63, 65, 66, 67, 69, 72, 73, 88, 89, 90, 91, 92, 93, 94, 95, 96],
                                                      '{1.GT.01-01-1900}')
    count = 0
    payload = []
    for item in work_orders_contacts_and_addresses["data"]:
        record = {}
        if item["90"]["value"] == "":
            count += 1
            # record[str(WORK_ORDERS_CONTACTS_AND_ADRESSES.table_work_order_number.value)] = {"value": item[""]}
            record[str(WORK_ORDERS_CONTACTS_AND_ADRESSES.Record_ID.value)] = {"value": item["3"]["value"]}
            record[str(WORK_ORDERS_CONTACTS_AND_ADRESSES.table_contact_email.value)] = {"value": item["88"]["value"]}
            record[str(WORK_ORDERS_CONTACTS_AND_ADRESSES.table_contact_mobile.value)] = {"value": item["67"]["value"]}
            record[str(WORK_ORDERS_CONTACTS_AND_ADRESSES.table_contact_address_details.value)] = {'value': item["72"]["value"]}
            record[str(WORK_ORDERS_CONTACTS_AND_ADRESSES.table_contact_name_display.value)] = {"value": item["66"]["value"]}
            record[str(WORK_ORDERS_CONTACTS_AND_ADRESSES.table_contact_company_name.value)] = {'value': item["65"]["value"]}
            record[str(WORK_ORDERS_CONTACTS_AND_ADRESSES.table_contact_address_type.value)] = {'value': item["73"]["value"]}
            record[str(WORK_ORDERS_CONTACTS_AND_ADRESSES.table_contact_phone.value)] = {'value': item["69"]["value"]}
        payload.append(record)
    qb_connect.update_record(os.getenv("p_work_orders_contacts_and_addresses_id"), payload)
    print("Total :", count, " Updated")



def main():
    backfill()

if __name__ == '__main__':
    main()


