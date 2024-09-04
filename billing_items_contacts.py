import os
from dotenv import load_dotenv
import requests
from qb_table_enums import WORK_ORDERS_LINE_ITEMS

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


# 41 (contacts WORK_ORDER_number) 55 (contact_name) 56 (contact_company)
work_orders_contacts = get_data_set(os.environ.get("p_work_orders_contacts_and_addresses_id"), [41, 55, 56], '{1.GT.01-01-1900}')
#
# contacts_wo_number = work_orders_contacts["data"
# contacts_name = work_orders_contacts["data"]
# contacts_company = work_orders_contacts["data"]

# 28 (wo_number) 62 (billing_item) 76 (invoice_number) 77 (line items WORK_ORDER created date)
work_orders_line_items = get_data_set(os.getenv("p_work_orders_line_items_id"), [28, 62, 76, 77], '{1.GT.01-01-1900}')

# billing_item = work_orders_line_items["data"]
# line_item_work_order = work_orders_line_items["data"]
# line_item_wo_created_date = work_orders_line_items["data"]

line_items_wo_numbers = []
total_line_items = []
complete = []


def line_items():

    for line_item in work_orders_line_items["data"]:
        if line_item["62"]["value"]:
            total_line_items.append(str(line_item["62"]["value"]))
            set_line_items = set(total_line_items)
            for item in set_line_items:
                complete.append(item)
        if line_item["28"]["value"]:
            line_items_wo_numbers.append(int(line_item["28"]["value"]))
        for contact in work_orders_contacts["data"]:
            print(contact)
    #         if str(contact["41"]["value"]) in line_items_wo_numbers:
    #             complete.append("billing item: ", line_item["62"]["value"], "wo_number: ", line_item["28"]["value"], "invoice: ", str(line_item["76"]["value"]), "created at: ", line_item["77"]["value"])
    # print(complete)




def main():
    line_items()


if __name__ == '__main__':
    main()