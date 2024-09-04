import requests
import os
from dotenv import load_dotenv
load_dotenv()
from scripts.al.qb_table_enums import VAULT
from scripts.al import qb_connect

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

rid_array = [110938, 2794]
records_updated_array = []
def main():
    vault_locations = get_data_set(os.environ.get('d_vault_id'), [3, 6, 28], '{1.GT.01-01-1900}')
    # print(vault_locations)
    count = 0
    data = []
    for location in vault_locations['data']:
        if location["3"]["value"] in rid_array:
            records_updated_array.append(location["3"]["value"])
            for rid in rid_array:
                qb_record = {}
                qb_record[str(VAULT.location.value)] = {"value": 'testing'}
                qb_record[str(VAULT.Record_ID.value)] = {"value": rid}
                count += 1

                data.append(qb_record)
    print(rid_array)
    print("data: ", data)

    qb_connect.update_record(os.environ.get('d_vault_id'), data)
    print(count, 'records have been updated.', records_updated_array)

if __name__ == '__main__':
    main()
