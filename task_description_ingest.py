from scripts.al import qb_connect
from scripts.al.qb_table_enums import TASKS
import requests
import os
from dotenv import load_dotenv
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

vault_id_array = []

def main():
    tasks_descriptions = get_data_set(os.environ.get('p_tasks_id'), [3, 6, 15], '{1.GT.01-01-1900}')
    # print(tasks_description)
    count = 0
    data = []
    # print(d_tasks_descriptions)
    for description in tasks_descriptions['data']:

        if description['6']['value'] == '':
            qb_record = {}
            department = description['15']['value']
            recordid = description['3']['value']
            count += 1
            qb_record[str(TASKS.description.value)] = {"value": 'Not Available'}
            qb_record[str(TASKS.Record_ID.value)] = {"value": recordid}
            qb_record[str(TASKS.department.value)] = {"value": department}

            data.append(qb_record)
    # print(data)
    print(count, ' total records without a task description')

    qb_connect.update_record(os.environ.get('d_tasks_id'), data)
    print(count, 'records have been updated.')

if __name__ == '__main__':
    main()

# print(json.dumps(r.json(),indent=4))
#
# d_tasks_descriptions = qb_connect.get_data_set(os.environ.get("d_tasks_id"), [3, 6, 15], "{1.GT.01-01-1900}")
#
# # qb_connect.get_data_set(os.environ.get("d_tasks_id")
# print(d_tasks_descriptions)
#
# count = 0
# data = []
# # print(d_tasks_descriptions)
# for description in d_tasks_descriptions['data']:
#
#     if description['6']['value'] == '':
#         qb_record = {}
#         department = description['15']['value']
#         recordid = description['3']['value']
#         count += 1
#         qb_record[str(TASKS.description.value)] = {"value": 'Not Available'}
#         qb_record[str(TASKS.Record_ID.value)] = {"value": recordid}
#         qb_record[str(TASKS.department.value)] = {"value": department}
#
#         data.append(qb_record)
# # print(data)
# print(count, ' total records without a task description')
#
# qb_connect.update_record(os.environ.get('d_tasks_id'), data)
# print(count, 'records have been updated.')




