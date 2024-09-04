from scripts.al import qb_connect
import os
from scripts.al.qb_table_enums import TASKS

def update_tasks_department():
    finaltext_records = qb_connect.get_data_set(os.environ.get("p_tasks_id"), [3, 15], "{15.EX.'Final Text'}")
    caption_records = qb_connect.get_data_set(os.environ.get("p_tasks_id"), [3, 15], "{15.EX.'Captioning'}")
    ft_count = 0
    caption_count = 0
    data = []
    for record in finaltext_records["data"]:
        ft_count += 1
        qb_record = {}
        department = record["15"]["value"]
        record_id = record["3"]["value"]
        qb_record[str(TASKS.department.value)] = {"value": "Accessibility Services"}
        qb_record[str(TASKS.Record_ID.value)] = {"value": record_id}
        data.append(qb_record)
    for record in caption_records["data"]:
        caption_count += 1
        qb_record2 = {}
        department = record["15"]["value"]
        record_id = record["3"]["value"]
        qb_record2[str(TASKS.department.value)] = {"value": "Accessibility Services"}
        qb_record2[str(TASKS.Record_ID.value)] = {"value": record_id}
        data.append(qb_record2)
    # edited_array = [x for x in data if x]
    print(data)
    print("Total caption: " + str(caption_count), "Total Final Text: " + str(ft_count) )
    print("Total Number of Records updated: " + str(ft_count + caption_count))
    qb_connect.update_record(os.environ.get('p_tasks_id'), data)

def main():
    update_tasks_department()

if __name__ == '__main__':
    main()