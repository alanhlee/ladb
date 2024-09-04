import csv
from qb_table_enums import DEV_CHARGE_CODES
import qb_connect
import os

def read_csv():
    data = []
    with open('../services_formats1027.csv', newline='') as csvfile:
        reader = csv.reader(csvfile)

        for rows in reader:
            print(rows)
            # print(', '.join(row))
            # print(rows[0])
            billing_code = rows[0]
            item = rows[1]
            item_client = rows[2]
            qb_record = {}
            qb_record[str(DEV_CHARGE_CODES.billing_code.value)] = {"value": billing_code.strip()}
            qb_record[str(DEV_CHARGE_CODES.item.value)] = {"value": item.strip()}
            qb_record[str(DEV_CHARGE_CODES.item_client.value)] = {"value": item_client.strip()}
            data.append(qb_record)
            print(data)
        qb_connect.insert_record(os.environ.get('d_charge_codes_id'), data)

def main():
    read_csv()

if __name__ == '__main__':
    main()
