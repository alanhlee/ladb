from scripts.al import qb_connect
import os
from dotenv import load_dotenv
import pymysql
from scripts.al.qb_table_enums import VAULT

load_dotenv()


def main():
    # Sql statement to select columns which need to be updated
    # Joining two tables on record ID where matching
    sql = "SELECT VAULT_FIXED_MAY22.Record_ID, VAULT_FIXED_MAY22.ladb_barcode, LIVE_TITLES.title_name, acquired_date, item, location, status, VAULT_FIXED_MAY22.date_created, VAULT_FIXED_MAY22.date_modified, VAULT_06_05.description " \
    "FROM VAULT_FIXED_MAY22 " \
    "JOIN VAULT_06_05 ON VAULT_FIXED_MAY22.Record_ID = VAULT_06_05.RECORD_ID " \
    "JOIN LIVE_TITLES ON LIVE_TITLES.Record_ID = VAULT_FIXED_MAY22.Related_TITLE " \
    "WHERE VAULT_FIXED_MAY22.description LIKE '%SDR UHD%' AND CHAR_LENGTH(VAULT_06_05.description) > 0;"

    # mySQL connection uses what is stored within the .env for values
    conn = pymysql.connect(host=os.environ.get('mysql_server'), user=os.environ.get('mysql_user'),
                           password=os.environ.get('mysql_password'), database=os.environ.get("mysql_database"),
                           cursorclass=pymysql.cursors.DictCursor)
    with conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            # Returns all rows of the sql statement
            rows = cursor.fetchall()
    # empty array we append to, to store what will be ingested (formatting)
    data = []
    for row in list(rows):
        temp = {}

        for value in row:
            # set record_id value to value in string
            # (this is how the formatting for ingested JSON when being entered into quickbase)
            if row[value]:
                temp[str(VAULT.Record_ID.value)] = {"value": row['Record_ID']}
                temp[str(VAULT.description.value)] = {"value": row['description']}
        data += [temp]
        print("Record " + str(len(data)) + " Updated")
        # if data is more than 1500 this will print a statement to if updates are going through
        if len(data) >= 1500:
            print("1500 done")
            qb_connect.update_record(os.environ.get("vault_id"), data)
            data = []
    if data:
        # Updates remaining entries
        qb_connect.update_record(os.environ.get("vault_id"), data)


if __name__ == '__main__':
    main()

