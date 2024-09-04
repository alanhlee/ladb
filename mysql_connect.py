import ast
import os
import pymysql
import sqlalchemy.exc
from dotenv import load_dotenv
from sqlalchemy import Integer, Column, create_engine, String, Table, MetaData, Text, update
from sqlalchemy.dialects.mysql import MEDIUMTEXT
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker

import qb_connect

load_dotenv()

bad_char = "\uFFFD"
type_dict = {"text-multiple-choice": Text, "Date / Time": Text, "Numeric": Integer,
             "user": String(64), "Address": Text, "email": String(64), "Report Link": Text,
             "Formula - URL": Text, "Formula - Rich Text": Text, "Multi-select Text": Text,
             "date": Text, "rich-text": Text, "text": Text, "dblink": Text,
             "recordid": Integer, "url": Text, "address": Text,
             "multitext": Text, "timestamp": Text, "numeric": Integer,
             "file": Text, "phone": String(32), "duration": Integer, "checkbox": String(5)}


# drops all of the tables in the database specified by the .env file
def drop_all_tables(engine):
    meta = MetaData()
    meta.reflect(engine)
    tables = meta.tables
    for key in tables:
        tables[key].drop(engine)


# connects to a data base using the environment variables declared in the .env file
def connect_to_db():
    db_url = {
        'database': os.environ.get("mysql_database"),
        'drivername': 'mysql+pymysql',
        'username': os.environ.get('mysql_user'),
        'password': os.environ.get('mysql_password'),
        'host': os.environ.get('mysql_server'),
    }
    engine = create_engine(URL.create(**db_url))
    conn = engine.connect()

    return engine, conn


# returns true if the field is to be included in the SQL table, and false if not
def valid_field(field):
    if field['label'] == "wo_number" or field['label'] == "CONTACT - COMPANY - name":
        return True
    return field["mode"] != "lookup" and field["fieldType"] != "Report Link" \
        and field["fieldType"] != "dblink" and field["mode"] != "formula"


# returns a single row from the sql table
# the return is a legacy cursor that SQLAlchemy returns
def obtain_row(table_name, record_id, migration=False):
    engine, conn = connect_to_db()
    meta = MetaData(engine)
    meta.reflect(engine)

    table = meta.tables[table_name]
    if table is None:
        print("A table with table name " + table_name + " does not exist in the data base "
              + os.environ.get("mysql_database"))
        return
    if migration:
        column = [c for c in table.c if c.name == "3"][0]
        statement = table.select().where(column == record_id)
    else:
        statement = table.select().where(table.c.Record_ID == record_id)
    ret = conn.execute(statement)
    conn.close()
    return ret


def obtain_delivery(movement_number):
    engine, conn = connect_to_db()
    meta = MetaData(engine)
    meta.reflect(engine)
    table_name = "DELIVERY_RECEIPTS"

    table = meta.tables[table_name]
    if table is None:
        print("A table with table name " + table_name + " does not exist in the data base "
              + os.environ.get("mysql_database"))
        return
    statement = table.select().where(table.c.receipt_number == movement_number)
    ret = conn.execute(statement)
    conn.close()
    return ret


# inserts a row into a table
def insert_row(table, conn, data):
    if table is None:
        print("could not insert because the table does not exist")
        return
    try:
        conn.execute(table.insert(), data)
    except sqlalchemy.exc.IntegrityError:
        print("a row with this unique id already exists")


# returns all of the record ids that can be found in the SQL table with table_name
def get_record_ids(table_name):
    engine, conn = connect_to_db()
    meta = MetaData()
    meta.reflect(engine)
    session = sessionmaker(bind=engine)()

    table = meta.tables[table_name]
    if table is None:
        print("A table with table name " + table_name + " does not exist in the data base "
              + os.environ.get("mysql_database"))

    temp = session.query(table.c.Record_ID)
    conn.close()
    ret = [i["Record_ID"] for i in temp]
    return ret


# returns a list of tuples that look like (name, column)
# This is not currently being used
def get_col_name(table_name):
    engine, conn = connect_to_db()
    meta = MetaData(engine)
    meta.reflect(engine)

    table = meta.tables.get(table_name)
    if table is None:
        return
    ret = table.columns.items()
    conn.close()
    return ret


# creates a table with table name table_name in the sql database with columns based on the fields
# from Quickbase
def create_table(table_name, table_id, engine):
    meta = MetaData()
    fields = qb_connect.get_all_fields(table_id)
    field_names = []
    field_ids = {}
    drop_if_exists(table_name)
    table = None

    for field in fields:
        # ignore lookup and formula
        if valid_field(field):
            field['label'] = field['label'].replace(" ", "_").replace("#", "")

            if 'address: ' in field['label']:
                field_ids[int(field["id"])] = (field['label'][9:], 0)
            else:
                field_ids[int(field["id"])] = (field["label"], qb_connect.field_type(field))

            type_field = type_dict.get(field['fieldType'], Text)
            if field['label'].lower() in field_names:
                field['label'] += '_1'
                field_ids[int(field["id"])] = (field["label"], qb_connect.field_type(field))

            if "description" in field["label"] or 'additional_details' in field['label']:
                type_field = MEDIUMTEXT

            field_names += [field['label'].lower()]

            if field["fieldType"] == "file":
                table = Table(table_name, meta,
                              Column(field['label'] + "_filename", type_field,
                                     primary_key=field["properties"]["primaryKey"]),
                              Column(field['label'] + "_file_data", MEDIUMTEXT,
                                     primary_key=field["properties"]["primaryKey"]),
                              extend_existing=True)
            else:
                table = Table(table_name, meta,
                              Column(field['label'], type_field,
                                     primary_key=field["properties"]["primaryKey"]),
                              extend_existing=True)
    table.create(engine, checkfirst=True)
    return table, field_ids


# fills in and updates existing entries in the mySQL database based on the records in Quickbase
def fill_table(table_name, table_id, conn, table, field_ids, query="{1.GT.01-01-1900}"):
    table_data = qb_connect.get_data_set(table_id, list(field_ids.keys()), query, "")

    ins = table.insert()
    print("table " + table_name + " has " + str(len(table_data["data"])) + " rows.")
    count = 0
    for row in table_data["data"]:
        to_insert = {}
        for key in row:
            label = field_ids[int(key)]
            temp = row[key]["value"]
            # if value is None skip this because mysql defaults to None if no value is given
            if not temp:
                continue

            if label[1] == 0:  # most columns
                if type(temp) == list:
                    temp = ', '.join(map(str, temp))
                if type(temp) == str and bad_char in temp:
                    to_insert[label[0]] = temp.replace(bad_char, "_")
                else:
                    to_insert[label[0]] = temp
            elif label[1] == 1:  # users
                to_insert[label[0]] = temp['email']
            else:  # files
                if len(temp["versions"]) == 0:
                    continue
                to_insert[label[0] + "_filename"] = temp["versions"][-1]["fileName"]
                to_insert[label[0] + "_file_data"] = qb_connect \
                    .get_file(table_id, row["3"]["value"], key, temp['versions'][-1]["versionNumber"])
        # if nothing got added to to_insert, we don't want to try to insert
        if to_insert == {}:
            continue
        try:
            conn.execute(ins, to_insert)
            count += 1
            if count % 5000 == 0 and count > 0:
                print(str(count) + " entries have finished so far")
            if count % 75000 == 0 and count > 0:
                conn.close()
                _, conn = connect_to_db()
        except sqlalchemy.exc.DataError as e:
            print("Could not insert into sql data base the following is the error message: \n")
            print(e)
        except sqlalchemy.exc.IntegrityError:
            smtm = update(table).where(table.c.Record_ID == to_insert["Record_ID"]).values(to_insert)
            conn.execute(smtm)
            continue
    print(str(count) + " new entries into " + table_name + "\n")


# creates a table in the mySQL database, and fills it with entries based on tables and records in Quickbase
def create_and_fill_table(table_name, table_id, engine, conn, query="{1.GT.01-01-1900}"):
    drop_if_exists(table_name)
    table, field_ids = create_table(table_name, table_id, engine)
    if table is None or field_ids is None:
        return
    fill_table(table_name, table_id, conn, table, field_ids, query)


# copies the whole app from Quickbase to the mySQL database
def copy_from_qb(engine, conn, to_skip=()):
    tables = qb_connect.get_all_tables(os.environ.get("app_id"))
    drop_all_tables(engine)

    for table_info in tables:
        if table_info['name'] in to_skip:
            continue
        table_info['name'] = table_info['name'].upper().replace(" ", "_")
        create_and_fill_table(table_info["name"], table_info["id"], engine, conn)
    return


# updates a single row in in the mySQL database based on the record_id, and my even assign a new record_id based
# on if Quickbase decided to change it when we inserted
def update_row(table_name, table_id, record_id, fields, new_record_id=None):
    engine, conn = connect_to_db()
    meta = MetaData(engine)
    meta.reflect(engine)
    table = meta.tables.get(table_name)
    if table is None:
        print("table", table_name, "does not exist")
        return

    if new_record_id:
        data = qb_connect.get_data_set(table_id, list(fields.keys()), "{3.CT." + str(new_record_id['value']) + "}")
    else:
        data = qb_connect.get_data_set(table_id, list(fields.keys()), "{3.CT." + str(record_id) + "}")
    if data is None:
        conn.close()
        return
    data = data["data"]
    to_update = {}

    for col, val in data[0].items():

        try:
            to_update[table.c[fields[col]]] = val["value"]["email"]
        except TypeError:
            to_update[table.c[fields[col]]] = val["value"]
        except KeyError:
            to_update[table.c[fields[col] + "_filename"]] = val["value"]["versions"][-1]["fileName"]
            to_update[table.c[fields[col] + "_file_data"]] = qb_connect.get_file(table_id,
                                                                                 record_id, col,
                                                                                 val["value"]['versions'][-1][
                                                                                     "versionNumber"])
    stmt = table.update().values(to_update).where(table.c.Record_ID == record_id)
    try:
        conn.execute(stmt)
    except sqlalchemy.exc.IntegrityError:
        print("that's weird")
        print(to_update)
        pass
    conn.close()


def update_many_rows(table_name, fields_dict, data, table_id, rec_ids):
    engine, conn = connect_to_db()
    meta = MetaData()
    meta.reflect(engine)
    table = meta.tables.get(table_name)
    if table is None:
        print("there is no table", table_name)

    # delete records that exist in sql
    stmt = table.delete().where(table.c.Record_ID.in_(rec_ids))
    conn.execute(stmt)

    # replace deleted records
    count = 0
    for record in data:
        count += 1
        record_id = record["3"]
        to_update = {}
        for col, val in record.items():
            try:
                to_update[table.c[fields_dict[col]]] = val["value"]["email"]
            except TypeError:
                if type(val["value"]) == str and bad_char in val["value"]:
                    to_update[table.c[fields_dict[col]]] = val["value"].replace(bad_char, "_")
                else:
                    to_update[table.c[fields_dict[col]]] = val["value"]
            except KeyError:
                to_update[table.c[fields_dict[col] + "_filename"]] = val["value"]["versions"][-1]["fileName"]
                to_update[table.c[fields_dict[col] + "_file_data"]] = qb_connect \
                    .get_file(table_id, record_id, col, val["value"]['versions'][-1]["versionNumber"])

        smtm = table.insert().values(to_update)
        try:
            conn.execute(smtm)
        except sqlalchemy.exc.IntegrityError:
            continue
        if count % 2000 == 0:
            conn.close()
            _, conn = connect_to_db()

    conn.close()


def create_and_fill_multiple_tables(table_ids, engine, conn):
    for table in table_ids:
        table_name = qb_connect.get_table_name(table)
        if table_name is not None:
            create_and_fill_table(table_name, table, engine, conn)


# uniques is a list of strings that are the names of unique columns
def populate(sql, table_name, uniques, database=os.environ.get('mysql_database')):
    conn = pymysql.connect(host=os.environ.get('mysql_server'), user=os.environ.get('mysql_user'),
                           password=os.environ.get('mysql_password'), database=database,
                           cursorclass=pymysql.cursors.DictCursor)
    with conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
            insert_to_sql(result, table_name, uniques)


def insert_to_sql(result, table_name, uniques):

    engine, connection = connect_to_db()
    meta = MetaData(engine)
    meta.reflect(engine)
    print(len(result), table_name)
    table = meta.tables.get(table_name)
    if table is not None:
        for j, row in enumerate(result):
            if uniques != ():
                sql2 = "SELECT * FROM " + table_name + " WHERE"
                for i, col in enumerate(uniques):
                    if i > 0:
                        sql2 += " and"
                    if col == "Related_CONTACT" or col == "ladb_barcode":
                        sql2 += ' ' + col + ' = "' + str(row[col]) + '"'
                    else:
                        sql2 += " " + col + " = " + str(row[col])
                sql2 += ";"
                try:
                    conn = pymysql.connect(host=os.environ.get('mysql_server'), user=os.environ.get('mysql_user'),
                                           password=os.environ.get('mysql_password'),
                                           database=os.environ.get("mysql_database"),
                                           cursorclass=pymysql.cursors.DictCursor)
                    with conn:
                        with conn.cursor() as cursor:
                            cursor.execute(sql2)
                            if cursor.fetchall() == ():
                                stmt = table.insert().values(row)
                                connection.execute(stmt)

                except pymysql.err.ProgrammingError as e:
                    print("bad sql statement")
                    print(e)
            else:
                stmt = table.insert().values(row)
                connection.execute(stmt)
            if j % 2000 == 0 and j != 0:
                print(j, "records have been processed.")
                connection.close()
                _, connection = connect_to_db()
    connection.close()


def get_global_var(item):
    conn = pymysql.connect(host=os.environ.get('mysql_server'), user=os.environ.get('mysql_user'),
                           password=os.environ.get('mysql_password'), database="LADBOPS")
    with conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT value FROM ladb_globals WHERE item = '" + item + "';")
            result = cursor.fetchall()
    return result[0][0]


def update_global(value, item):
    conn = pymysql.connect(host=os.environ.get('mysql_server'), user=os.environ.get('mysql_user'),
                           password=os.environ.get('mysql_password'), database="LADBOPS")
    with conn:
        with conn.cursor() as cursor:
            sql = "UPDATE ladb_globals SET value = '" + str(value) + \
                  "' WHERE item = '" + item + "';"
            cursor.execute(sql)
            conn.commit()


def drop_if_exists(table_name):
    """ Drops the table from the mySQL database if a table with the name table_name exists """
    engine, conn = connect_to_db()
    meta = MetaData()
    meta.reflect(engine)
    tables = meta.tables
    table = tables.get(table_name)
    if table is not None:
        table.drop(engine)
    conn.close()


def create_qb_vault_table(table_name, table_id):
    """ creates a not readable version of a quickbase table in the mySQL database """
    engine, conn = connect_to_db()
    meta = MetaData()
    meta.reflect(engine)
    fields = qb_connect.get_all_fields(table_id)
    table = None
    for field in fields:
        if valid_field(field):
            table = Table(table_name, meta,
                          Column(str(field['id']), MEDIUMTEXT), extend_existing=True)
    if table is not None:
        table.create(engine, checkfirst=True)


def copy_to_qb_sql_table(table_id, qb_table_name, old_table_name=None):
    """ copies all of the records from the Quickbase table to a table in the mySQL database
        This table is not very readable, used more for fast imports/exports """
    engine, conn = connect_to_db()
    meta = MetaData()
    meta.reflect(engine)
    tables = meta.tables
    table = tables.get(qb_table_name)
    if old_table_name is None:
        old_table_name = qb_connect.get_table_name(table_id)
    rec_ids = get_record_ids(old_table_name)

    fields = qb_connect.get_all_fields(table_id)
    valid_fields = {}
    for field in fields:
        if valid_field(field) and field['label'] != "CONTACT - COMPANY - name" \
                and field["mode"] != "summary" and field['label'] != "acquired_from":
            valid_fields[field["label"].replace("#", "").replace(" ", "_")] = (field["id"],
                                                                               qb_connect.field_type(field))

    for rec in rec_ids:
        body = qb_connect.get_insert_body(old_table_name, valid_fields, rec, for_vault_transfer=True)
        insert_row(table, conn, body)


def migrate_to_qb(table_name, table_id):
    sql = "SELECT * FROM " + table_name + "; "
    conn = pymysql.connect(host=os.environ.get('mysql_server'), user=os.environ.get('mysql_user'),
                           password=os.environ.get('mysql_password'), database=os.environ.get("mysql_database"),
                           cursorclass=pymysql.cursors.DictCursor)
    with conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            row = cursor.fetchall()
    data = []
    for r in list(row):
        data += [{str(i): ast.literal_eval(str(r[i])) for i in r if r[i] and i != "3"}]
        if len(data) > 1500:
            print("1500 done")
            qb_connect.update_record(table_id, data)
            data = []
    if data:
        qb_connect.update_record(table_id, data)


def empty_table(table_name):
    conn = pymysql.connect(host=os.environ.get('mysql_server'), user=os.environ.get('mysql_user'),
                           password=os.environ.get('mysql_password'), database=os.environ.get("mysql_database"),
                           cursorclass=pymysql.cursors.DictCursor)
    with conn:
        with conn.cursor() as cursor:
            cursor.execute("TRUNCATE " + table_name + "; ")


def table_exists(table_name):
    engine, conn = connect_to_db()
    meta = MetaData()
    meta.reflect(engine)
    tables = meta.tables
    table = tables.get(table_name)
    return table


def create_table_mongo(table_name, fields):
    engine, conn = connect_to_db()
    meta = MetaData()
    table = None
    if table_name not in meta.tables.keys():
        for field in fields:
            table = Table(table_name, meta,
                          Column(field, MEDIUMTEXT), extend_existing=True)
        table.create(engine, checkfirst=True)
    return table


def main():
    engine, conn = connect_to_db()
    meta = MetaData()
    meta.reflect(engine)
    tables = ['Document Templates', "Document Subtables"]
    copy_from_qb(engine, conn, to_skip=tables)
    conn.close()


def testing():
    engine, conn = connect_to_db()
    create_and_fill_table("CONTACTS", os.environ.get("contacts_id"), engine, conn)
    # create_and_fill_table("ADDRESSES_COMPANIES", os.environ.get("addresses_companies_id"), engine, conn)
    create_and_fill_table("CONTACTS_ADDRESSES", os.environ.get("contacts_addresses_id"), engine, conn)
    # create_and_fill_table("FULL_CONTACTS", os.environ.get("full_contacts_id"), engine, conn)
    # create_and_fill_table("WORK_ORDERS", os.environ.get("work_orders_id"), engine, conn)
    # create_and_fill_table("WORK_ORDERS_CONTACTS_AND_ADDRESSES",
    # os.environ.get("work_orders_contacts_and_addresses_id"), engine, conn)
    # create_and_fill_table("WORK_ORDERS_FULL_CONTACTS", os.environ.get("work_orders_full_contacts_id"), engine, conn)
    create_and_fill_table("DELIVERY_RECEIPTS", os.environ.get("delivery_receipts_id"), engine, conn)
    # create_and_fill_table("DELIVERY_RECEIPTS_FULL_CONTACTS", os.environ.get("delivery_receipts_full_contacts_id")
    #                       , engine, conn)
    create_and_fill_table("COMPANIES", os.environ.get("companies_id"), engine, conn)
    create_and_fill_table("ADDRESSES", os.environ.get("addresses_id"), engine, conn)
    # create_and_fill_table("VAULT_jamelle_test_3", os.environ.get("vault_id"), engine, conn)
    # create_and_fill_table("VAULT_MOVEMENTS", os.environ.get("vault_movements_id"), engine, conn)
    # create_and_fill_table("VAULT_FULL_CONTACTS", os.environ.get("vault_full_contacts_id"), engine, conn)
    # create_and_fill_table("STAFF_MEMBERS", os.environ.get("staff_members_id"), engine, conn)
    # conn.close()
    # rows = obtain_delivery("100877")
    # for row in rows:
    #     print(row["toText_wo_number"])
    # conn = pymysql.connect(host=os.environ.get('mysql_server'), user=os.environ.get('mysql_user'),
    #                        password=os.environ.get('mysql_password'),
    #                        cursorclass=pymysql.cursors.DictCursor)
    # with conn:
    #     with conn.cursor() as cursor:
    #         # sql = "DROP TABLE LADB_QB_DEV.VAULT_MOVEMENTS;"
    #         # cursor.execute(sql)
    #         sql2 = "CREATE TABLE LADB_QB_DEV.VAULT_MOVEMENTS SELECT * FROM LADB_QB_PROD.VAULT_MOVEMENTS;"
    #         cursor.execute(sql2)
    # get_global_var("vault_check_in_last_updated")

    # create_qb_vault_table("vault_for_quickbase", os.environ.get("vault_id"))
    # copy_to_qb_sql_table(os.environ.get("vault_id"), "vault_for_quickbase")
    # migrate_to_qb()
    # i need to update the vault table with all of the new record ids so that vault movements is correct
    # create_qb_vault_table("vault_movements_for_quickbase", os.environ.get("vault_movements_id"))

    print('\uFFFD')


if __name__ == '__main__':
    testing()

