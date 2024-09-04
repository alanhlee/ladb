from scripts.al import mysql_connect
import os
from dotenv import load_dotenv

load_dotenv()


def main():

    dict_prod = {'TASKS': os.environ.get('p_tasks_id'),
                 'CONTACTS': os.environ.get('p_contacts_id'),
                 'CONTACTS_ADDRESSES': os.environ.get('p_contact_addresses_id'),
                 'ADDRESSES': os.environ.get('p_addresses_id'),
                 'COMPANIES': os.environ.get('p_companies_id'),
                 'TITLES': os.environ.get('p_titles_id'),
                 'PROJECTS': os.environ.get('p_projects_id'),
                 'PROJECTS_TITLES': os.environ.get('p_projects_titles_id'),
                 'ASSET_NAMES': os.environ.get('p_asset_names_id'),
                 'TITLES_ASSET_NAMES': os.environ.get('p_titles_asset_names_id'),
                 'TITLES_CONTACTS': os.environ.get('p_vault_movements_id'),
                 'VAULT_MOVEMENTS': os.environ.get('p_vault_movements_id'),
                 'WORK_ORDERS_CONTACTS_AND_ADDRESSES': os.environ.get('p_work_orders_contacts_and_addresses_id'),
                 'WORK_ORDERS_LINE_ITEMS': os.environ.get('p_work_orders_line_items_id'),
                 'LTO': os.environ.get('p_lto_id')}

    for key, value in dict_prod.items():
        engine, connect = mysql_connect.connect_to_db()
        # will delete table if exists before creating/filling
        mysql_connect.create_and_fill_table(key, value, engine, connect, query='{1.GT.01-01-1900}')


if __name__ == '__main__':
    main()