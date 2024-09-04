import qb_connect
import os
import requests
import json

# 6 asset tag
# 12 status [Deployed, Ready to Deploy, Decommissioned, Pending, Archived, In Testing, RMA, In Stock]
# 63 location
# 70 assigned staff
asset_records = qb_connect.get_data_set(os.environ.get("cella_assets"), [3, 6, 12, 63, 70], '{1.GT.01-01-1900}')


update_records = []
def cella_update():
    # print(entries)
    for record in asset_records['data']:
        record_id = record['3']
        asset_tag = record['6']
        status = record['12']
        location = record['63']
        assigned_staff = record['70']['value']
        # if assigned_staff is not None:
        #     print(assigned_staff['name'])



def user_prompts():
    user_assets_update = input("Which record(s) would you like to update? (comma delimited) ")
    # print(user_assets_update)
    for asset in user_assets_update.strip().split(','):
        update_records.append(int(asset))
    print(update_records)


def main():
    cella_update()


if __name__ == '__main__':
    main()