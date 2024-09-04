from enum import Enum


class OPERATIONS(Enum):
    ValueEquals = 'EX'
    NotValueEquals = 'XEX'
    Equals = 'CT'
    NotEquals = 'XCT'
    StartsWith = 'SW'
    NotStartsWith = 'XSW'
    ExistsInList = 'HAS'
    GreaterThan = 'GT'
    GreaterThanOrEqual = 'GTE'
    LessThan = 'LT'
    LessThanOrEqual = 'LTE'
    Before = 'BF'
    OnBefore = 'OBF'
    After = 'AF'
    OnAfter = 'OAF'
    InRange = 'IR'
    NotInRange = 'XIR'


class COMPANY(Enum):
    Date_Created = 1
    Date_Modified = 2
    Record_ID = 3
    Record_Owner = 4
    Last_Modified_By = 5
    # begin custom fields
    main_phone = 6
    website = 7
    support_email = 8
    support_phone = 9
    name = 10
    CONTACTS = 17
    ADDRESS_COMPANIES = 23
    CONTACT_ADDRESS_COMPANIES = 28
    WORK_ORDERS = 2


class WORK_ORDERS(Enum):
    Date_Created = 1
    Date_Modified = 2
    Record_ID = 3
    Record_Owner = 4
    Last_Modified_By = 5
    # begin custom fields
    production_status = 6
    WORK_ORDER_LINE_ITEMS = 7
    PROJECT = 14
    project_number = 16
    DOCUMENTS = 21
    TASKS = 23
    NOTES = 25
    REDOS = 29
    STAFF_MEMBER = 36
    ASSET = 115
    wo_number = 121
    WORK_ORDER_TITLES = 123
    WORK_ORDER_STAFFS = 125
    WORK_ORDER_VAULTS = 127
    description = 152
    billing_status = 154
    quick_qc = 169
    current_user = 170
    check_in = 171
    check_out = 172
    check_in_out_status_code = 173
    num_of_redos = 177
    max_redo_date_created = 179
    WORK_ORDER_CONTACTS = 185
    department = 188
    department_code = 189
    combined_title_names = 205
    combined_line_item_names = 210
    DELIVERY_RECEIPT_WORK_ORDERS = 220
    combined_notes = 225
    PAYLOADS = 229
    work_order_title = 257


class CONTACTS(Enum):
    Date_Created = 1
    Date_Modified = 2
    Record_ID = 3
    Record_Owner = 4
    Last_Modified_By = 5
    # begin custom fields
    email = 6
    phone = 7
    mobile = 8
    first_name = 9
    last_name = 10
    contact_name = 11
    title_job_function = 12
    sales_rep = 35
    contact_type = 106
    main_phone = 90
    website = 93
    support_email = 91
    support_phone = 92

    ADDRESS_COMPANIES = 118
    CONTACT_ADDRESS_COMPANIES = 116
    WORK_ORDER_CONTACTS = 98


class CONTACT_ADDRESS_COMPANIES(Enum):
    Date_Created = 1
    Date_Modified = 2
    Record_ID = 3
    Record_Owner = 4
    Last_Modified_By = 5
    # begin custom fields
    # Need to actually fix the fields in here
    Related_CONTACT = 6
    contact_name = 7
    Related_COMPANY = 8
    COMPANY_name = 8
    Related_CONTACT_ADDRESS = 22


class ADDRESS_COMPANIES(Enum):
    Date_Created = 1
    Date_Modified = 2
    Record_ID = 3
    Record_Owner = 4
    Last_Modified_By = 5
    # begin custom fields
    # Need to actually fix the fields in here
    main_phone = 6
    website = 7
    support_email = 8
    support_phone = 9
    name = 10
    CONTACTS = 17
    ADDRESS_COMPANIES = 23
    CONTACT_ADDRESS_COMPANIES = 28
    WORK_ORDERS = 2


class DELIVERY_RECEIPTS(Enum):
    Date_Created = 1
    Date_Modified = 2
    Record_ID = 3
    Record_Owner = 4
    Last_Modified_By = 5
    # begin custom fields
    # Need to actually fix the fields in here
    receipt_number = 16
    status = 28
    sent_via = 39
    tracking_number = 40
    delivery_notes = 42
    contact_notes = 43
    DELIVERY_RECEIPT_DELIVERY_SIGNATURES = 82 # this is related to another table of N signatures
    signature = 130


class VAULT(Enum):
    Date_Created = 1
    Date_Modified = 2
    Record_ID = 3
    Record_Owner = 4
    Last_Modified_By = 5
    # begin custom fields
    ladb_barcode = 6
    location = 28
    description = 13
    STAFF_MEMBER = 23
    acquired_date = 30
    item = 8
    trt = 53
    client_barcode = 7
    audio = 54
    status = 21
    media_length = 55
    asset_serial_number = 52
    released_by = 39
    last_location = 111
    media_standard = 68
    title = 18
    acquired_from = 29


class VAULT_MOVEMENTS(Enum):
    Date_Created = 1
    Date_Modified = 2
    Record_ID = 3
    Record_Owner = 4
    Last_Modified_By = 5
    # begin custom fields
    Related_ASSET = 10
    ladb_barcode = 21
    current_location = 20
    previous_location = 19
    current_status = 18
    previous_status = 17
    notes = 8
    date_time = 9
    STAFF_MEMBER = 6


class TASKS(Enum):
    Date_Created = 1
    Date_Modified = 2
    Record_ID = 3
    Record_Owner = 4
    Last_Modified_By = 5
    # begin custom fields
    description = 6
    work_order_number = 14
    department = 15
    assigned_staff = 16
    task_status = 60
    project_name = 67
    start_date = 92
    due_date = 112


class WORK_ORDERS_LINE_ITEMS(Enum):
    Record_ID = 3
    Related_CHARGE_CODE = 14
    CHARGE_CODE_billing_code = 15
    CHARGE_CODE_billing_item = 16
    WORK_ORDER_wo_number = 28
    CHARGE_CODE_category = 56
    billing_code = 61
    billing_item = 62
    billing_category = 63
    WORK_ORDER_invoice_number = 76


class DELIVERIES(Enum):
    Date_Created = 1
    Date_Modified = 2
    Record_ID = 3
    Record_Owner = 4
    Last_Modified_By = 5
    delivery_id = 6
    receipt_numbers = 7
    signature = 8
    recipient = 9
    quick_notes = 10

class CHARGE_CODES(Enum):
    Date_Created = 1
    Date_Modified = 2
    Record_ID = 3
    Record_Owner = 4
    Last_Modified_By = 5
    name = 25
    description_invoice = 23
    description_internal = 24

class DEV_CHARGE_CODES(Enum):
    Date_Created = 1
    Date_Modified = 2
    Record_ID = 3
    Record_Owner = 4
    Last_Modified_By = 5
    billing_code = 6
    item = 7
    item_client = 22

class WORK_ORDERS_CONTACTS_AND_ADRESSES(Enum):
    Date_Created = 1
    Date_Modified = 2
    Record_ID = 3
    Record_Owner = 4
    Last_Modified_By = 5
    WORK_ORDER_wo_display_number = 40
    contact_name_Lookup = 60
    contact_type = 63
    CONTACT_COMPANY_name = 65
    contact_name_display = 66
    contact_mobile = 67
    contact_phone = 69
    CONTACT_ADDRESS_address_details = 72
    contact_address_type = 73
    contact_email = 88
    table_work_order_number = 89
    table_contact_name_display = 90
    table_contact_email = 91
    table_contact_phone = 92
    table_contact_mobile = 93
    table_contact_company_name = 94
    table_contact_address_type = 95
    table_contact_address_details = 96
