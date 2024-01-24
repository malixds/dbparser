import psycopg2


def create_connection():
    connection = None
    connection = psycopg2.connect(
    host="localhost",
    port='5432',
    database="aqua3",
    user="postgres",
    password="1234"
    )
    return connection

def send_sql_query(query):
    # Подключение к базе данных
    db = create_connection()

    # Создание объекта cursor
    cursor = db.cursor()

    # Отправка SQL-запроса
    try:
        cursor.execute(query)
    except:
        return False
    res = True
    if 'SELECT' in query:
        res = cursor.fetchone()
    db.commit()


    # Закрытие соединения с базой данных
    db.close()
    return res

def check_availability(uid, table):
    db = create_connection()

    table = table.replace('public.', '')
    # Создание объекта cursor
    cursor = db.cursor()
    sql = "SELECT * FROM {} WHERE uid = '{}'".format(table, uid)
    cursor.execute(sql)
    row = cursor.fetchone()
    db.close()
    return row is not None

def check_availability_all_components(article):
    db = create_connection()

    # Создание объекта cursor
    cursor = db.cursor()
    sql = "SELECT * FROM all_components WHERE article = '{}'".format(article)
    cursor.execute(sql)
    row = cursor.fetchone()
    db.close()
    return row is not None

def create_component_query(component):
    if component['type'] == 'CPU':
        return create_cpu_query(component)
    elif component['type'] == 'RAM':
        return create_ram_query(component)
    elif component['type'] == 'SSD' or component['type'] == 'HDD':
        return create_drive_query(component)
    elif component['type'] == 'VGA' or component['type'] == 'GPU':
        return create_gpu_query(component)
    elif (component['type'] == 'NIC' or component['type'] == 'OCP') and component['table'] == 'nic':
        return create_nic_query(component)
    elif component['type'] == 'NIC' and component['table'] == 'netcard':
        return create_netcard_query(component)
    elif component['type'] == 'WFA':
        return create_wfa_query(component)
    elif component['type'] == 'CBL' or component['type'] == 'HDM':
        return create_cables_query(component)
    elif component['type'] == 'MRK':
        return create_mobile_rack_query(component)
    elif component['type'] == 'ODD':
        return create_optical_drive_query(component)
    elif component['type'] == 'BRB':
        return create_barebone_laptop_query(component)
    elif (component['type'] == 'KEY' or component['type'] == 'MOU' or component['type'] == 'KMK' or component['type'] == 'OTR'
          or component['type'] == 'JBD' or component['type'] == 'TAB' or component['type'] == 'KPK' or component['type'] == 'FAN'
          or component['type'] == 'CPC' or component['type'] == 'LTE' or component['type'] == 'DOC'):
        return create_peripherals_query(component)
    elif component['type'] == 'CAS':
        return create_case_query(component)
    elif component['type'] == 'SFT':
        return create_server_software_query(component)
    elif (component['type'] == 'HBA' or component['type'] == 'RDC') and component['table'] == 'raid':
        return create_raid_query(component)
    elif component['type'] == 'HBA' and component['table'] == 'fc_adapter':
        return create_fc_adapter_query(component)
    elif (component['type'] == 'PSU'):
        return create_psu_query(component)
    elif (component['type'] == 'CAS'):
        return create_case_query(component)


def create_all_query(component):
    uid = component['UID']
    name = component['name']
    power = component['power']
    type_of = component['type']
    article = component['article']
    if type(power) is int:
        query = 'INSERT INTO all_components (uid, name, power, type, article) VALUES (\'{}\', \'{}\', {}, \'{}\', \'{}\');'.format(uid, name, power, type_of, article)
    else:
        query = 'INSERT INTO all_components (uid, name, power, type, article) VALUES (\'{}\', \'{}\', {}, \'{}\', \'{}\');'.format(uid, name, 0, type_of, article)
    return query


def create_netcard_query(component):
    uid = component['UID']
    name = component['name']
    power = component['power']
    vendor_code = component['article']
    if type(power) == int:
        query = 'INSERT INTO netcard (uid, name, power, vendor_code) VALUES (\'{}\', \'{}\', {}, \'{}\');'.format(
            uid, name, power, vendor_code)
    else:
        query = 'INSERT INTO netcard (uid, name, vendor_code) VALUES (\'{}\', \'{}\', \'{}\');'.format(
            uid, name, vendor_code)
    return query

def create_fc_adapter_query(component):
    uid = component['UID']
    name = component['name']
    power = component['power']
    slot_id = component['slot_id']
    capacity = component['capacity']
    port_type = component['port_type']
    article = component['article']
    if type(power) == int:
        query = 'INSERT INTO fc_adapter (uid, name, power, slot_id, capacity, port_type, vendor_code) VALUES (\'{}\', \'{}\', {}, {}, {}, \'{}\', \'{}\');'.format(
            uid, name, power, slot_id, capacity, port_type, article)
    else:
        query = 'INSERT INTO fc_adapter (uid, name, slot_id, capacity, port_type, vendor_code) VALUES (\'{}\', \'{}\', {}, {}, \'{}\', \'{}\');'.format(
            uid, name, slot_id, capacity, port_type, article)
    return query


def create_raid_query(component):
    uid = component['UID']
    name = component['name']
    power = component['power']
    cost = component['cost']
    gpl = component['gpl']
    slot_id = component['slot_id']
    int = component['int']
    ext = component['ext']
    comp_type = component['type_cntrl']
    article = component['article']
    if type(power) == int:
        query = 'INSERT INTO raid (uid, name, power, cost, gpl, slot_id, int_slots, ext_slots, type, vendor_code) VALUES (\'{}\', \'{}\', {}, {}, {}, {}, {}, {}, {}, \'{}\');'.format(
            uid, name, power, cost, gpl, slot_id, int, ext, comp_type, article)
    else:
        query = 'INSERT INTO raid (uid, name, cost, gpl, slot_id, int_slots, ext_slots, type, vendor_code) VALUES (\'{}\', \'{}\', {}, {}, {}, {}, {}, {}, \'{}\');'.format(
            uid, name, cost, gpl, slot_id, int, ext, comp_type, article)
    return query

def create_cpu_query(component):
    uid = component['UID']
    name = component['name']
    power = component['power']
    cost = component['cost']
    gpl = component['gpl']
    article = component['article']
    query = 'INSERT INTO cpu (uid, name, power, cost, gpl, vendor_code) VALUES (\'{}\', \'{}\', {}, {}, {}, \'{}\');'.format(uid, name, power, cost, gpl, article)
    return query

def create_ram_query(component):
    uid = component['UID']
    name = component['name']
    power = component['power']
    clock = component['clock']
    amount = component['amount']
    cost = component['cost']
    gpl = component['gpl']
    article = component['article']
    query = 'INSERT INTO ram (uid, name, power, cost, gpl, clock, amount, vendor_code) VALUES (\'{}\', \'{}\', {}, {}, {}, {}, {}, \'{}\');'.format(uid, name, power, cost, gpl, clock, amount, article)
    return query


def create_drive_query(component):
    uid = component['UID']
    name = component['name']
    power = component['power']
    capacity = component['capacity']
    type_id = component['type_id']
    group_id = component['group_id']
    slot_id = component['slot_id']
    size = component['size']
    cost = component['cost']
    gpl = component['gpl']
    article = component['article']
    query = 'INSERT INTO drives (uid, name, size, power, cost, gpl, type_id, group_id, slot_id, capacity, vendor_code) VALUES (\'{}\', \'{}\', \'{}\', {}, {}, {}, {}, {}, {}, {}, \'{}\');'.format(uid, name, size, power, cost, gpl, type_id, group_id, slot_id, capacity, article)
    return query

def create_gpu_query(component):
    uid = component['UID']
    name = component['name']
    power = component['power']
    cost = component['cost']
    gpl = component['gpl']
    slot_id = component['slot_id']
    article = component['article']
    query = 'INSERT INTO gpu (uid, name, power, cost, gpl, slot_id, vendor_code) VALUES (\'{}\', \'{}\', {}, {}, {}, {}, \'{}\');'.format(uid, name, power, cost, gpl, slot_id, article)
    return query

def create_nic_query(component):
    uid = component['UID']
    name = component['name']
    power = component['power']
    cost = component['cost']
    gpl = component['gpl']
    slot_id = component['slot_id']
    article = component['article']
    if slot_id != 0:
        query = 'INSERT INTO nic (uid, name, cost, power, gpl, slot_id, vendor_code) VALUES (\'{}\', \'{}\', {}, {}, {}, \'{}\');'.format(uid, name, cost, power, gpl, slot_id, article)
    else:
        query = 'INSERT INTO nic (uid, name, cost, power, gpl, vendor_code) VALUES (\'{}\', \'{}\', {}, {}, \'{}\');'.format(uid, name, cost, power, gpl, article)
    return query


def create_wfa_query(component):
    uid = component['UID']
    name = component['name']
    power = component['power']
    cost = component['cost']
    gpl = component['gpl']
    article = component['article']
    query = 'INSERT INTO wifi_adapter (uid, name, power, cost, gpl, vendor_code) VALUES (\'{}\', \'{}\', {}, {}, {}, \'{}\');'.format(uid, name, power, cost, gpl, article)
    return query


def create_cables_query(component):
    uid = component['UID']
    name = component['name']
    power = component['power']
    type_id = component['type_id']
    article = component['article']
    table = component['table']
    if table == 'cables':
        query = 'INSERT INTO {} (uid, name, type_id, vendor_code) VALUES (\'{}\', \'{}\', {}, {});'.format(table, uid, name, type_id, article)
    else:
        query = 'INSERT INTO {} (uid, name, vendor_code) VALUES (\'{}\', \'{}\', \'{}\');'.format(table, uid, name, article)
    return query


def create_mobile_rack_query(component):
    uid = component['UID']
    name = component['name']
    power = component['power']
    cost = component['cost']
    gpl = component['gpl']
    article = component['article']
    query = 'INSERT INTO mobile_rack (uid, name, power, cost, gpl, vendor_code) VALUES (\'{}\', \'{}\', {}, {}, {}, \'{}\');'.format(uid, name, power, cost, gpl, article)
    return query


def create_barebone_laptop_query(component):
    uid = component['UID']
    name = component['name']
    vendor = component['article']
    query = 'INSERT INTO barebone_laptop (uid, name, vendor_code) VALUES (\'{}\', \'{}\', {});'.format(uid, name, vendor)
    return query


def create_optical_drive_query(component):
    uid = component['UID']
    name = component['name']
    power = component['power']
    cost = component['cost']
    gpl = component['gpl']
    article = component['article']
    query = 'INSERT INTO optical_drive (uid, name, power, cost, gpl, vendor_code) VALUES (\'{}\', \'{}\', {}, {}, {}, \'{}\');'.format(uid, name, power, cost, gpl, article)
    return query


def create_peripherals_query(component):
    uid = component['UID']
    name = component['name']
    power = component['power']
    cost = component['cost']
    gpl = component['gpl']
    table = component['table']
    article = component['article']
    query = 'INSERT INTO {} (uid, name, power, cost, gpl, vendor_code) VALUES (\'{}\', \'{}\', {}, {}, {}, \'{}\');'.format(table, uid, name, power, cost, gpl, article)
    return query


def create_psu_query(component, fake=False):
    uid = component['UID']
    name = component['name']
    power = component['power']
    article = component['article']
    query = 'INSERT INTO psu (uid, name, fake, vendor_code) VALUES (\'{}\', \'{}\', {}, \'{}\');'.format(uid, name, fake, article)
    return query

def create_case_query(component):
    uid = component['UID']
    name = component['name']
    article = component['article']
    query = 'INSERT INTO public."case" (uid, name, vendor_code) VALUES (\'{}\', \'{}\', \'{}\');'.format(uid, name, article)
    if component['psu_id'] != 1:
        psu_id = component['psu_id']
        query = 'INSERT INTO public."case" (uid, name, psu_id, vendor_code) VALUES (\'{}\', \'{}\', \'{}\', \'{}\');'.format(uid, name, psu_id, article)
        query_psu = create_psu_query(component, fake=True)
        send_sql_query(query_psu)
    return query


def create_server_software_query(component):
    uid = component['UID']
    name = component['name']
    article = component['article']
    try:
        soft_type = component['soft_type']
        query = 'INSERT INTO server_software (uid, name, type, full_name, vendor_code) VALUES (\'{}\', \'{}\', \'{}\', \'{}\', \'{}\');'.format(uid, name, soft_type, name, article)
        return query
    except KeyError:
        query = 'INSERT INTO server_software (uid, name, full_name, vendor_code) VALUES (\'{}\', \'{}\', \'{}\', \'{}\');'.format(uid, name, name, article)
        return query

def add_commodity_to_db(component):
    for plat in component['valid_platform']:
        create_component_platform_commodity_query(plat, component['UID'], component['table'])
        if component['type'] == 'CAS' and component['power'] is not None:
            create_component_platform_commodity_query(plat, component['UID'], 'psu')

def get_plat_id(name):
    plat_id_query = 'SELECT id FROM platform WHERE name = \'{}\';'.format(name)
    res = send_sql_query(plat_id_query)
    if res is None:
        return res
    return res[0]

def create_component_platform_commodity_query(plat, uid_cpu, table):
    plat_id = get_plat_id(plat)
    table = table.replace('public.', '').replace('"', '')
    if not plat_id:
        return None
    if not check_component_platform_commodity(plat_id, uid_cpu, table):
        res_query = 'INSERT INTO platform_{} (platform_id, {}_uid) VALUES (\'{}\',\'{}\')'.format(table, table,
                                                                                                  plat_id, uid_cpu)
        if table == 'jbod':
            res_query = 'INSERT INTO platform_{} (platform_id, {}_uid) VALUES (\'{}\',\'{}\')'.format('backplane',
                                                                                                      table, plat_id,
                                                                                                      uid_cpu)

        db = create_connection()

        cursor = db.cursor()
        cursor.execute(res_query)
        db.commit()
        db.close()


def check_component_platform_commodity(plat_id, uid_com, table):
    db = create_connection()

    # Создание объекта cursor
    cursor = db.cursor()
    table = table.replace('public.', '').replace('"', '')
    sql = "SELECT * FROM platform_{} WHERE {}_uid = \'{}\' AND platform_id = \'{}\';".format(table, table, uid_com, plat_id)
    if table == 'jbod':
        sql = "SELECT * FROM platform_{} WHERE {}_uid = \'{}\' AND platform_id = \'{}\';".format('backplane', table, uid_com,
                                                                                                plat_id)
    cursor.execute(sql)
    row = cursor.fetchone()
    db.close()
    return row is not None


def create_cost_query(component):
    uid = component['UID']
    cost = component['cost']
    gpl = component['gpl']
    table = component['table']
    query = "UPDATE {} SET cost = {}, gpl = {} WHERE UID = '{}'".format(table, cost, gpl, uid)
    return query


def remove_commodity(plat, uid_com, table):
    plat_id = get_plat_id(plat)
    query = "DELETE FROM platform_{} WHERE {}_uid = '{}' AND platform_id = '{}';".format(table, table, uid_com, plat_id)
    if table == 'jbod':
        "DELETE FROM platform_{} WHERE {}_uid = '{}' AND platform_id = '{}';".format('backplane', table, uid_com, plat_id)

    send_sql_query(query)


def refactor_to_update_query(query):
    query = query.replace('INSERT INTO', 'UPDATE')
    query = query.replace('VALUES', '=')
    query = query.replace('uid, ', '')
    query = query.replace(' (', ' SET (', 1)
    uid_index = query.find('AQ') - 1
    uid_index2 = uid_index
    while query[uid_index2] != ',':
        uid_index2 += 1
    uid = query[uid_index:uid_index2+2]
    query = query.replace(uid, '')
    query = query[:-1]
    uid = uid[:-2]
    uid_res = ' WHERE uid = {};'.format(uid)
    query = query + uid_res
    return query