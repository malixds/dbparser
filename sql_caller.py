import psycopg2

def send_sql_query(query):
    # Подключение к базе данных
    db = psycopg2.connect(
        host="localhost",
        port='5432',
        database="aqua2",
        user="postgres",
        password="1234"
    )

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
    db = psycopg2.connect(
        host="localhost",
        port='5432',
        database="aqua2",
        user="postgres",
        password="1234"
    )

    table = table.replace('public.', '')
    # Создание объекта cursor
    cursor = db.cursor()
    sql = "SELECT * FROM {} WHERE uid = '{}'".format(table, uid)
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
    elif component['type'] == 'VGA':
        return create_gpu_query(component)
    elif component['type'] == 'NIC' or component['type'] == 'OCP':
        return create_nic_query(component)
    elif component['type'] == 'WFA':
        return create_wfa_query(component)
    elif component['type'] == 'CBL' or component['type'] == 'HDM':
        return create_cables_query(component)
    elif component['type'] == 'MRK':
        return create_mobile_rack_query(component)
    elif component['type'] == 'ODD':
        return create_optical_drive_query(component)
    elif (component['type'] == 'KEY' or component['type'] == 'MOU' or component['type'] == 'KMK'
          or component['type'] == 'JBD' or component['type'] == 'TAB'):
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
        query = 'INSERT INTO all_components (uid, name, type, article) VALUES (\'{}\', \'{}\', \'{}\', \'{}\');'.format(uid, name, type_of, article)
    return query


def create_fc_adapter_query(component):
    uid = component['UID']
    name = component['name']
    power = component['power']
    slot_id = component['slot_id']
    capacity = component['capacity']
    port_type = component['port_type']
    if type(power) == int:
        query = 'INSERT INTO fc_adapter (uid, name, power, slot_id, capacity, port_type) VALUES (\'{}\', \'{}\', {}, {}, {}, \'{}\');'.format(
            uid, name, power, slot_id, capacity, port_type)
    else:
        query = 'INSERT INTO fc_adapter (uid, name, slot_id, capacity, port_type) VALUES (\'{}\', \'{}\', {}, {}, \'{}\');'.format(
            uid, name, slot_id, capacity, port_type)
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
    if type(power) == int:
        query = 'INSERT INTO raid (uid, name, power, cost, gpl, slot_id, int_slots, ext_slots, type) VALUES (\'{}\', \'{}\', {}, {}, {}, {}, {}, {}, {});'.format(
            uid, name, power, cost, gpl, slot_id, int, ext, comp_type)
    else:
        query = 'INSERT INTO raid (uid, name, cost, gpl, slot_id, int_slots, ext_slots, type) VALUES (\'{}\', \'{}\', {}, {}, {}, {}, {}, {});'.format(
            uid, name, cost, gpl, slot_id, int, ext, comp_type)
    return query

def create_cpu_query(component):
    uid = component['UID']
    name = component['name']
    power = component['power']
    cost = component['cost']
    gpl = component['gpl']
    if type(power) == int:
        query = 'INSERT INTO cpu (uid, name, power, cost, gpl) VALUES (\'{}\', \'{}\', {}, {}, {});'.format(uid, name, power, cost, gpl)
    else:
        query = 'INSERT INTO cpu (uid, name, cost, gpl) VALUES (\'{}\', \'{}\', {}, {});'.format(uid, name, cost, gpl)
    return query

def create_ram_query(component):
    uid = component['UID']
    name = component['name']
    power = component['power']
    clock = component['clock']
    amount = component['amount']
    cost = component['cost']
    gpl = component['gpl']
    if type(power) == int:
        query = 'INSERT INTO ram (uid, name, power, cost, gpl, clock, amount) VALUES (\'{}\', \'{}\', {}, {}, {}, {}, {});'.format(uid, name, power, cost, gpl, clock, amount)
    else:
        query = 'INSERT INTO ram (uid, name, cost, gpl, clock, amount) VALUES (\'{}\', \'{}\', {}, {}, {}, {});'.format(uid, name, cost, gpl, clock, amount)
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
    if type(power) == int:
        query = 'INSERT INTO drives (uid, name, size, power, cost, gpl, type_id, group_id, slot_id, capacity) VALUES (\'{}\', \'{}\', \'{}\', {}, {}, {}, {}, {}, {}, {});'.format(uid, name, size, power, cost, gpl, type_id, group_id, slot_id, capacity)
    else:
        query = 'INSERT INTO drives (uid, name, size, cost, gpl, type_id, group_id, slot_id, capacity) VALUES (\'{}\', \'{}\', \'{}\', {}, {}, {}, {}, {}, {});'.format(uid, name, size, cost, gpl, type_id, group_id, slot_id, capacity)
    return query

def create_gpu_query(component):
    uid = component['UID']
    name = component['name']
    power = component['power']
    cost = component['cost']
    gpl = component['gpl']
    slot_id = component['slot_id']
    if type(power) == int:
        query = 'INSERT INTO gpu (uid, name, power, cost, gpl, slot_id) VALUES (\'{}\', \'{}\', {}, {}, {}, {});'.format(uid, name, power, cost, gpl, slot_id)
    else:
        query = 'INSERT INTO gpu (uid, name, cost, gpl, slot_id) VALUES (\'{}\', \'{}\', {}, {}, {});'.format(uid, name, cost, gpl, slot_id)
    return query

def create_nic_query(component):
    uid = component['UID']
    name = component['name']
    power = component['power']
    cost = component['cost']
    gpl = component['gpl']
    slot_id = component['slot_id']
    if type(power) == int and slot_id != 0:
        query = 'INSERT INTO nic (uid, name, power, cost, gpl, slot_id) VALUES (\'{}\', \'{}\', {}, {}, {}, {});'.format(uid, name, power, cost, gpl, slot_id)
    elif type(power) == int:
        query = 'INSERT INTO nic (uid, name, power, cost, gpl) VALUES (\'{}\', \'{}\', {}, {}, {});'.format(uid, name, power, cost, gpl)
    elif slot_id != 0:
        query = 'INSERT INTO nic (uid, name, cost, gpl, slot_id) VALUES (\'{}\', \'{}\', {}, {}, {});'.format(uid, name, cost, gpl, slot_id)
    else:
        query = 'INSERT INTO nic (uid, name, cost, gpl) VALUES (\'{}\', \'{}\', {}, {});'.format(uid, name, cost, gpl)
    return query


def create_wfa_query(component):
    uid = component['UID']
    name = component['name']
    power = component['power']
    cost = component['cost']
    gpl = component['gpl']
    if type(power) == int:
        query = 'INSERT INTO wifi_adapter (uid, name, power, cost, gpl) VALUES (\'{}\', \'{}\', {}, {}, {});'.format(uid, name, power, cost, gpl)
    else:
        query = 'INSERT INTO wifi_adapter (uid, name, cost, gpl) VALUES (\'{}\', \'{}\', {}, {});'.format(uid, name, cost, gpl)
    return query


def create_cables_query(component):
    uid = component['UID']
    name = component['name']
    power = component['power']
    cost = component['cost']
    gpl = component['gpl']
    type_id = component['type_id']
    if type(power) == int:
        query = 'INSERT INTO cables (uid, name, power, cost, gpl, type_id) VALUES (\'{}\', \'{}\', {}, {}, {}, {});'.format(uid, name, power, cost, gpl, type_id)
    else:
        query = 'INSERT INTO cables (uid, name, cost, gpl, type_id) VALUES (\'{}\', \'{}\', {}, {}, {});'.format(uid, name, cost, gpl, type_id)
    return query


def create_mobile_rack_query(component):
    uid = component['UID']
    name = component['name']
    power = component['power']
    cost = component['cost']
    gpl = component['gpl']
    if type(power) == int:
        query = 'INSERT INTO mobile_rack (uid, name, power, cost, gpl) VALUES (\'{}\', \'{}\', {}, {}, {});'.format(uid, name, power, cost, gpl)
    else:
        query = 'INSERT INTO mobile_rack (uid, name, cost, gpl) VALUES (\'{}\', \'{}\', {}, {});'.format(uid, name, cost, gpl)
    return query


def create_optical_drive_query(component):
    uid = component['UID']
    name = component['name']
    power = component['power']
    cost = component['cost']
    gpl = component['gpl']
    if type(power) == int:
        query = 'INSERT INTO optical_drive (uid, name, power, cost, gpl) VALUES (\'{}\', \'{}\', {}, {}, {});'.format(uid, name, power, cost, gpl)
    else:
        query = 'INSERT INTO optical_drive (uid, name, cost, gpl) VALUES (\'{}\', \'{}\', {}, {});'.format(uid, name, cost, gpl)
    return query


def create_peripherals_query(component):
    uid = component['UID']
    name = component['name']
    power = component['power']
    cost = component['cost']
    gpl = component['gpl']
    table = component['table']
    if type(power) == int and power > 0:
        query = 'INSERT INTO {} (uid, name, power, cost, gpl) VALUES (\'{}\', \'{}\', {}, {}, {});'.format(table, uid, name, power, cost, gpl)
    else:
        query = 'INSERT INTO {} (uid, name, cost, gpl) VALUES (\'{}\', \'{}\', {}, {});'.format(table, uid, name, cost, gpl)
    return query


def create_psu_query(component, fake=False):
    uid = component['UID']
    name = component['name']
    power = component['power']
    if type(power) == int and power > 0:
        query = 'INSERT INTO psu (uid, name, power, fake) VALUES (\'{}\', \'{}\', {}, {});'.format(uid, name, power, fake)
    else:
        query = 'INSERT INTO psu (uid, name, fake) VALUES (\'{}\', \'{}\', {});'.format(uid, name, fake)
    return query

def create_case_query(component):
    uid = component['UID']
    name = component['name']
    query = 'INSERT INTO public."case" (uid, name) VALUES (\'{}\', \'{}\');'.format(uid, name)
    if component['psu_id'] != 1:
        psu_id = component['psu_id']
        query = 'INSERT INTO public."case" (uid, name, psu_id) VALUES (\'{}\', \'{}\', \'{}\');'.format(uid, name, psu_id)
        query_psu = create_psu_query(component, fake=True)
        send_sql_query(query_psu)
    return query


def create_server_software_query(component):
    uid = component['UID']
    name = component['name']
    try:
        soft_type = component['soft_type']
        query = 'INSERT INTO server_software (uid, name, type, full_name) VALUES (\'{}\', \'{}\', \'{}\', \'{}\');'.format(uid, name, soft_type, name)
        return query
    except KeyError:
        query = 'INSERT INTO server_software (uid, name, full_name) VALUES (\'{}\', \'{}\', \'{}\');'.format(uid, name, name)
        return query

def add_commodity_to_db(component):
    for plat in component['valid_platform']:
        create_component_platform_commodity_query(plat, component['UID'], component['table'])

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

        db = psycopg2.connect(
            host="localhost",
            port='5432',
            database="aqua2",
            user="postgres",
            password="1234"
        )

        cursor = db.cursor()
        cursor.execute(res_query)
        db.commit()
        db.close()


def check_component_platform_commodity(plat_id, uid_com, table):
    db = psycopg2.connect(
        host="localhost",
        port='5432',
        database="aqua2",
        user="postgres",
        password="1234"
    )

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