import psycopg2

def send_sql_query(query):
    # Подключение к базе данных
    db = psycopg2.connect(
        host="localhost",
        port='5432',
        database="test_parser",
        user="postgres",
        password="1234"
    )

    # Создание объекта cursor
    cursor = db.cursor()

    # Отправка SQL-запроса
    cursor.execute(query)
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
        database="test_parser",
        user="postgres",
        password="1234"
    )

    # Создание объекта cursor
    cursor = db.cursor()
    sql = "SELECT * FROM {} WHERE uid = %s;".format(table)
    cursor.execute(sql, (uid,))
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
    elif component['type'] == 'NIC':
        return create_nic_query(component)
    elif component['type'] == 'WFA':
        return create_wfa_query(component)
    elif component['type'] == 'CBL':
        return create_cables_query(component)

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


def add_commodity_to_db(component):
    for plat in component['valid_platform']:
        create_component_platform_commodity_query(plat, component['UID'], component['table'])

def create_component_platform_commodity_query(plat, uid_cpu, table):
    plat_id_query = 'SELECT id FROM platform WHERE name = \'{}\';'.format(plat)
    plat_id = send_sql_query(plat_id_query)
    if not plat_id:
        return None
    if not check_component_platform_commodity(plat_id[0], uid_cpu, table):
        res_query = 'INSERT INTO platform_{} (platform_id, {}_uid) VALUES (\'{}\',\'{}\')'.format(table, table, plat_id[0], uid_cpu)

        db = psycopg2.connect(
            host="localhost",
            port='5432',
            database="test_parser",
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
        database="test_parser",
        user="postgres",
        password="1234"
    )

    # Создание объекта cursor
    cursor = db.cursor()
    sql = "SELECT * FROM platform_{} WHERE {}_uid = \'{}\' AND platform_id = \'{}\';".format(table, table, uid_com, plat_id)
    cursor.execute(sql)
    row = cursor.fetchone()
    db.close()
    return row is not None