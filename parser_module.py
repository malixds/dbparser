import pandas as pd
import numpy as np
import sql_caller
import re
from tqdm import tqdm


def parse_pc_validation():

    wb = pd.read_excel("C:\\Users\\timan\\OneDrive\\Рабочий стол\\Работа\\Аквариус\\Справочник валидированной номенклатуры.xlsx", sheet_name='Справочник доп. комплектующих', usecols='A,B,E,F,H:AS')
    wb = wb[~wb['UID'].isnull()]
    wb = wb[~wb['Наименование в шаблоне'].isnull()]
    wb = wb.fillna(0)
    # for i in range(1, wb.shape[0]):
    #     row = wb.iloc[i-1:i]
    #     add_to_all_components(row, 'PC')

    wb = wb.drop_duplicates(subset=['UID'])
    wb = wb.reset_index()
    wb = wb.drop(columns='index')

    print('Парсинг компонентов пользовательских устройств:')
    all_components = int(wb.shape[0])
    added_components, parsed_components, updated_components = 0, 0, 0
    # for i in tqdm(range(1, wb.shape[0])):
    for i in tqdm(range(50, 150)):

        row = wb.iloc[i-1:i]
        add_to_all_components(row, 'PC')
        component = create_component(row, 'PC')

        if not component:
            continue
        parsed_components += 1

        query = sql_caller.create_component_query(component)
        if not sql_caller.check_availability(component['UID'], component['table']):
            sql_caller.send_sql_query(query)
            added_components += 1
        else:
            query = sql_caller.refactor_to_update_query(query)
            sql_caller.send_sql_query(query)
            updated_components += 1

        if (component['type'] != 'KEY' and component['type'] != 'MOU'
                and component['type'] != 'KPK' and component['type'] != 'TAB' and component['type'] != 'CBL'
                and component['type'] != 'HDM' and component['type'] != 'LTE' and component['type'] != 'DOC'):
            component = create_commodity(component, row, i)
            sql_caller.add_commodity_to_db(component)

    print('Парсинг завершён!\n\tОтсканировано компонентов: {} из {}\n\tДобавлено новых компонентов: {} из {}\n\tОбновлено компонентов: {} из {}\n'.format(
            parsed_components, all_components, added_components, all_components, updated_components, all_components))


def parse_server_validation():
    print('Парсинг компонентов серверных:')
    wb = pd.read_excel("/Users/malixds/dev/work/dbparser/valid.xlsx", sheet_name='Справочник', usecols='A,C,F,G,H:AH,AJ') # смотрим файл с валидацией
    #удаляем пустые колонки где uid null
    wb = wb[~wb['UID'].isnull()]

    # Замена пустых значений в колонке 'Наименование в шаблоне' значениями из 'Рабочее наименование ERP'
    for index, row in wb.iterrows():
        if row['Наименование в шаблоне'] == '':
            row['Наименование в шаблоне'] = row['Рабочее наименование ERP']


    # Удаление строк с пустыми значениями в колонке 'Наименование в шаблоне'
    wb = wb[~wb['Наименование в шаблоне'].isnull()]
    # Заполнение всех оставшихся пустых ячеек нулями
    wb = wb.fillna(0)
    # Удаление дубликатов по колонке 'UID'
    # wb = wb.drop_duplicates(subset=['UID'])
    # Сброс индексов DataFrame и удаление старого индекса
    wb = wb.reset_index()
    wb = wb.drop(columns='index')

    # Удаление колонки 'Рабочее наименование ERP' как ненужной
    wb = wb.drop(columns='Рабочее наименование ERP')

    all_components = int(wb.shape[0])
    added_components, parsed_components, updated_components = 0, 0, 0
    for i in tqdm(range(1, wb.shape[0])):
    # for i in tqdm(range(50, 150)):

        # текущая строка
        row = wb.iloc[i - 1:i]
        print('тек строка', row)
        add_to_all_components(row, 'Server')
        component = create_component(row, 'Server')

        if not component:
            continue
        if component['type'] == 'BRB':
            continue
        parsed_components += 1

        query = sql_caller.create_component_query(component)
        if not sql_caller.check_availability(component['UID'], component['table'], component['article']): # если такого uid нет, то создаем зпись
            result_of_sql_query = sql_caller.send_sql_query(query)
            print('res uf sql qry', result_of_sql_query)
            if result_of_sql_query:
                added_components += 1
        else: # если такой есть мы обновляем данные
            query = sql_caller.refactor_to_update_query(query)
            # print('component is updating', component)
            result_of_sql_query = sql_caller.send_sql_query(query)
            if result_of_sql_query:
                updated_components += 1

        if component['table'] != 'transceivers':
            component = create_commodity(component, row, i)
            sql_caller.add_commodity_to_db(component)

    print('Парсинг завершён!\n\tОтсканировано компонентов: {} из {}\n\tДобавлено новых компонентов: {} из {}\n\tОбновлено компонентов: {} из {}\n'.format(
        parsed_components, all_components, added_components, all_components, updated_components, all_components))


def add_to_all_components(component, table):
    res = {'type': get_uid_type(component)}
    res['UID'] = get_uid(component)
    res['name'] = get_name(component)
    res['power'] = get_power(component, table)
    res['article'] = str(get_article(component, table)).replace('.0', '')
    print('ARTICLE! TYPE!, UID!', res['article'], res['type'], res['UID'])
    query = sql_caller.create_all_query(res)
    if not sql_caller.check_availability_all_components(res['article']):
        sql_caller.send_sql_query(query)


def create_component(component, table):

    res = {'type': get_uid_type(component)}
    res['UID'] = get_uid(component)
    res['name'] = get_name(component)
    power = get_power(component, table)
    if power:
        res['power'] = power
    else:
        res['power'] = 0
    res['article'] = str(get_article(component, table)).replace('.0', '')
    print('Компонент',res)

    if res['type'] == 'CPU':
        res['table'] = 'cpu'
        res = create_cpu(res)
    elif res['type'] == 'RAM':
        res['table'] = 'ram'
        res = create_ram(res)
    elif res['type'] == 'SSD' or res['type'] == 'HDD':
        res['table'] = 'drives'
        res = create_drive(res)
    elif res['type'] == 'VGA' or res['type'] == 'GPU':
        res['table'] = 'gpu'
        res = create_vga(res)
    elif (res['type'] == 'NIC' or res['type'] == 'OCP') and table == 'Server':
        res['table'] = 'nic'
        res = create_nic(res)
    elif res['type'] == 'NIC' and table == 'PC':
        res['table'] = 'netcard'
    elif res['type'] == 'FAN' or res['type'] == 'CPC':
        res['table'] = 'fan'
        res = create_fan(res)
    elif res['type'] == 'WFA' and not 'Антенна' in res['name']:
        res['table'] = 'wifi_adapter'
        res = create_wfa(res)
    elif res['type'] == 'CBL' or res['type'] == 'HDM':
        if table == 'Server':
            res['table'] = 'cables'
        else:
            res['table'] = 'pc_cables'
        res = create_cable(res)
    elif res['type'] == 'MRK':
        res['table'] = 'mobile_rack'
        res = create_mobile_rack(res)
    elif res['type'] == 'ODD' and res['UID'] != 'AQC-ODD-00006':
        res['table'] = 'optical_drive'
        res = create_optical_drive(res)
    elif res['type'] == 'JBD':
        res['table'] = 'jbod'
        res = create_peripherals(res)
    elif res['type'] == 'DOC':
        res['table'] = 'doc_station'
        res = create_peripherals(res)
    elif res['type'] == 'KEY':
        res['table'] = 'keyboard'
        res = create_peripherals(res)
    elif res['type'] == 'MOU':
        res['table'] = 'mouse'
        res = create_peripherals(res)
    elif res['type'] == 'KPK' or res['type'] == 'TAB':
        res['table'] = 'tablet_phone'
        res = create_peripherals(res)
    elif res['type'] == 'PSU':
        res['table'] = 'psu'
        res = create_psu(res)
    elif res['type'] == 'OTR':
        res['table'] = 'transceivers'
        res = create_peripherals(res)
    elif res['type'] == 'LTE':
        res['table'] = 'lte'
        res = create_peripherals(res)
    elif res['type'] == 'SFT' and table == 'Server':
        res['table'] = 'server_software'
        res = create_server_software(res)
    elif res['type'] == 'BRB':
        res['table'] = 'barebone_laptop'
    elif res['type'] == 'HBA' or res['type'] == 'RDC':
        if 'FC' in res['name']:
            res['table'] = 'fc_adapter'
            res = create_fc_adapter(res)
        else:
            res['table'] = 'raid'
            res = create_raid_controller(res)
    elif res['type'] == 'CAS':
        res['table'] = 'case'
        res = create_case(res)
    else:
        return None
    return res

def get_uid(component):
    return component.iloc[0, 0]
def get_uid_type(component):
    type = get_component_type(component.iloc[0, 0])
    print('TYPE!', type)
    return type

def get_component_type(uid):
    return uid.split('-')[1]

def get_name(component):
    return component.iloc[0, 1]


def get_power(component, table):
    if table == 'Server':
        return component.iloc[0, 30]
    elif table == 'PC':
        return component.iloc[0, 2]


def get_article(component, table):
    if table == 'Server':
        print('ARTICL??', component.iloc[0, 2])
        return component.iloc[0, 2]
    elif table == 'PC':
        return component.iloc[0, 3]

def create_cpu(res):
    return res

def create_ram(res):
    res['clock'] = get_ram_clock(res['name'])
    res['amount'] = get_ram_amount(res['name'])
    return res

def create_fan(res):
    return res


def get_ram_clock(name):
    index = name.find('MHz')
    temp = index
    while name[temp-1].isdigit():
        temp -= 1
    return name[temp:index]


def get_ram_amount(name):
    index = name.find('GB')
    temp = index
    while name[temp-1].isdigit():
        temp -= 1
    return name[temp:index]


def create_drive(res):
    res['capacity'] = get_drive_capacity(res['name'])
    res['type_id'] = get_drive_type(res['name'])
    res['group_id'] = get_drive_group(res['name'])
    res['slot_id'] = get_drive_slot(res['name'])
    res['size'] = get_drive_size(res['name'])
    return res


def get_drive_capacity(name):
    temp = re.findall(r'\d+', name)
    res = list(map(int, temp))
    capacity = res[0]
    if 'GB' in name:
        return capacity
    elif 'TB' in name:
        return capacity*1024
    return capacity


def get_drive_type(name):
    if 'NVMe' in name:
        return 1
    elif 'SATA' in name:
        return 2
    elif 'SAS' in name:
        return 3
    else:
        return 0


def get_drive_group(name):
    if 'RI' in name:
        return 1
    elif 'MU' in name:
        return 2
    elif 'WI' in name:
        return 3
    elif 'Boot' in name:
        return 5
    else:
        return 4


def get_drive_slot(name):
    if 'M.2' in name:
        return 10
    else:
        return 4


def get_drive_size(name):
    if 'M.2 22110' in name:
        return 'M.2 22110'
    elif 'M.2 2242' in name:
        return 'M.2 2242'
    elif 'M.2 2280' in name:
        return 'M.2 2280'
    elif 'HHHL' in name:
        return 'HHHL'
    elif '3.5' in name:
        return '3.5'
    elif '2.5 7mm' in name:
        return '2.5 7mm'
    elif '2.5' in name:
        return '2.5'


def create_fc_adapter(res):
    name = res['name']
    if 'Quad' in name:
        res['port_type'] = 'Quad'
    elif 'Dual' in name:
        res['port_type'] = 'Dual'
    res['slot_id'] = 3
    index = name.find('Gb')
    res['capacity'] = int(name[index-2:index])
    return res


def create_vga(res):
    res['slot_id'] = 3
    return res

def create_nic(res):
    if get_nic_slot(res['name']):
        res['slot_id'] = get_nic_slot(res['name'])
    else:
        res['slot_id'] = 0
    return res

def get_nic_slot(name):
    if 'PHY' in name:
        return 9
    elif 'OCP' in name:
        return 8
    elif '16' in name:
        return 3
    elif '8' in name:
        return 2
    elif '4' in name:
        return 1
    else:
        return False


def create_wfa(res):
    return res


def create_cable(res):
    res['type_id'] = get_cable_type(res['name'])
    return res

def get_cable_type(name):
    if 'HDminiSAS' in name:
        return 1
    elif 'DAC' in name:
        return 2
    elif 'AOC' in name:
        return 3
    elif'OCuLink' in name:
        return 4
    else:
        return 0


def create_mobile_rack(res):
    return res


def create_peripherals(res):
    return res


def create_case(res):
    name = res['name']
    pattern = r'(\d+)W'
    match = re.search(pattern, name)
    res['power'] = get_psu_power(res)
    res['psu_id'] = 1
    res['table'] = 'public."case"'
    if match and len(match.group(1)) > 0:
        res['psu_id'] = res['UID']
    return res





def create_psu(res):
    res['power'] = get_psu_power(res)
    return res


def get_psu_power(res):
    name = res['name']
    pattern = r'(\d+)W'
    match = re.search(pattern, name)
    if match:
        return int(match.group(1))
    else:
        return None


def create_optical_drive(res):
    return res

def create_commodity(component, row, axe):
    valid_plats = []
    for col in list(row.columns)[4:]:
        val = row.loc[axe-1, '{0}'.format(col)]
        if type(val) != str:
            if val > 0 and col != 'Потребляемая мощность, Вт':
                if col == 'T50 D204CF':
                    col1 = col + '-f'
                    col2 = col + '-b'
                    valid_plats.append(col1)
                    valid_plats.append(col2)
                    continue
                if 'T40' in col:
                    col1 = col + '-V'
                    col2 = col + '-B'
                    valid_plats.append(col1)
                    valid_plats.append(col2)
                    continue
                if 'P30 K43 USFF1' == col:
                    col1 = 'P30 K43 USFF1'
                    col2 = 'P30 K43 USFF1 noLVDS'
                    valid_plats.append(col1)
                    valid_plats.append(col2)
                    continue
                valid_plats.append(col)
            elif val == 0 and sql_caller.check_component_platform_commodity(sql_caller.get_plat_id(col), component['UID'], component['table']):
                sql_caller.remove_commodity(col, component['UID'], component['table'])
    component['valid_platform'] = valid_plats
    return component

def create_raid_controller(res):
    if res['type'] == 'HBA':
        res['type_cntrl'] = 2
    else:
        res['type_cntrl'] = 1
    res = get_raid_slots_int_ext(res)
    res['slot_id'] = get_raid_slots(res)
    return res


def get_raid_slots_int_ext(res):
    name = res['name']
    res['int'] = 0
    res['ext'] = 0
    if 'P ext' in name:
        index = name.find('P ext')
        temp = index
        while name[temp-1].isdigit():
            temp -= 1
        res['ext'] = int(name[temp:index])
    if 'P int' in name:
        index = name.find('P int')
        temp = index
        while name[temp-1].isdigit():
            temp -= 1
        res['int'] = int(name[temp:index])
    return res

def get_raid_slots(res):
    if res['int'] + res['ext'] > 8:
        return 2
    else:
        return 12


def create_server_software(res):
    if 'BIOS' in res['name']:
        res['soft_type'] = 'bios_software'
    elif 'BMC' in res['name']:
        res['soft_type'] = 'bmc_software'
    elif 'Управляющее ПО' in res['name']:
        res['soft_type'] = 'dss_software'
    return res
