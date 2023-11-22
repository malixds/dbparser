import pandas as pd
import sql_caller
import re
from tqdm import tqdm


def parse_pc_validation():
    wb = pd.read_excel("C:\\Users\\timan\Downloads\\valid_test.xlsx", sheet_name='Справочник доп. комплектующих', usecols='A,B,E,H:AS')
    wb = wb[~wb['UID'].isnull()]
    wb = wb[~wb['Наименование в шаблоне'].isnull()]
    wb = wb.fillna(0)
    wb = wb.drop_duplicates(subset=['UID'])
    wb = wb.reset_index()
    wb = wb.drop(columns='index')

    all_components = int(wb.shape[0])
    added_components, parsed_components = 0, 0
    for i in tqdm(range(1, wb.shape[0])):

        row = wb.iloc[i-1:i]
        component = create_component(row)

        if not component:
            continue
        parsed_components += 1

        query = sql_caller.create_component_query(component)
        if not sql_caller.check_availability(component['UID'], component['table']):
            sql_caller.send_sql_query(query)
            added_components += 1

        component = create_commodity(component, row, i)
        sql_caller.add_commodity_to_db(component)

    print('Парсинг завершён!\nОтсканировано компонентов: {} из {}\nДобавлено новых компонентов: {} из {}\n'.format(parsed_components, all_components, added_components, all_components))





def create_component(component):

    res = {'type': get_uid_type(component)}
    res['UID'] = get_uid(component)
    res['name'] = get_name(component)
    res['power'] = get_power(component)

    if res['type'] == 'CPU':
        res['table'] = 'cpu'
        res = create_cpu(res)
    elif res['type'] == 'RAM':
        res['table'] = 'ram'
        res = create_ram(res)
    elif res['type'] == 'SSD' or res['type'] == 'HDD':
        res['table'] = 'drives'
        res = create_drive(res)
    elif res['type'] == 'VGA':
        res['table'] = 'gpu'
        res = create_vga(res)
    elif res['type'] == 'NIC':
        res['table'] = 'nic'
        res = create_nic(res)
    elif res['type'] == 'WFA':
        res['table'] = 'wifi_adapter'
        res = create_wfa(res)
    elif res['type'] == 'CBL':
        res['table'] = 'cables'
        res = create_cable(res)
    else:
        return None
    # elif res['type'] == 'BTA':
    #     res.update({create_cpu()})
    # elif res['type'] == 'CMA':
    #     res.update({create_cpu()})
    # elif res['type'] == 'HDM':
    #     res.update({create_cpu()})
    # elif res['type'] == 'CBL':
    #     res.update({create_cpu()})
    # elif res['type'] == 'RMK':
    #     res.update({create_cpu()})
    # elif res['type'] == 'ISW':
    #     res.update({create_cpu()})
    # elif res['type'] == 'MRK':
    #     res.update({create_cpu()})
    # elif res['type'] == 'ODD':
    #     res.update({create_cpu()})
    # elif res['type'] == 'CAR':
    #     res.update({create_cpu()})
    # elif res['type'] == 'KEY':
    #     res.update({create_cpu()})
    # elif res['type'] == 'MOU':
    #     res.update({create_cpu()})
    # elif res['type'] == 'IOS':
    #     res.update({create_cpu()})
    # elif res['type'] == 'BAG':
    #     res.update({create_cpu()})
    # elif res['type'] == 'STY':
    #     res.update({create_cpu()})
    # elif res['type'] == 'OPTION':
    #     res.update({create_cpu()})
    return res

def get_uid(component):
    return component.iloc[0, 0]
def get_uid_type(component):
    type = get_component_type(component.iloc[0, 0])
    return type

def get_component_type(uid):
    return uid.split('-')[1]

def get_name(component):
    return component.iloc[0, 1]


def get_power(component):
    return component.iloc[0, 2]


def create_cpu(res):
    res['cost'] = 100
    res['gpl'] = 300
    return res

def create_ram(res):
    res['cost'] = 19
    res['gpl'] = 60
    res['clock'] = get_ram_clock(res['name'])
    res['amount'] = get_ram_amount(res['name'])
    return res


def get_ram_clock(name):
    if '-' in name:
        return name.split('-')[4]
    return name.split(' ')[4][0:4]


def get_ram_amount(name):
    if '-' in name:
        return name.split('-')[3][:-2]
    return name.split(' ')[3][:-2]


def create_drive(res):
    res['cost'] = 120
    res['gpl'] = 450
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
    elif 'HHHL' in name:
        return 'HHHL'
    elif '3.5' in name:
        return '3.5'
    elif '2.5 7mm' in name:
        return '2.5 7mm'
    elif '2.5' in name:
        return '2.5'


def create_vga(res):
    res['cost'] = 200
    res['gpl'] = 600
    res['slot_id'] = 3
    return res

def create_nic(res):
    res['cost'] = 60
    res['gpl'] = 200
    if get_nic_slot(res['name']):
        res['slot_id'] = get_nic_slot(res['name'])
    else:
        res['slot_id'] = 0
    return res

def get_nic_slot(name):
    if '16' in name:
        return 3
    elif '8' in name:
        return 2
    elif '4' in name:
        return 1
    else:
        return False


def create_wfa(res):
    res['cost'] = 5
    res['gpl'] = 10
    return res

def create_cable(res):
    res['cost'] = 3
    res['gpl'] = 8
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


def create_commodity(component, row, axe):
    valid_plats = []
    for col in list(row.columns)[3:]:
        val = row.loc[axe-1, '{0}'.format(col)]
        if val > 0:
            valid_plats.append(col)
    component['valid_platform'] = valid_plats
    return component