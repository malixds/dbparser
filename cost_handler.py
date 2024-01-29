import pandas as pd

import parser_module
import sql_caller
from tqdm import tqdm
import parser_module as pm

def update_costs():
    print('Парсинг цен серверных:\n')
    wb = pd.read_excel("C:\\Users\\timan\\OneDrive\\Рабочий стол\\Работа\\Аквариус\\Справочник_цен.xlsx", sheet_name='Справочник', usecols='A,B,C,F')
    wb = wb.drop_duplicates(subset=['UID'])
    wb = wb.reset_index()
    wb = wb.drop(columns='index')

    all_components = int(wb.shape[0])
    added_components, parsed_costs = 0, 0
    for i in tqdm(range(1, wb.shape[0])):

        row = wb.iloc[i-1:i]
        component = create_component_cost(row, 'Server')

        if not component['table']:
            continue
        parsed_costs += 1

        query = sql_caller.create_cost_query(component)
        if not sql_caller.check_availability(component['UID'], component['table']):
            continue
        else:
            sql_caller.send_sql_query(query)
            added_components += 1
    print('Парсинг завершён!\n\tОтсканировано ценников: {} из {}\n\tДобавлено новых ценников: {} из {}\n'.format(
        parsed_costs, all_components, added_components, all_components))
    print('Парсинг цен клиентских:\n')
    wb = pd.read_excel("C:\\Users\\timan\\OneDrive\\Рабочий стол\\Работа\\Аквариус\\Справочник_цен_клиенты.xlsx", sheet_name='Справочник', usecols='A,B,C,F')
    wb = wb.drop_duplicates(subset=['UID'])
    wb = wb.reset_index()
    wb = wb.drop(columns='index')

    all_components = int(wb.shape[0])
    added_components, parsed_costs = 0, 0
    for i in tqdm(range(1, wb.shape[0])):

        row = wb.iloc[i-1:i]
        component = create_component_cost(row, 'PC')

        if not component['table']:
            continue
        parsed_costs += 1

        query = sql_caller.create_cost_query(component)
        if not sql_caller.check_availability(component['UID'], component['table']):
            continue
        else:
            sql_caller.send_sql_query(query)
            added_components += 1
    print('Парсинг завершён!\n\tОтсканировано ценников: {} из {}\n\tДобавлено новых ценников: {} из {}\n'.format(
        parsed_costs, all_components, added_components, all_components))


def create_component_cost(row, profile_table):
    res = {'UID': row.iloc[0, 0], 'cost': row.iloc[0, 1], 'gpl': row.iloc[0, 2], 'name': row.iloc[0, 3]}
    res['type'] = parser_module.get_component_type(res['UID'])
    res = get_table_type(res, profile_table)
    return res


def get_table_type(res, table):
    if res['type'] == 'CPU':
        res['table'] = 'cpu'
    elif res['type'] == 'RAM':
        res['table'] = 'ram'
    elif res['type'] == 'SSD' or res['type'] == 'HDD':
        res['table'] = 'drives'
    elif res['type'] == 'VGA' or res['type'] == 'GPU':
        res['table'] = 'gpu'
    elif (res['type'] == 'NIC' or res['type'] == 'OCP') and table == 'Server':
        res['table'] = 'nic'
    elif res['type'] == 'NIC' and table == 'PC':
        res['table'] = 'netcard'
    elif res['type'] == 'FAN' or res['type'] == 'CPC':
        res['table'] = 'fan'
    elif res['type'] == 'WFA':
        res['table'] = 'wifi_adapter'
    elif res['type'] == 'CBL' or res['type'] == 'HDM':
        if table == 'Server':
            res['table'] = 'cables'
        else:
            res['table'] = 'pc_cables'
    elif res['type'] == 'MRK':
        res['table'] = 'mobile_rack'
    elif res['type'] == 'ODD':
        res['table'] = 'optical_drive'
    elif res['type'] == 'JBD':
        res['table'] = 'jbod'
    elif res['type'] == 'KEY':
        res['table'] = 'keyboard'
    elif res['type'] == 'MOU':
        res['table'] = 'mouse'
    elif res['type'] == 'KPK' or res['type'] == 'TAB':
        res['table'] = 'tablet_phone'
    elif res['type'] == 'PSU':
        res['table'] = 'psu'
    elif res['type'] == 'OTR':
        res['table'] = 'transceivers'
    elif res['type'] == 'LTE':
        res['table'] = 'lte'
    elif res['type'] == 'BRB':
        res['table'] = 'barebone_laptop'
    elif res['type'] == 'SFT' and table == 'Server':
        res['table'] = 'server_software'
    elif res['type'] == 'HBA' or res['type'] == 'RDC':
        if 'FC' in res['name']:
            res['table'] = 'fc_adapter'
        else:
            res['table'] = 'raid'
    elif res['type'] == 'CAS':
        res['table'] = '"case"'
    else:
        res['table'] = False
        return res
    return res