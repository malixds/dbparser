import pandas as pd
import sql_caller
from tqdm import tqdm
import parser_module as pm

def update_costs():
    print('Парсинг цен:\n')
    wb = pd.read_excel("C:\\Users\\timan\\OneDrive\\Рабочий стол\\Работа\\Аквариус\\Справочник_цен.xlsx", sheet_name='Справочник', usecols='A,B,C,E')
    wb = wb[~wb['UID'].isnull()]
    wb = wb.fillna(0)
    wb = wb.drop_duplicates(subset=['UID'])
    wb = wb.reset_index()
    wb = wb.drop(columns='index')

    all_components = int(wb.shape[0])
    added_components, parsed_costs = 0, 0
    for i in tqdm(range(1, wb.shape[0])):

        row = wb.iloc[i-1:i]
        component = create_component_cost(row)

        if not component['table']:
            continue
        parsed_costs += 1

        query = sql_caller.create_cost_query(component)
        if not sql_caller.check_availability(component['UID'], component['table']):
            continue
        else:
            sql_caller.send_sql_query(query)
            added_components += 1
    print('Парсинг завершён!\n\tОтсканировано ценников: {} из {}\n\tДобавлено новых ценников: {} из {}\n'.format(parsed_costs, all_components, added_components, all_components))


def create_component_cost(row):
    res = {'UID': row.iloc[0, 0], 'type': row.iloc[0, 3], 'cost': row.iloc[0, 1], 'gpl': row.iloc[0, 2]}
    res = get_table_type(res)
    res['UID'] = res['UID'].replace('AQ', 'AQC', 1)
    return res


def get_table_type(res):
    if res['type'] == 'CPU':
        res['table'] = 'cpu'
    elif res['type'] == 'RAM':
        res['table'] = 'ram'
    elif res['type'] == 'SSD' or res['type'] == 'HDD':
        res['table'] = 'drives'
    elif res['type'] == 'VGA':
        res['table'] = 'gpu'
    elif res['type'] == 'NIC':
        res['table'] = 'nic'
    elif res['type'] == 'WFA':
        res['table'] = 'wifi_adapter'
    elif res['type'] == 'CBL' or res['type'] == 'HDM':
        res['table'] = 'cables'
    elif res['type'] == 'MRK':
        res['table'] = 'mobile_rack'
    elif res['type'] == 'ODD':
        res['table'] = 'optical_drive'
    elif res['type'] == 'KEY' or res['type'] == 'MOU' or res['type'] == 'BAG' or res['type'] == 'STY' or res['type'] == 'KMK':
        res['table'] = 'peripherals'
    else:
        res['table'] = False
        return res
    return res