import json
from pathlib import Path
import excel_handler
import pandas as pd
import re

abs_path = Path(__file__).parent.parent

excel_path = abs_path / 'contatos_trello.xlsx'

def list_users(excel_path):
    dfs = excel_handler.import_excel(excel_path)
    df_data = dfs['data']
    ids = {
        "62a87fa97bfc7b44bcb56c66": {"name": "lucia", "data": {}}, 
        "64a420973d8b8741a3a6f67b": {"name": "thiago", "data": {}}
    }

    for index, row in df_data.iterrows():
        if pd.isna(row['Membro']):
            continue

        whatsapp, name = extract_name_n_phone(row['Dado'])
        for member in row['Membro'].split(','):
            member = member.strip()

            if member in ids:
                ids[member]['data'][name] = whatsapp
    
    for member_data in ids.values():
        with open(f'{member_data["name"]}.json', 'w', encoding='utf-8') as f:
            json.dump(member_data['data'], f, indent=4, ensure_ascii=False)

def extract_name_n_phone(message):
    whatsapp = re.findall(r'whatsapp: ?(\d+)', message, re.I | re.M)[0]
    name = re.findall(r'nome da empresa: ?(.+),', message, re.I | re.M)[0]

    return whatsapp, name

if __name__ == '__main__':
    list_users(excel_path)