from pathlib import Path
import pandas as pd
import excel_handler
import json
import requests
import re
import phonenumbers

abs_path = Path(__file__).parent.parent

ZAPI_TOKEN="76D870027FDE0133FDCCB517"
ZAPI_INSTANCE="3B94EC8107E6603D5ADEA6D2A1CCEF8E"


def number_with_zap(number):
    # formated_number = format_phone(number)
    formated_number = re.sub(r'[^0-9]+', '', number)
    # url = f'https://api.z-api.io/instances/{ZAPI_INSTANCE}/token/{ZAPI_TOKEN}/phone-exists/{formated_number}'
    url = f'https://api.z-api.io/instances/{ZAPI_INSTANCE}/token/{ZAPI_TOKEN}/phone-exists-batch'

    payload = {
        "phones": [formated_number]
    }
    headers = {
        'Content-Type': 'application/json'
    }
    try:
        response = requests.post(url, data=json.dumps(payload), headers=headers)
        response.raise_for_status()
        response_data = response.json()

        if response_data[0]['exists']:
            return response_data[0]['outputPhone']
        else:
            return None
    except requests.RequestException as e:
        print(f"Erro ao fazer a chamada para o ZAPI: {e}")
        return None

def format_phone(phone, country="BR"):
    try:
        parsed_phone = phonenumbers.parse(phone, country)
        if phonenumbers.is_valid_number(parsed_phone):
            return phonenumbers.format_number(parsed_phone, phonenumbers.PhoneNumberFormat.INTERNATIONAL)
        else:
            return phone
    except phonenumbers.NumberParseException:
        return phone

def create_google_json(abs_path):
    excel_path = abs_path /'clientes_com_telefone.xlsx'
    json_path = abs_path /'raspagem_hortifruti_google.json'

    sheet_name = 'com_tel'
    dfs = excel_handler.import_excel(excel_path)
    df_contacts = dfs[sheet_name]
    clients_data = []
    seen_phones = set()

    for index, row in df_contacts.iterrows():
        if str(row["title"]) == "nan" or str(row["phoneNumber"]) == "nan":
            continue
        
        phone_number = row["phoneNumber"]
        if phone_number in seen_phones or row['repeated'] == 'yes':
            print(f'Telefone {phone_number} já cadastrado...')
            df_contacts.at[index, 'repeated'] = 'yes'
            continue

        seen_phones.add(phone_number)
        phone = number_with_zap(phone_number)
        
        if phone is None or row['hasWhatsapp'] == 'no':
            print(f'Contato {row["title"]} não possui whatsapp.')
            df_contacts.at[index, 'hasWhatsapp'] = 'no'
            continue
        df_contacts.at[index, 'numberSearched'] = phone

        clients_data.append({
            "name": row["title"],
            "address": row["address"] if str(row["address"]) != "nan" else None,
            "phone": phone
        })

    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(clients_data, f, indent=4, ensure_ascii=False, allow_nan=True)
    print(f'Arquivo {json_path} criado com sucesso.')

    excel_handler.save_excel(dfs, excel_path, excel_path, sheet_name=sheet_name)

def create_db_json(abs_path, max_clients=500):
    excel_path = abs_path /'bd_empresas.xlsx'
    json_path = abs_path /'raspagem_hortifruti_bd_empresas.json'

    sheet_name = 'Hortifruti'
    dfs = excel_handler.import_excel(excel_path)
    df_contacts = dfs[sheet_name]
    clients_data = []
    seen_phones = set()

    for index, row in df_contacts.iterrows():
        if str(row["razao_social"]) == "nan" or str(row["telefones"]) == "nan" or str(row["hasWhatsapp"]) != "nan":
            continue

        phone_numbers = row["telefones"].split(',')
        valid_phone = None

        for phone_number in phone_numbers:
            formatted_phone = format_phone(phone_number.strip())
            if formatted_phone in seen_phones or row['repeated'] == 'yes':
                print(f'Telefone {formatted_phone} já cadastrado...')
                df_contacts.at[index, 'repeated'] = 'yes'
                continue

            seen_phones.add(formatted_phone)
            whatsapp_phone = number_with_zap(formatted_phone)
        
            if whatsapp_phone:
                valid_phone = whatsapp_phone
                df_contacts.at[index, 'hasWhatsapp'] = 'yes'
                df_contacts.at[index, 'numberSearched'] = whatsapp_phone
                break 
            else:
                df_contacts.at[index, 'hasWhatsapp'] = 'no'

        if valid_phone:
            clients_data.append({
                "name": re.sub(r'\d{2}\.\d{3}\.\d{3}\s', '', row["razao_social"]).strip(),
                "address": row["address"] if not pd.isna(row["address"]) else None,
                "phone": valid_phone
            })

        if len(clients_data) > max_clients:
            break

    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(clients_data, f, indent=4, ensure_ascii=False, allow_nan=True)
    print(f'Arquivo {json_path} criado com sucesso.')

    excel_handler.save_excel(dfs, excel_path, excel_path, sheet_name=sheet_name)

def send_link(ZAPI_INSTANCE, ZAPI_TOKEN):
    url = f"https://api.z-api.io/instances/{ZAPI_INSTANCE}/token/{ZAPI_TOKEN}/send-link"
    payload = {
        "phone": "553198929068",
        "message": "Clique no link para saber mais: https://youtu.be/ToPuJiQSdAM?si=YAA95MDZ2867PO7y",
        "image": "https://drive.google.com/uc?export=download&id=1seSFgQijrsg6mp76K1bb8E6bbtfIQI03",
        "linkUrl": "https://youtu.be/ToPuJiQSdAM?si=YAA95MDZ2867PO7y",
        "title": "Crie encartes pelo WhatsApp!",
        "linkDescription": "Descubra como criar vídeos, artes e encartes\npara seu hortifruti direto do WhatsApp com o Video AI!"
    }
    headers = {
        'Content-Type': 'application/json'
    }
    response = requests.post(url, data=json.dumps(payload), headers=headers)

    print(response.text)

# Criar uma nova função, para separar os telefones que estão juntos.
if __name__ == '__main__':
    create_db_json(abs_path, 500)
    # create_google_json(abs_path)
    # number_with_zap('+55 31 998929068')
    # send_link(ZAPI_INSTANCE, ZAPI_TOKEN)