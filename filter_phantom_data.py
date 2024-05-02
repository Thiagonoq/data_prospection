from pathlib import Path
import excel_handler
import phonenumbers
import re
import pandas as pd

abs_path = Path(__file__).parent.parent
excel_data_dir = abs_path / 'raspagem_google_maps'
bd_path = abs_path / 'bd_google_maps.xlsx'

def list_data_files(excel_data_dir):
    return [f for f in excel_data_dir.iterdir() if f.suffix == '.xlsx']

def main():
    dfs = excel_handler.import_excel(bd_path)
    sheet_name = 'Hortifruti'
    df = dfs[sheet_name]
    data_files = list_data_files(excel_data_dir)
    
    for data_file in data_files:
        region_name = data_file.stem
        print(f'Processando {region_name}...')
        data_dfs = excel_handler.import_excel(data_file)
        sheet_name = region_name
        data_df = data_dfs[sheet_name]

        for index, row in data_df.iterrows():
            if pd.isna(row['title']) or (pd.isna(row['phoneNumber']) and pd.isna(row['phoneFromWebsite'])):
                continue

            name = row['title']
            website = row['website']
            address = row['address']
            phone_number = format_phone(row['phoneNumber'])
            secondary_phone_number = format_phone(row['phoneFromWebsite']) if not pd.isna(row['phoneFromWebsite']) else 'nan'

            phone_numbers = ",".join([phone_number, secondary_phone_number] if secondary_phone_number != 'nan' else [phone_number])
            if not pd.isna(website):
                if website.startswith('https://wa.me/') or website.startswith('http://wa.me/'):
                    whatsapp_number = format_phone(website.split('/')[-1])
                    phone_numbers =','.join([phone_numbers, whatsapp_number])
            
            df.at[index, 'nome_fantasia'] = name
            df.at[index, 'website'] = website
            df.at[index, 'categoria'] = row['category']
            df.at[index, 'endereco'] = address
            df.at[index, 'regiao'] = region_name
            df.at[index, 'telefones'] = phone_numbers

        excel_handler.save_excel(dfs, bd_path, bd_path, sheet_name=sheet_name)
        print(f'Regiao {region_name} salva no arquivo.')      


def format_phone(phone, country="BR"):
    try:
        parsed_phone = phonenumbers.parse(phone, country)
        if phonenumbers.is_valid_number(parsed_phone):
            phone = re.sub(r'\D', '', phonenumbers.format_number(parsed_phone, phonenumbers.PhoneNumberFormat.INTERNATIONAL))
            return phone
        else:
            return phone
    except phonenumbers.NumberParseException:
        return phone

if __name__ == '__main__':
    # main()
    phone_number = '5531900000000'
    secondary_phone_number = '553595658756'
    website = 'ahttps://wa.me/553199999999'
    phone_numbers = ",".join([phone_number, secondary_phone_number] if secondary_phone_number != 'nan' else [phone_number])
    if not pd.isna(website):
        if website.startswith('https://wa.me/') or website.startswith('http://wa.me/'):
            whatsapp_number = format_phone(website.split('/')[-1])
    
            phone_numbers =','.join([phone_numbers, whatsapp_number])
            print(phone_numbers)

    print(phone_numbers)
                                    