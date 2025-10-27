import pandas as pd

df = pd.read_csv('data/output/testitem/testitem_20251027.csv')
print(f'Всего колонок: {len(df.columns)}')
print(f'Всего строк: {len(df)}')

# Проверим заполненность основных полей
key_fields = ['molecule_chembl_id', 'molregno', 'pref_name', 'max_phase', 'therapeutic_flag', 
              'molecular_formula', 'molecular_weight', 'canonical_smiles', 'pubchem_cid']

print('\nЗаполненность ключевых полей:')
for col in key_fields:
    if col in df.columns:
        non_null = df[col].notna().sum()
        print(f'{col}: {non_null}/10')
    else:
        print(f'{col}: НЕТ В ДАННЫХ')

# Проверим PubChem поля
pubchem_fields = [col for col in df.columns if 'pubchem' in col.lower()]
print(f'\nPubChem поля ({len(pubchem_fields)}):')
for col in pubchem_fields:
    non_null = df[col].notna().sum()
    print(f'{col}: {non_null}/10')
