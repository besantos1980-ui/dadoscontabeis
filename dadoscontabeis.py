import requests
from io import BytesIO
import zipfile
import pandas as pd
import os
from datetime import datetime

# Função para baixar e processar ZIPs de CADOP para anos 2023-2025
# Processamento em chunks para eficiência de memória
# Renomeação de colunas case-insensitive
# Filtros e cálculos conforme especificado

def processar_cadop():
    # URLs base para downloads (exemplo, ajustar conforme necessário)
    base_url = 'https://exemplo.com/cadop_{ano}_{trimestre}.zip'
    anos = [2023, 2024, 2025]
    trimestres = [1, 2, 3, 4]
    
    # Colunas obrigatórias
    required_cols = ['REG_ANS', 'CD_CONTA_CONTABIL', 'VL_SALDO_INICIAL', 'VL_SALDO_FINAL']
    
    # Mapeamento de renomeação (case-insensitive)
    rename_map = {
        'REGISTRO': 'REGISTRO_OPERADORA',
        'NOME_FANTASIA': 'Nome_Fantasia',
        'MODALIDADE': 'Modalidade',
        'REG_ANS': 'REG_ANS',
        'CD_CONTA_CONTABIL': 'CD_CONTA_CONTABIL',
        'VL_SALDO_INICIAL': 'VL_SALDO_INICIAL',
        'VL_SALDO_FINAL': 'VL_SALDO_FINAL'
    }
    
    all_chunks = []
    
    for ano in anos:
        for t in trimestres:
            url = base_url.format(ano=ano, trimestre=t)
            try:
                response = requests.get(url, stream=True)
                response.raise_for_status()
                
                with zipfile.ZipFile(BytesIO(response.content)) as zf:
                    for file_name in zf.namelist():
                        if file_name.endswith('.csv'):
                            with zf.open(file_name) as f:
                                # Processar em chunks
                                for chunk in pd.read_csv(f, sep=';', encoding='latin1', chunksize=10000):
                                    # Verificar colunas obrigatórias
                                    if not all(col in chunk.columns for col in required_cols):
                                        print(f'Colunas obrigatórias faltando em {file_name}: {chunk.columns.tolist()}')
                                        continue
                                    
                                    # Renomear colunas (case-insensitive)
                                    chunk.columns = [col.upper() for col in chunk.columns]
                                    chunk = chunk.rename(columns=rename_map)
                                    
                                    # Diagnóstico pré-filtro
                                    print(f'Pré-filtro: {len(chunk)} linhas, colunas: {chunk.columns.tolist()}')
                                    
                                    # Filtro único numérico
                                    chunk = chunk[chunk['CD_CONTA_CONTABIL'].isin([311, 3117, 3119, 41])]
                                    
                                    # Diagnóstico pós-filtro
                                    print(f'Pós-filtro: {len(chunk)} linhas')
                                    
                                    # Formatar saldos BR e converter para numérico
                                    for col in ['VL_SALDO_INICIAL', 'VL_SALDO_FINAL']:
                                        chunk[col] = chunk[col].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False).astype(float)
                                    
                                    # Calcular diferença
                                    chunk['Diferenca'] = chunk['VL_SALDO_FINAL'] - chunk['VL_SALDO_INICIAL']
                                    
                                    # Adicionar trimestre
                                    chunk['Trimestre'] = f'{t}T{ano}'
                                    
                                    all_chunks.append(chunk)
                                    
                                    # Diagnóstico prontas
                                    print(f'Prontas: {len(chunk)} linhas, unique CD_CONTA_CONTABIL[:10]: {chunk["CD_CONTA_CONTABIL"].unique()[:10].tolist()}')
                                    print(f'Samples head(3):\n{chunk.head(3)}')
                
            except requests.exceptions.HTTPError as e:
                if response.status_code == 404:
                    print(f'ZIP não encontrado para {ano}T{t}, pulando...')
                    continue
                else:
                    raise e
    
    # Concatenar todos os chunks
    if all_chunks:
        contabeis_df = pd.concat(all_chunks, ignore_index=True)
        contabeis_df.to_csv('contabeis_debug.csv', index=False)
        
        # Carregar base de operadoras (assumindo arquivo existente)
        operadoras_df = pd.read_csv('operadoras.csv')  # Ajustar caminho
        
        # Merge inner
        merged_df = pd.merge(contabeis_df, operadoras_df, left_on='REG_ANS', right_on='REGISTRO_OPERADORA', how='inner')
        final_df = merged_df[['REGISTRO_OPERADORA', 'Nome_Fantasia', 'Modalidade', 'Trimestre', 'CD_CONTA_CONTABIL', 'Diferenca']]
        
        # Salvar Excel se houver dados
        if not final_df.empty:
            today = datetime.now().strftime('%d_%m_%Y')
            final_df.to_excel(f'arquivo_base_cenario_saude_{today}.xlsx', index=False)
            print('Arquivo Excel salvo com sucesso.')
        else:
            print('Nenhum dado para salvar no Excel.')
    else:
        print('Nenhum chunk processado.')

# Executar função
if __name__ == '__main__':
    processar_cadop()
