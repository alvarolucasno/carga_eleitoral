# Carga Eleitoral

Copia todos os dados eleitorais do BigQuery (BasedosDados) para o PostgreSQL.

Tabelas: `tse_candidatos`, `tse_receitas_candidato`, `tse_despesas_candidato` — todos os anos disponíveis.

## Instalação

```bash
pip install -r requirements.txt
```

## Configuração

```bash
cp .env.example .env
# Edite .env com suas credenciais
```

**Obrigatório:** `BASEDOSDADOS_BILLING_PROJECT_ID` com billing ativo no Google Cloud.

## Uso

```bash
# Todos os anos, todas as tabelas
python eleicoes/dados_campanha_carga_completa.py --yes

# Apenas anos específicos
python eleicoes/dados_campanha_carga_completa.py --anos 2022,2024

# Apenas uma tabela
python eleicoes/dados_campanha_carga_completa.py --tabelas candidatos

# Reusar CSVs já baixados
python eleicoes/dados_campanha_carga_completa.py --skip-download

# Forçar re-download
python eleicoes/dados_campanha_carga_completa.py --force-download

# Recomeçar do zero
python eleicoes/dados_campanha_carga_completa.py --drop-tabelas --yes
```

O script detecta anos já carregados e pula automaticamente (retomável).
