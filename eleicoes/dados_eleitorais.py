# -*- coding: utf-8 -*-
"""
Copia TODOS os anos eleitorais do BigQuery (BasedosDados) para o PostgreSQL.

Processa ano a ano, tabela a tabela (tse_candidatos, tse_receitas_candidato,
tse_despesas_candidato), verificando contagem de linhas em cada etapa.

Uso:
    python dados_campanha_carga_completa.py
    python dados_campanha_carga_completa.py --skip-download
    python dados_campanha_carga_completa.py --force-download --yes
    python dados_campanha_carga_completa.py --anos 2022,2024
    python dados_campanha_carga_completa.py --tabelas candidatos,doacoes

Requisitos:
    - BASEDOSDADOS_BILLING_PROJECT_ID definido no ambiente/.env
    - DB=postgres com DB_* + TSE_DB/TSE_SCHEMA no ambiente/.env
"""

import argparse
import csv
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config import (
    descrever_destino_postgres,
    resolver_caminho,
    resolver_config_postgres,
    resolver_schema,
)
from utils.db import conectar_postgres, garantir_schema, executar_sql, contar_tabela, LeitorSanitizado
from utils.log import configurar_log
from dotenv import load_dotenv
from google.cloud import bigquery
from pydata_google_auth import get_user_credentials


load_dotenv()

log = configurar_log("dados_campanha_carga_completa")

# ── Queries BigQuery (parametrizadas por ano) ──────────────────────────────

QUERY_CANDIDATOS_TPL = """
SELECT
  dados.ano as ano,
  dados.id_eleicao as id_eleicao,
  dados.tipo_eleicao as tipo_eleicao,
  dados.data_eleicao as data_eleicao,
  dados.sigla_uf AS sigla_uf,
  diretorio_sigla_uf.nome AS sigla_uf_nome,
  dados.id_municipio AS id_municipio,
  diretorio_id_municipio.nome AS id_municipio_nome,
  dados.id_municipio_tse AS id_municipio_tse,
  diretorio_id_municipio_tse.nome AS id_municipio_tse_nome,
  dados.titulo_eleitoral as titulo_eleitoral,
  dados.cpf as cpf,
  dados.sequencial as sequencial,
  dados.numero as numero,
  dados.nome as nome,
  dados.nome_urna as nome_urna,
  dados.numero_partido as numero_partido,
  dados.sigla_partido as sigla_partido,
  dados.cargo as cargo,
  dados.situacao as situacao,
  dados.data_nascimento as data_nascimento,
  dados.idade as idade,
  dados.genero as genero,
  dados.instrucao as instrucao,
  dados.ocupacao as ocupacao,
  dados.estado_civil as estado_civil,
  dados.nacionalidade as nacionalidade,
  dados.sigla_uf_nascimento as sigla_uf_nascimento,
  dados.municipio_nascimento as municipio_nascimento,
  dados.email as email,
  dados.raca as raca
FROM `basedosdados.br_tse_eleicoes.candidatos` AS dados
LEFT JOIN (
  SELECT DISTINCT sigla, nome
  FROM `basedosdados.br_bd_diretorios_brasil.uf`
) AS diretorio_sigla_uf
  ON dados.sigla_uf = diretorio_sigla_uf.sigla
LEFT JOIN (
  SELECT DISTINCT id_municipio, nome
  FROM `basedosdados.br_bd_diretorios_brasil.municipio`
) AS diretorio_id_municipio
  ON dados.id_municipio = diretorio_id_municipio.id_municipio
LEFT JOIN (
  SELECT DISTINCT id_municipio_tse, nome
  FROM `basedosdados.br_bd_diretorios_brasil.municipio`
) AS diretorio_id_municipio_tse
  ON dados.id_municipio_tse = diretorio_id_municipio_tse.id_municipio_tse
WHERE dados.ano = {ano}
"""

QUERY_DOACOES_TPL = """
SELECT
  dados.ano as ano,
  dados.turno as turno,
  dados.id_eleicao as id_eleicao,
  dados.tipo_eleicao as tipo_eleicao,
  dados.data_eleicao as data_eleicao,
  dados.sigla_uf AS sigla_uf,
  diretorio_sigla_uf.nome AS sigla_uf_nome,
  dados.id_municipio AS id_municipio,
  diretorio_id_municipio.nome AS id_municipio_nome,
  dados.id_municipio_tse AS id_municipio_tse,
  diretorio_id_municipio_tse.nome AS id_municipio_tse_nome,
  dados.titulo_eleitoral_candidato as titulo_eleitoral_candidato,
  dados.sequencial_candidato as sequencial_candidato,
  dados.numero_candidato as numero_candidato,
  dados.cnpj_candidato as cnpj_candidato,
  dados.numero_partido as numero_partido,
  dados.sigla_partido as sigla_partido,
  dados.cargo as cargo,
  dados.sequencial_receita as sequencial_receita,
  dados.data_receita as data_receita,
  dados.fonte_receita as fonte_receita,
  dados.origem_receita as origem_receita,
  dados.natureza_receita as natureza_receita,
  dados.especie_receita as especie_receita,
  dados.situacao_receita as situacao_receita,
  dados.descricao_receita as descricao_receita,
  dados.valor_receita as valor_receita,
  dados.sequencial_candidato_doador as sequencial_candidato_doador,
  dados.cpf_cnpj_doador as cpf_cnpj_doador,
  dados.sigla_uf_doador as sigla_uf_doador,
  dados.id_municipio_tse_doador as id_municipio_tse_doador,
  dados.nome_doador as nome_doador,
  dados.nome_doador_rf as nome_doador_rf,
  dados.cargo_candidato_doador as cargo_candidato_doador,
  dados.numero_partido_doador as numero_partido_doador,
  dados.sigla_partido_doador as sigla_partido_doador,
  dados.esfera_partidaria_doador as esfera_partidaria_doador,
  dados.numero_candidato_doador as numero_candidato_doador,
  dados.cnae_2_doador as cnae_2_doador,
  dados.cnae_2_doador_classe as cnae_2_doador_classe,
  dados.cnae_2_doador_subclasse AS cnae_2_doador_subclasse,
  diretorio_cnae_2_doador_subclasse.descricao_subclasse AS cnae_2_doador_subclasse_descricao_subclasse,
  diretorio_cnae_2_doador_subclasse.descricao_classe AS cnae_2_doador_subclasse_descricao_classe,
  diretorio_cnae_2_doador_subclasse.descricao_grupo AS cnae_2_doador_subclasse_descricao_grupo,
  diretorio_cnae_2_doador_subclasse.descricao_divisao AS cnae_2_doador_subclasse_descricao_divisao,
  diretorio_cnae_2_doador_subclasse.descricao_secao AS cnae_2_doador_subclasse_descricao_secao,
  dados.descricao_cnae_2_doador as descricao_cnae_2_doador,
  dados.cpf_cnpj_doador_orig as cpf_cnpj_doador_orig,
  dados.nome_doador_orig as nome_doador_orig,
  dados.nome_doador_orig_rf as nome_doador_orig_rf,
  dados.tipo_doador_orig as tipo_doador_orig,
  dados.descricao_cnae_2_doador_orig as descricao_cnae_2_doador_orig,
  dados.nome_administrador as nome_administrador,
  dados.cpf_administrador as cpf_administrador,
  dados.numero_recibo_eleitoral as numero_recibo_eleitoral,
  dados.numero_documento as numero_documento,
  dados.numero_recibo_doacao as numero_recibo_doacao,
  dados.numero_documento_doacao as numero_documento_doacao,
  dados.tipo_prestacao_contas as tipo_prestacao_contas,
  dados.data_prestacao_contas as data_prestacao_contas,
  dados.sequencial_prestador_contas as sequencial_prestador_contas,
  dados.cnpj_prestador_contas as cnpj_prestador_contas,
  dados.entrega_conjunto as entrega_conjunto
FROM `basedosdados.br_tse_eleicoes.receitas_candidato` AS dados
LEFT JOIN (
  SELECT DISTINCT sigla, nome
  FROM `basedosdados.br_bd_diretorios_brasil.uf`
) AS diretorio_sigla_uf
  ON dados.sigla_uf = diretorio_sigla_uf.sigla
LEFT JOIN (
  SELECT DISTINCT id_municipio, nome
  FROM `basedosdados.br_bd_diretorios_brasil.municipio`
) AS diretorio_id_municipio
  ON dados.id_municipio = diretorio_id_municipio.id_municipio
LEFT JOIN (
  SELECT DISTINCT id_municipio_tse, nome
  FROM `basedosdados.br_bd_diretorios_brasil.municipio`
) AS diretorio_id_municipio_tse
  ON dados.id_municipio_tse = diretorio_id_municipio_tse.id_municipio_tse
LEFT JOIN (
  SELECT DISTINCT
    subclasse,
    descricao_subclasse,
    descricao_classe,
    descricao_grupo,
    descricao_divisao,
    descricao_secao
  FROM `basedosdados.br_bd_diretorios_brasil.cnae_2`
) AS diretorio_cnae_2_doador_subclasse
  ON dados.cnae_2_doador_subclasse = diretorio_cnae_2_doador_subclasse.subclasse
WHERE dados.ano = {ano}
"""

QUERY_DESPESAS_TPL = """
SELECT
    dados.ano as ano,
    dados.turno as turno,
    dados.id_eleicao as id_eleicao,
    dados.tipo_eleicao as tipo_eleicao,
    dados.data_eleicao as data_eleicao,
    dados.sigla_uf AS sigla_uf,
    diretorio_sigla_uf.nome AS sigla_uf_nome,
    dados.id_municipio AS id_municipio,
    diretorio_id_municipio.nome AS id_municipio_nome,
    dados.id_municipio_tse AS id_municipio_tse,
    diretorio_id_municipio_tse.nome AS id_municipio_tse_nome,
    dados.titulo_eleitoral_candidato as titulo_eleitoral_candidato,
    dados.sequencial_candidato as sequencial_candidato,
    dados.numero_candidato as numero_candidato,
    dados.cnpj_candidato as cnpj_candidato,
    dados.numero_partido as numero_partido,
    dados.sigla_partido as sigla_partido,
    dados.cargo as cargo,
    dados.sequencial_despesa as sequencial_despesa,
    dados.data_despesa as data_despesa,
    dados.tipo_despesa as tipo_despesa,
    dados.descricao_despesa as descricao_despesa,
    dados.origem_despesa as origem_despesa,
    dados.valor_despesa as valor_despesa,
    dados.tipo_prestacao_contas as tipo_prestacao_contas,
    dados.data_prestacao_contas as data_prestacao_contas,
    dados.sequencial_prestador_contas as sequencial_prestador_contas,
    dados.cnpj_prestador_contas as cnpj_prestador_contas,
    dados.tipo_documento as tipo_documento,
    dados.numero_documento as numero_documento,
    dados.especie_recurso as especie_recurso,
    dados.fonte_recurso as fonte_recurso,
    dados.cpf_cnpj_fornecedor as cpf_cnpj_fornecedor,
    dados.nome_fornecedor as nome_fornecedor,
    dados.nome_fornecedor_rf as nome_fornecedor_rf,
    dados.cnae_2_fornecedor as cnae_2_fornecedor,
    dados.cnae_2_fornecedor_classe as cnae_2_fornecedor_classe,
    dados.cnae_2_fornecedor_subclasse AS cnae_2_fornecedor_subclasse,
    diretorio_cnae_2_fornecedor_subclasse.descricao_subclasse AS cnae_2_fornecedor_subclasse_descricao_subclasse,
    diretorio_cnae_2_fornecedor_subclasse.descricao_classe AS cnae_2_fornecedor_subclasse_descricao_classe,
    diretorio_cnae_2_fornecedor_subclasse.descricao_grupo AS cnae_2_fornecedor_subclasse_descricao_grupo,
    diretorio_cnae_2_fornecedor_subclasse.descricao_divisao AS cnae_2_fornecedor_subclasse_descricao_divisao,
    diretorio_cnae_2_fornecedor_subclasse.descricao_secao AS cnae_2_fornecedor_subclasse_descricao_secao,
    dados.descricao_cnae_2_fornecedor as descricao_cnae_2_fornecedor,
    dados.tipo_fornecedor as tipo_fornecedor,
    dados.esfera_partidaria_fornecedor as esfera_partidaria_fornecedor,
    dados.sigla_uf_fornecedor as sigla_uf_fornecedor,
    dados.id_municipio_tse_fornecedor as id_municipio_tse_fornecedor,
    dados.sequencial_candidato_fornecedor as sequencial_candidato_fornecedor,
    dados.numero_candidato_fornecedor as numero_candidato_fornecedor,
    dados.numero_partido_fornecedor as numero_partido_fornecedor,
    dados.sigla_partido_fornecedor as sigla_partido_fornecedor,
    dados.cargo_fornecedor as cargo_fornecedor
FROM `basedosdados.br_tse_eleicoes.despesas_candidato` AS dados
LEFT JOIN (
  SELECT DISTINCT sigla, nome
  FROM `basedosdados.br_bd_diretorios_brasil.uf`
) AS diretorio_sigla_uf
  ON dados.sigla_uf = diretorio_sigla_uf.sigla
LEFT JOIN (
  SELECT DISTINCT id_municipio, nome
  FROM `basedosdados.br_bd_diretorios_brasil.municipio`
) AS diretorio_id_municipio
  ON dados.id_municipio = diretorio_id_municipio.id_municipio
LEFT JOIN (
  SELECT DISTINCT id_municipio_tse, nome
  FROM `basedosdados.br_bd_diretorios_brasil.municipio`
) AS diretorio_id_municipio_tse
  ON dados.id_municipio_tse = diretorio_id_municipio_tse.id_municipio_tse
LEFT JOIN (
  SELECT DISTINCT
    subclasse,
    descricao_subclasse,
    descricao_classe,
    descricao_grupo,
    descricao_divisao,
    descricao_secao
  FROM `basedosdados.br_bd_diretorios_brasil.cnae_2`
) AS diretorio_cnae_2_fornecedor_subclasse
  ON dados.cnae_2_fornecedor_subclasse = diretorio_cnae_2_fornecedor_subclasse.subclasse
WHERE dados.ano = {ano}
"""

# ── Queries para descobrir anos disponíveis ────────────────────────────────

QUERY_ANOS_CANDIDATOS = "SELECT DISTINCT ano FROM `basedosdados.br_tse_eleicoes.candidatos` ORDER BY ano"
QUERY_ANOS_DOACOES = "SELECT DISTINCT ano FROM `basedosdados.br_tse_eleicoes.receitas_candidato` ORDER BY ano"
QUERY_ANOS_DESPESAS = "SELECT DISTINCT ano FROM `basedosdados.br_tse_eleicoes.despesas_candidato` ORDER BY ano"

# ── Colunas ─────────────────────────────────────────────────────────────────

COLUNAS_CANDIDATOS = [
    "ano", "id_eleicao", "tipo_eleicao", "data_eleicao",
    "sigla_uf", "sigla_uf_nome", "id_municipio", "id_municipio_nome",
    "id_municipio_tse", "id_municipio_tse_nome", "titulo_eleitoral",
    "cpf", "sequencial", "numero", "nome", "nome_urna",
    "numero_partido", "sigla_partido", "cargo", "situacao",
    "data_nascimento", "idade", "genero", "instrucao", "ocupacao",
    "estado_civil", "nacionalidade", "sigla_uf_nascimento",
    "municipio_nascimento", "email", "raca",
]

COLUNAS_DOACOES = [
    "ano", "turno", "id_eleicao", "tipo_eleicao", "data_eleicao",
    "sigla_uf", "sigla_uf_nome", "id_municipio", "id_municipio_nome",
    "id_municipio_tse", "id_municipio_tse_nome",
    "titulo_eleitoral_candidato", "sequencial_candidato",
    "numero_candidato", "cnpj_candidato", "numero_partido",
    "sigla_partido", "cargo", "sequencial_receita", "data_receita",
    "fonte_receita", "origem_receita", "natureza_receita",
    "especie_receita", "situacao_receita", "descricao_receita",
    "valor_receita", "sequencial_candidato_doador", "cpf_cnpj_doador",
    "sigla_uf_doador", "id_municipio_tse_doador", "nome_doador",
    "nome_doador_rf", "cargo_candidato_doador", "numero_partido_doador",
    "sigla_partido_doador", "esfera_partidaria_doador",
    "numero_candidato_doador", "cnae_2_doador", "cnae_2_doador_classe",
    "cnae_2_doador_subclasse",
    "cnae_2_doador_subclasse_descricao_subclasse",
    "cnae_2_doador_subclasse_descricao_classe",
    "cnae_2_doador_subclasse_descricao_grupo",
    "cnae_2_doador_subclasse_descricao_divisao",
    "cnae_2_doador_subclasse_descricao_secao",
    "descricao_cnae_2_doador", "cpf_cnpj_doador_orig",
    "nome_doador_orig", "nome_doador_orig_rf", "tipo_doador_orig",
    "descricao_cnae_2_doador_orig", "nome_administrador",
    "cpf_administrador", "numero_recibo_eleitoral", "numero_documento",
    "numero_recibo_doacao", "numero_documento_doacao",
    "tipo_prestacao_contas", "data_prestacao_contas",
    "sequencial_prestador_contas", "cnpj_prestador_contas",
    "entrega_conjunto",
]

COLUNAS_DESPESAS = [
    "ano", "turno", "id_eleicao", "tipo_eleicao", "data_eleicao",
    "sigla_uf", "sigla_uf_nome", "id_municipio", "id_municipio_nome",
    "id_municipio_tse", "id_municipio_tse_nome",
    "titulo_eleitoral_candidato", "sequencial_candidato",
    "numero_candidato", "cnpj_candidato", "numero_partido",
    "sigla_partido", "cargo",
    "sequencial_despesa", "data_despesa", "tipo_despesa",
    "descricao_despesa", "origem_despesa", "valor_despesa",
    "tipo_prestacao_contas", "data_prestacao_contas",
    "sequencial_prestador_contas", "cnpj_prestador_contas",
    "tipo_documento", "numero_documento", "especie_recurso", "fonte_recurso",
    "cpf_cnpj_fornecedor", "nome_fornecedor", "nome_fornecedor_rf",
    "cnae_2_fornecedor", "cnae_2_fornecedor_classe", "cnae_2_fornecedor_subclasse",
    "cnae_2_fornecedor_subclasse_descricao_subclasse",
    "cnae_2_fornecedor_subclasse_descricao_classe",
    "cnae_2_fornecedor_subclasse_descricao_grupo",
    "cnae_2_fornecedor_subclasse_descricao_divisao",
    "cnae_2_fornecedor_subclasse_descricao_secao",
    "descricao_cnae_2_fornecedor",
    "tipo_fornecedor", "esfera_partidaria_fornecedor", "sigla_uf_fornecedor",
    "id_municipio_tse_fornecedor", "sequencial_candidato_fornecedor",
    "numero_candidato_fornecedor", "numero_partido_fornecedor",
    "sigla_partido_fornecedor", "cargo_fornecedor",
]

# ── Tipos ────────────────────────────────────────────────────────────────────

TIPOS_CANDIDATOS = {
    "ano": "INTEGER",
    "id_eleicao": "VARCHAR(32)",
    "tipo_eleicao": "VARCHAR(64)",
    "data_eleicao": "DATE",
    "sigla_uf": "VARCHAR(2)",
    "sigla_uf_nome": "VARCHAR(64)",
    "id_municipio": "VARCHAR(16)",
    "id_municipio_nome": "VARCHAR(128)",
    "id_municipio_tse": "VARCHAR(16)",
    "id_municipio_tse_nome": "VARCHAR(128)",
    "titulo_eleitoral": "VARCHAR(12)",
    "cpf": "VARCHAR(11)",
    "sequencial": "VARCHAR(32)",
    "numero": "VARCHAR(16)",
    "nome": "VARCHAR(255)",
    "nome_urna": "VARCHAR(128)",
    "numero_partido": "VARCHAR(8)",
    "sigla_partido": "VARCHAR(20)",
    "cargo": "VARCHAR(64)",
    "situacao": "VARCHAR(64)",
    "data_nascimento": "DATE",
    "idade": "INTEGER",
    "genero": "VARCHAR(32)",
    "instrucao": "VARCHAR(64)",
    "ocupacao": "VARCHAR(128)",
    "estado_civil": "VARCHAR(32)",
    "nacionalidade": "VARCHAR(64)",
    "sigla_uf_nascimento": "VARCHAR(2)",
    "municipio_nascimento": "VARCHAR(128)",
    "email": "VARCHAR(320)",
    "raca": "VARCHAR(32)",
}

TIPOS_DOACOES = {
    "ano": "INTEGER",
    "turno": "INTEGER",
    "id_eleicao": "VARCHAR(32)",
    "tipo_eleicao": "VARCHAR(64)",
    "data_eleicao": "DATE",
    "sigla_uf": "VARCHAR(2)",
    "sigla_uf_nome": "VARCHAR(64)",
    "id_municipio": "VARCHAR(16)",
    "id_municipio_nome": "VARCHAR(128)",
    "id_municipio_tse": "VARCHAR(16)",
    "id_municipio_tse_nome": "VARCHAR(128)",
    "titulo_eleitoral_candidato": "VARCHAR(12)",
    "sequencial_candidato": "VARCHAR(32)",
    "numero_candidato": "VARCHAR(16)",
    "cnpj_candidato": "VARCHAR(14)",
    "numero_partido": "VARCHAR(8)",
    "sigla_partido": "VARCHAR(20)",
    "cargo": "VARCHAR(64)",
    "sequencial_receita": "VARCHAR(32)",
    "data_receita": "DATE",
    "fonte_receita": "VARCHAR(64)",
    "origem_receita": "VARCHAR(128)",
    "natureza_receita": "VARCHAR(32)",
    "especie_receita": "VARCHAR(64)",
    "situacao_receita": "VARCHAR(64)",
    "descricao_receita": "VARCHAR(2000)",
    "valor_receita": "NUMERIC(18,2)",
    "sequencial_candidato_doador": "VARCHAR(32)",
    "cpf_cnpj_doador": "VARCHAR(14)",
    "sigla_uf_doador": "VARCHAR(2)",
    "id_municipio_tse_doador": "VARCHAR(16)",
    "nome_doador": "VARCHAR(255)",
    "nome_doador_rf": "VARCHAR(255)",
    "cargo_candidato_doador": "VARCHAR(64)",
    "numero_partido_doador": "VARCHAR(8)",
    "sigla_partido_doador": "VARCHAR(20)",
    "esfera_partidaria_doador": "VARCHAR(64)",
    "numero_candidato_doador": "VARCHAR(16)",
    "cnae_2_doador": "VARCHAR(16)",
    "cnae_2_doador_classe": "VARCHAR(16)",
    "cnae_2_doador_subclasse": "VARCHAR(16)",
    "cnae_2_doador_subclasse_descricao_subclasse": "VARCHAR(255)",
    "cnae_2_doador_subclasse_descricao_classe": "VARCHAR(255)",
    "cnae_2_doador_subclasse_descricao_grupo": "VARCHAR(255)",
    "cnae_2_doador_subclasse_descricao_divisao": "VARCHAR(255)",
    "cnae_2_doador_subclasse_descricao_secao": "VARCHAR(255)",
    "descricao_cnae_2_doador": "VARCHAR(255)",
    "cpf_cnpj_doador_orig": "VARCHAR(14)",
    "nome_doador_orig": "VARCHAR(255)",
    "nome_doador_orig_rf": "VARCHAR(255)",
    "tipo_doador_orig": "VARCHAR(64)",
    "descricao_cnae_2_doador_orig": "VARCHAR(255)",
    "nome_administrador": "VARCHAR(255)",
    "cpf_administrador": "VARCHAR(14)",
    "numero_recibo_eleitoral": "VARCHAR(64)",
    "numero_documento": "VARCHAR(64)",
    "numero_recibo_doacao": "VARCHAR(64)",
    "numero_documento_doacao": "VARCHAR(64)",
    "tipo_prestacao_contas": "VARCHAR(32)",
    "data_prestacao_contas": "DATE",
    "sequencial_prestador_contas": "VARCHAR(32)",
    "cnpj_prestador_contas": "VARCHAR(14)",
    "entrega_conjunto": "VARCHAR(32)",
}

TIPOS_DESPESAS = {
    "ano": "INTEGER",
    "turno": "INTEGER",
    "id_eleicao": "VARCHAR(32)",
    "tipo_eleicao": "VARCHAR(64)",
    "data_eleicao": "DATE",
    "sigla_uf": "VARCHAR(2)",
    "sigla_uf_nome": "VARCHAR(64)",
    "id_municipio": "VARCHAR(16)",
    "id_municipio_nome": "VARCHAR(128)",
    "id_municipio_tse": "VARCHAR(16)",
    "id_municipio_tse_nome": "VARCHAR(128)",
    "titulo_eleitoral_candidato": "VARCHAR(12)",
    "sequencial_candidato": "VARCHAR(32)",
    "numero_candidato": "VARCHAR(16)",
    "cnpj_candidato": "VARCHAR(14)",
    "numero_partido": "VARCHAR(8)",
    "sigla_partido": "VARCHAR(20)",
    "cargo": "VARCHAR(64)",
    "sequencial_despesa": "VARCHAR(32)",
    "data_despesa": "DATE",
    "tipo_despesa": "VARCHAR(64)",
    "descricao_despesa": "VARCHAR(2000)",
    "origem_despesa": "VARCHAR(128)",
    "valor_despesa": "NUMERIC(18,2)",
    "tipo_prestacao_contas": "VARCHAR(32)",
    "data_prestacao_contas": "DATE",
    "sequencial_prestador_contas": "VARCHAR(32)",
    "cnpj_prestador_contas": "VARCHAR(14)",
    "tipo_documento": "VARCHAR(64)",
    "numero_documento": "VARCHAR(64)",
    "especie_recurso": "VARCHAR(64)",
    "fonte_recurso": "VARCHAR(64)",
    "cpf_cnpj_fornecedor": "VARCHAR(14)",
    "nome_fornecedor": "VARCHAR(255)",
    "nome_fornecedor_rf": "VARCHAR(255)",
    "cnae_2_fornecedor": "VARCHAR(16)",
    "cnae_2_fornecedor_classe": "VARCHAR(16)",
    "cnae_2_fornecedor_subclasse": "VARCHAR(16)",
    "cnae_2_fornecedor_subclasse_descricao_subclasse": "VARCHAR(255)",
    "cnae_2_fornecedor_subclasse_descricao_classe": "VARCHAR(255)",
    "cnae_2_fornecedor_subclasse_descricao_grupo": "VARCHAR(255)",
    "cnae_2_fornecedor_subclasse_descricao_divisao": "VARCHAR(255)",
    "cnae_2_fornecedor_subclasse_descricao_secao": "VARCHAR(255)",
    "descricao_cnae_2_fornecedor": "VARCHAR(255)",
    "tipo_fornecedor": "VARCHAR(64)",
    "esfera_partidaria_fornecedor": "VARCHAR(64)",
    "sigla_uf_fornecedor": "VARCHAR(2)",
    "id_municipio_tse_fornecedor": "VARCHAR(16)",
    "sequencial_candidato_fornecedor": "VARCHAR(32)",
    "numero_candidato_fornecedor": "VARCHAR(16)",
    "numero_partido_fornecedor": "VARCHAR(8)",
    "sigla_partido_fornecedor": "VARCHAR(20)",
    "cargo_fornecedor": "VARCHAR(64)",
}

# ── Indices (criados ao final, apos todos os anos) ──────────────────────────

INDICES_CANDIDATOS = [
    "CREATE INDEX idx_tse_candidatos_ano ON tse_candidatos (ano)",
    "CREATE INDEX idx_tse_candidatos_sequencial ON tse_candidatos (sequencial)",
    "CREATE INDEX idx_tse_candidatos_cpf ON tse_candidatos (cpf)",
    "CREATE INDEX idx_tse_candidatos_numero ON tse_candidatos (numero)",
    """CREATE INDEX idx_tse_candidatos_titulo_eleitoral_export
    ON tse_candidatos (titulo_eleitoral, cpf, numero, nome, nome_urna, ocupacao)""",
]

INDICES_DOACOES = [
    "CREATE INDEX idx_tse_receitas_candidato_ano ON tse_receitas_candidato (ano)",
    "CREATE INDEX idx_tse_receitas_candidato_titulo_eleitoral_candidato ON tse_receitas_candidato (titulo_eleitoral_candidato)",
    "CREATE INDEX idx_tse_receitas_candidato_sequencial_candidato ON tse_receitas_candidato (sequencial_candidato)",
    "CREATE INDEX idx_tse_receitas_candidato_sequencial_receita ON tse_receitas_candidato (sequencial_receita)",
    "CREATE INDEX idx_tse_receitas_candidato_cnpj_candidato ON tse_receitas_candidato (cnpj_candidato)",
    "CREATE INDEX idx_tse_receitas_candidato_cnpj_prestador_contas ON tse_receitas_candidato (cnpj_prestador_contas)",
    """CREATE INDEX idx_tse_receitas_candidato_id_doador_export
    ON tse_receitas_candidato (
        (CASE
            WHEN regexp_replace(COALESCE(cpf_cnpj_doador, ''), '\\D', '', 'g') IS NOT NULL
             AND LENGTH(regexp_replace(COALESCE(cpf_cnpj_doador, ''), '\\D', '', 'g')) IN (11, 14)
            THEN regexp_replace(COALESCE(cpf_cnpj_doador, ''), '\\D', '', 'g')
            ELSE NULL
        END)
    )
    INCLUDE (cpf_cnpj_doador, nome_doador, nome_doador_rf)""",
    """CREATE INDEX idx_tse_receitas_candidato_doc_nome_doador_norm
    ON tse_receitas_candidato (
        (regexp_replace(COALESCE(cpf_cnpj_doador, ''), '\\D', '', 'g')),
        (UPPER(TRIM(COALESCE(NULLIF(nome_doador_rf, ''), NULLIF(nome_doador, '')))))
    )
    WHERE LENGTH(regexp_replace(COALESCE(cpf_cnpj_doador, ''), '\\D', '', 'g')) = 11
      AND NULLIF(TRIM(COALESCE(NULLIF(nome_doador_rf, ''), NULLIF(nome_doador, ''))), '') IS NOT NULL""",
    "CREATE INDEX idx_tse_receitas_candidato_doc_doador_orig_norm ON tse_receitas_candidato ((regexp_replace(COALESCE(cpf_cnpj_doador_orig, ''), '\\D', '', 'g')))",
    "CREATE INDEX idx_tse_receitas_candidato_cpf_administrador ON tse_receitas_candidato (cpf_administrador)",
    "CREATE INDEX idx_tse_receitas_candidato_cpf_cnpj_doador ON tse_receitas_candidato (cpf_cnpj_doador)",
]

INDICES_DESPESAS = [
    "CREATE INDEX idx_tse_despesas_candidato_ano ON tse_despesas_candidato (ano)",
    "CREATE INDEX idx_tse_despesas_candidato_sequencial_candidato ON tse_despesas_candidato (sequencial_candidato)",
    "CREATE INDEX idx_tse_despesas_candidato_sequencial_despesa ON tse_despesas_candidato (sequencial_despesa)",
    "CREATE INDEX idx_tse_despesas_candidato_cnpj_candidato ON tse_despesas_candidato (cnpj_candidato)",
    "CREATE INDEX idx_tse_despesas_candidato_cnpj_prestador_contas ON tse_despesas_candidato (cnpj_prestador_contas)",
    """CREATE INDEX idx_tse_despesas_candidato_id_fornecedor_export
    ON tse_despesas_candidato (
        (CASE
            WHEN regexp_replace(COALESCE(cpf_cnpj_fornecedor, ''), '\\D', '', 'g') IS NOT NULL
             AND LENGTH(regexp_replace(COALESCE(cpf_cnpj_fornecedor, ''), '\\D', '', 'g')) IN (11, 14)
            THEN regexp_replace(COALESCE(cpf_cnpj_fornecedor, ''), '\\D', '', 'g')
            ELSE NULL
        END)
    )
    INCLUDE (cpf_cnpj_fornecedor, nome_fornecedor, nome_fornecedor_rf)""",
    "CREATE INDEX idx_tse_despesas_candidato_cpf_cnpj_fornecedor ON tse_despesas_candidato (cpf_cnpj_fornecedor)",
]

# ── Configuracao por finalidade ──────────────────────────────────────────────

CONFIGS = {
    "candidatos": {
        "query_tpl": QUERY_CANDIDATOS_TPL,
        "query_anos": QUERY_ANOS_CANDIDATOS,
        "colunas": COLUNAS_CANDIDATOS,
        "tipos": TIPOS_CANDIDATOS,
        "indices": INDICES_CANDIDATOS,
        "tabela_destino": "tse_candidatos",
        "tabela_staging": "tse_candidatos_staging",
        "arquivo_csv_tpl": "candidatos_{ano}.csv",
        "descricao": "candidatos de campanha",
    },
    "doacoes": {
        "query_tpl": QUERY_DOACOES_TPL,
        "query_anos": QUERY_ANOS_DOACOES,
        "colunas": COLUNAS_DOACOES,
        "tipos": TIPOS_DOACOES,
        "indices": INDICES_DOACOES,
        "tabela_destino": "tse_receitas_candidato",
        "tabela_staging": "tse_receitas_candidato_staging",
        "arquivo_csv_tpl": "receitas_candidato_{ano}.csv",
        "descricao": "receitas de campanha",
    },
    "despesas": {
        "query_tpl": QUERY_DESPESAS_TPL,
        "query_anos": QUERY_ANOS_DESPESAS,
        "colunas": COLUNAS_DESPESAS,
        "tipos": TIPOS_DESPESAS,
        "indices": INDICES_DESPESAS,
        "tabela_destino": "tse_despesas_candidato",
        "tabela_staging": "tse_despesas_candidato_staging",
        "arquivo_csv_tpl": "despesas_candidato_{ano}.csv",
        "descricao": "despesas de campanha",
    },
}

# ── Configuracao de ambiente ─────────────────────────────────────────────────

BASEDOSDADOS_BILLING_PROJECT_ID = os.getenv("BASEDOSDADOS_BILLING_PROJECT_ID") or ""
DOACOES_CAMPANHA_RAW_DIR = resolver_caminho("DOACOES_CAMPANHA_RAW_DIR", "output/dados-doacoes-campanha")
GOOGLE_SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]
TSE_SCHEMA = resolver_schema("TSE_SCHEMA", padrao="public")
POSTGRES_KWARGS = resolver_config_postgres(
    "TSE_DB",
    schema_names=[TSE_SCHEMA],
    purpose="carga completa TSE",
)
POSTGRES_TARGET = descrever_destino_postgres(
    database_name=os.getenv("TSE_DB"),
    schema_names=[TSE_SCHEMA],
)


# ── Funcoes auxiliares ───────────────────────────────────────────────────────

def _conectar():
    return conectar_postgres(**POSTGRES_KWARGS)


def _validar_billing():
    if BASEDOSDADOS_BILLING_PROJECT_ID:
        return
    log.error("ERRO: defina BASEDOSDADOS_BILLING_PROJECT_ID no ambiente/.env.")
    sys.exit(1)


def _validar_csv(caminho):
    if not os.path.isfile(caminho):
        log.error(f"ERRO: arquivo nao encontrado: {caminho}")
        sys.exit(1)
    tamanho = os.path.getsize(caminho)
    if tamanho <= 0:
        log.error(f"ERRO: arquivo vazio: {caminho}")
        sys.exit(1)
    log.info(f"CSV OK: {caminho} ({tamanho / 1024 / 1024:.1f} MB)")


def _criar_bigquery_client():
    _validar_billing()
    credentials = get_user_credentials(GOOGLE_SCOPES)
    return bigquery.Client(
        project=BASEDOSDADOS_BILLING_PROJECT_ID,
        credentials=credentials,
    )


def _descobrir_anos(client, cfg):
    """Consulta os anos distintos disponíveis no BigQuery para uma tabela."""
    log.info(f"Consultando anos disponíveis para {cfg['descricao']}...")
    result = client.query(cfg["query_anos"]).result()
    anos = sorted(int(row["ano"]) for row in result if row["ano"] is not None)
    log.info(f"Anos disponíveis para {cfg['descricao']}: {anos}")
    return anos


def _contar_bigquery_ano(client, cfg, ano):
    """Conta linhas no BigQuery para um ano específico."""
    query = cfg["query_tpl"].format(ano=ano)
    count_query = f"SELECT COUNT(*) AS total FROM ({query}) AS base"
    log.info(f"Consultando total esperado no BigQuery para ano {ano}...")
    total = int(next(client.query(count_query).result())["total"])
    log.info(f"Total esperado no BigQuery para ano {ano}: {total}")
    return total


def _baixar_csv_ano(client, cfg, ano, force_download=False):
    """Baixa CSV de um ano específico via BigQuery client."""
    arquivo_csv = os.path.join(
        DOACOES_CAMPANHA_RAW_DIR,
        cfg["arquivo_csv_tpl"].format(ano=ano),
    )
    os.makedirs(DOACOES_CAMPANHA_RAW_DIR, exist_ok=True)

    if os.path.isfile(arquivo_csv) and not force_download:
        log.info(f"CSV ja existe, reutilizando: {arquivo_csv}")
        _validar_csv(arquivo_csv)
        return arquivo_csv

    if os.path.isfile(arquivo_csv):
        os.remove(arquivo_csv)

    query = cfg["query_tpl"].format(ano=ano)
    log.info(f"Baixando {cfg['descricao']} ano {ano} do BigQuery para: {arquivo_csv}")

    try:
        job = client.query(query)
        result = job.result(page_size=20000)
        header = [field.name for field in result.schema]

        total = 0
        with open(arquivo_csv, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(header)
            for row in result:
                writer.writerow(["" if valor is None else valor for valor in row])
                total += 1
                if total % 100000 == 0:
                    log.info(f"  Linhas gravadas no CSV: {total}")

        log.info(f"Download finalizado. Total de linhas: {total}")
    except Exception:
        if os.path.exists(arquivo_csv):
            os.remove(arquivo_csv)
        raise

    _validar_csv(arquivo_csv)
    return arquivo_csv


def _ddl_tabela(nome_tabela, colunas):
    cols = ",\n    ".join(f"{col} TEXT" for col in colunas)
    return f"CREATE TABLE {nome_tabela} (\n    {cols}\n)"


def _ddl_tabela_tipada(nome_tabela, colunas, tipos):
    cols = ",\n    ".join(f"{col} {tipos[col]}" for col in colunas)
    return f"CREATE TABLE {nome_tabela} (\n    {cols}\n)"


def _expr_cast(coluna, tipos):
    tipo = tipos[coluna]
    valor = f"NULLIF(BTRIM({coluna}), '')"
    return f"{valor}::{tipo} AS {coluna}"


def _sql_insert_tipado(destino, staging, colunas, tipos):
    cols = ", ".join(colunas)
    selects = ",\n    ".join(_expr_cast(col, tipos) for col in colunas)
    return f"""
    INSERT INTO {destino} ({cols})
    SELECT
        {selects}
    FROM {staging}
    """


def _tabela_existe(cur, tabela):
    """Verifica se uma tabela existe no schema atual."""
    cur.execute(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)",
        (tabela,),
    )
    return cur.fetchone()[0]


def _contar_ano_postgres(cur, tabela, ano):
    """Conta linhas de um ano específico numa tabela PostgreSQL."""
    cur.execute(f"SELECT COUNT(*) FROM {tabela} WHERE ano = %s", (ano,))
    return int(cur.fetchone()[0])


def _criar_tabelas(conn, configs_usados):
    """Cria as tabelas tipadas (se não existirem) e remove dados anteriores."""
    with conn.cursor() as cur:
        garantir_schema(cur, TSE_SCHEMA)
        for finalidade, cfg in configs_usados.items():
            tabela = cfg["tabela_destino"]
            if not _tabela_existe(cur, tabela):
                log.info(f"Criando tabela {tabela}...")
                executar_sql(
                    cur,
                    _ddl_tabela_tipada(tabela, cfg["colunas"], cfg["tipos"]),
                    f"Criando tabela {tabela}",
                    log.info,
                )
            else:
                log.info(f"Tabela {tabela} ja existe, sera reutilizada (append por ano).")
    conn.commit()


def _carregar_ano(conn, cfg, caminho_csv, ano, total_esperado_bigquery):
    """Carrega CSV de um ano na tabela destino via staging."""
    tabela_destino = cfg["tabela_destino"]
    tabela_staging = cfg["tabela_staging"]
    colunas = cfg["colunas"]
    tipos = cfg["tipos"]

    with conn.cursor() as cur:
        # Verificar se ano ja esta carregado
        if _tabela_existe(cur, tabela_destino):
            total_existente = _contar_ano_postgres(cur, tabela_destino, ano)
            if total_existente == total_esperado_bigquery and total_esperado_bigquery > 0:
                log.info(
                    f"Ano {ano} ja carregado em {tabela_destino} "
                    f"({total_existente} linhas). Pulando."
                )
                return
            if total_existente > 0:
                log.info(
                    f"Ano {ano} parcialmente carregado em {tabela_destino} "
                    f"({total_existente} linhas, esperado {total_esperado_bigquery}). "
                    f"Removendo dados do ano para recarregar..."
                )
                executar_sql(
                    cur,
                    f"DELETE FROM {tabela_destino} WHERE ano = {ano}",
                    f"Removendo dados parciais do ano {ano}",
                    log.info,
                )

        # Criar staging
        executar_sql(cur, f"DROP TABLE IF EXISTS {tabela_staging}", "Removendo staging anterior", log.info)
        executar_sql(cur, _ddl_tabela(tabela_staging, colunas), "Criando staging", log.info)

        # COPY CSV para staging
        log.info(f"Executando COPY para staging (ano {ano})...")
        with open(caminho_csv, "r", encoding="utf-8", newline="") as f:
            reader = LeitorSanitizado(f)
            cur.copy_expert(
                f"""
                COPY {tabela_staging} ({", ".join(colunas)})
                FROM STDIN
                WITH (FORMAT CSV, HEADER TRUE, ENCODING 'UTF8')
                """,
                reader,
            )
            if reader.null_bytes_removed:
                log.info(
                    f"COPY concluiu com sanitizacao de "
                    f"{reader.null_bytes_removed} byte(s) NUL removido(s)"
                )

        # Validar staging
        total_staging = contar_tabela(cur, tabela_staging)
        log.info(f"Total na staging: {total_staging}")
        if total_staging != total_esperado_bigquery:
            raise RuntimeError(
                f"Divergencia ano {ano}: BigQuery={total_esperado_bigquery}, "
                f"staging={total_staging}. "
                f"Rode novamente com --force-download se o CSV local esta desatualizado."
            )

        # INSERT tipado da staging para destino
        executar_sql(
            cur,
            _sql_insert_tipado(tabela_destino, tabela_staging, colunas, tipos),
            f"Inserindo ano {ano} na tabela {tabela_destino}",
            log.info,
        )

        # Validar contagem no destino para esse ano
        total_ano = _contar_ano_postgres(cur, tabela_destino, ano)
        log.info(f"Total do ano {ano} em {tabela_destino}: {total_ano}")
        if total_ano != total_esperado_bigquery:
            raise RuntimeError(
                f"Divergencia apos insert ano {ano}: "
                f"BigQuery={total_esperado_bigquery}, PostgreSQL={total_ano}."
            )

        # Limpar staging
        executar_sql(cur, f"DROP TABLE IF EXISTS {tabela_staging}", "Removendo staging", log.info)

    conn.commit()
    log.info(f"Ano {ano} carregado com sucesso em {tabela_destino}: {total_ano} linhas.")


def _criar_indices(conn, configs_usados):
    """Cria índices em todas as tabelas ao final da carga."""
    with conn.cursor() as cur:
        for finalidade, cfg in configs_usados.items():
            tabela = cfg["tabela_destino"]
            log.info(f"--- Criando indices para {tabela} ---")
            for sql in cfg["indices"]:
                nome_idx = sql.split()[2]
                # Verificar se indice ja existe para pular
                cur.execute(
                    "SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = %s)",
                    (nome_idx,),
                )
                if cur.fetchone()[0]:
                    log.info(f"Indice {nome_idx} ja existe, pulando.")
                    continue
                executar_sql(cur, sql, f"Criando indice: {nome_idx}", log.info)
            executar_sql(cur, f"ANALYZE {tabela}", f"ANALYZE {tabela}", log.info)
    conn.commit()


def _resumo_final(conn, configs_usados):
    """Exibe contagem total por tabela e por ano."""
    with conn.cursor() as cur:
        for finalidade, cfg in configs_usados.items():
            tabela = cfg["tabela_destino"]
            total = contar_tabela(cur, tabela)
            log.info(f"=== {tabela}: {total} linhas total ===")
            cur.execute(
                f"SELECT ano, COUNT(*) AS total FROM {tabela} GROUP BY ano ORDER BY ano"
            )
            for row in cur.fetchall():
                log.info(f"  ano {row[0]}: {row[1]} linhas")


def _confirmar(args, configs_usados, anos_filtro):
    if args.yes:
        return
    tabelas = ", ".join(cfg["tabela_destino"] for cfg in configs_usados.values())
    filtro_txt = f" (anos: {anos_filtro})" if anos_filtro else " (todos os anos)"
    resp = input(
        f"Isto ira baixar dados do BigQuery e carregar nas tabelas "
        f"{tabelas} no PostgreSQL {POSTGRES_TARGET}{filtro_txt}.\n"
        f"Deseja prosseguir? (S/N)? "
    )
    if not resp or resp.upper() != "S":
        sys.exit()


def main():
    parser = argparse.ArgumentParser(
        description="Copia todos os anos eleitorais do BigQuery (BasedosDados) para o PostgreSQL."
    )
    parser.add_argument("--skip-download", action="store_true", help="Reutiliza os CSVs locais existentes")
    parser.add_argument("--force-download", action="store_true", help="Baixa novamente os CSVs das consultas")
    parser.add_argument("--yes", action="store_true", help="Nao solicita confirmacao interativa")
    parser.add_argument(
        "--anos",
        type=str,
        default=None,
        help="Anos específicos separados por vírgula (ex: 2022,2024). Padrão: todos.",
    )
    parser.add_argument(
        "--tabelas",
        type=str,
        default=None,
        help="Tabelas específicas separadas por vírgula (candidatos,doacoes,despesas). Padrão: todas.",
    )
    parser.add_argument(
        "--drop-tabelas",
        action="store_true",
        help="Remove as tabelas existentes e recria do zero (cuidado: perde dados anteriores).",
    )
    args = parser.parse_args()

    log.info(f"Inicio de {sys.argv[0]}")
    log.info(f"Pasta de dados brutos: {DOACOES_CAMPANHA_RAW_DIR}")
    log.info(f"Destino PostgreSQL: {POSTGRES_TARGET}")

    # Filtrar tabelas
    if args.tabelas:
        nomes = [t.strip() for t in args.tabelas.split(",")]
        configs_usados = {k: v for k, v in CONFIGS.items() if k in nomes}
        if not configs_usados:
            log.error(f"Nenhuma tabela válida em --tabelas={args.tabelas}. Opções: candidatos, doacoes, despesas")
            sys.exit(1)
    else:
        configs_usados = CONFIGS

    # Parsear filtro de anos
    anos_filtro = None
    if args.anos:
        anos_filtro = sorted(int(a.strip()) for a in args.anos.split(","))
        log.info(f"Filtro de anos: {anos_filtro}")

    _confirmar(args, configs_usados, anos_filtro)

    # Criar client BigQuery (reutilizado para toda a sessão)
    _validar_billing()
    client = _criar_bigquery_client()

    # Conexão PostgreSQL persistente
    conn = _conectar()
    try:
        # Drop tabelas se solicitado
        if args.drop_tabelas:
            with conn.cursor() as cur:
                garantir_schema(cur, TSE_SCHEMA)
                for cfg in configs_usados.values():
                    tabela = cfg["tabela_destino"]
                    executar_sql(
                        cur,
                        f"DROP TABLE IF EXISTS {tabela} CASCADE",
                        f"Removendo tabela {tabela}",
                        log.info,
                    )
            conn.commit()

        # Criar tabelas (se não existem)
        _criar_tabelas(conn, configs_usados)

        # Processar cada tabela/ano
        for finalidade, cfg in configs_usados.items():
            log.info(f"")
            log.info(f"{'=' * 60}")
            log.info(f"  {cfg['descricao'].upper()}")
            log.info(f"{'=' * 60}")

            # Descobrir anos disponíveis
            if anos_filtro:
                anos = anos_filtro
                log.info(f"Usando anos do filtro: {anos}")
            else:
                anos = _descobrir_anos(client, cfg)

            for ano in anos:
                log.info(f"")
                log.info(f"--- {finalidade.upper()} / ANO {ano} ---")

                # 1) Contar no BigQuery
                total_bq = _contar_bigquery_ano(client, cfg, ano)
                if total_bq == 0:
                    log.info(f"Ano {ano} sem dados no BigQuery para {cfg['descricao']}. Pulando.")
                    continue

                # 2) Baixar CSV
                arquivo_csv = os.path.join(
                    DOACOES_CAMPANHA_RAW_DIR,
                    cfg["arquivo_csv_tpl"].format(ano=ano),
                )
                if args.skip_download:
                    _validar_csv(arquivo_csv)
                    caminho_csv = arquivo_csv
                else:
                    caminho_csv = _baixar_csv_ano(
                        client, cfg, ano, force_download=args.force_download
                    )

                # 3) Carregar no PostgreSQL
                _carregar_ano(conn, cfg, caminho_csv, ano, total_bq)

        # Criar indices ao final
        log.info(f"")
        log.info(f"{'=' * 60}")
        log.info(f"  CRIANDO INDICES")
        log.info(f"{'=' * 60}")
        _criar_indices(conn, configs_usados)

        # Resumo final
        log.info(f"")
        log.info(f"{'=' * 60}")
        log.info(f"  RESUMO FINAL")
        log.info(f"{'=' * 60}")
        _resumo_final(conn, configs_usados)

    finally:
        conn.close()

    log.info("FIM")


if __name__ == "__main__":
    main()
