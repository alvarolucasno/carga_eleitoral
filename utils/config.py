# -*- coding: utf-8 -*-
"""Helpers para resolver configuracao via .env."""

import os
from urllib.parse import quote


DB_CHOICES = {"mysql", "postgres"}
DB_ALIASES = {
    "pg": "postgres",
    "postgresql": "postgres",
    "pcse": "postgres",
}


def obter_env(nome, padrao=None):
    valor = os.getenv(nome)
    if valor is None:
        return padrao
    valor = valor.strip()
    return valor if valor else padrao


def resolver_caminho(nome_env, padrao):
    valor = obter_env(nome_env, padrao)
    return os.path.abspath(os.path.expandvars(os.path.expanduser(valor)))


def normalizar_tipo_db(valor):
    if not valor:
        return ""
    valor = valor.strip().lower()
    return DB_ALIASES.get(valor, valor)


def resolver_tipo_db(db_cli=None, env_name="DB"):
    if db_cli:
        bruto = db_cli
        origem = "--db"
    else:
        bruto = obter_env(env_name)
        origem = env_name

    db_kind = normalizar_tipo_db(bruto)
    if not db_kind:
        raise SystemExit(f"ERRO: defina {env_name} no ambiente/.env.")
    if db_kind not in DB_CHOICES:
        raise SystemExit(f"ERRO: valor invalido para {origem}. Use mysql ou postgres.")
    return db_kind, origem


def resolver_schema(env_name, padrao=None):
    valor = obter_env(env_name)
    return valor if valor is not None else padrao


def escapar_identificador_pg(nome):
    return '"' + (nome or "").replace('"', '""') + '"'


def _search_path(schema_names):
    unicos = []
    for nome in schema_names or []:
        nome = (nome or "").strip()
        if nome and nome not in unicos:
            unicos.append(nome)
    return ",".join(unicos)


def _build_relational_url(db_kind, database_name=None):
    if db_kind == "postgres":
        driver = "postgresql"
        database_name = database_name or "postgres"
        usuario_padrao = "postgres"
        senha_padrao = "33441065"
        porta_padrao = "5432"
    else:
        driver = "mysql+pymysql"
        if not database_name:
            raise SystemExit("ERRO: defina o nome do database MySQL no ambiente/.env.")
        usuario_padrao = "root"
        senha_padrao = ""
        porta_padrao = "3306"

    usuario = obter_env("DB_USER", usuario_padrao)
    senha = obter_env("DB_PASSWORD", senha_padrao)
    host = obter_env("DB_HOST", "localhost")
    porta = obter_env("DB_PORT", porta_padrao)

    credenciais = quote(usuario, safe="")
    if senha != "":
        credenciais += ":" + quote(senha, safe="")

    return f"{driver}://{credenciais}@{host}:{porta}/{database_name}"


def resolver_config_sqlalchemy(db_kind, database_env, schema_names=None):
    database_name = obter_env(database_env)
    engine_url = _build_relational_url(db_kind, database_name)

    connect_args = {}
    if db_kind == "postgres":
        search_path = _search_path(schema_names)
        if search_path:
            connect_args["options"] = f"-c search_path={search_path}"

    return engine_url, connect_args


def _build_postgres_kwargs(database_name=None, schema_names=None):
    kwargs = {
        "dbname": database_name or "postgres",
        "user": obter_env("DB_USER", "postgres"),
        "password": obter_env("DB_PASSWORD", "33441065"),
        "host": obter_env("DB_HOST", "localhost"),
        "port": obter_env("DB_PORT", "5432"),
    }
    search_path = _search_path(schema_names)
    if search_path:
        kwargs["options"] = f"-c search_path={search_path}"
    return kwargs


def resolver_config_postgres(database_env, schema_names=None, purpose=None):
    db_env = obter_env("DB")
    if db_env is not None:
        db_kind = normalizar_tipo_db(db_env)
        if db_kind != "postgres":
            detalhe = f" para {purpose}" if purpose else ""
            raise SystemExit(
                f"ERRO: este script exige DB=postgres{detalhe} no ambiente/.env."
            )
    kwargs = _build_postgres_kwargs(obter_env(database_env), schema_names)
    return kwargs


def descrever_destino_postgres(database_name=None, schema_names=None):
    host = obter_env("DB_HOST", "localhost")
    dbname = database_name or "postgres"

    descricao = f"{host}/{dbname}"
    search_path = _search_path(schema_names)
    if search_path:
        descricao += f" schema={search_path}"
    return descricao
