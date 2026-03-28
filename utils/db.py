# -*- coding: utf-8 -*-
"""Funcoes compartilhadas de acesso ao PostgreSQL."""

import psycopg2

from utils.config import escapar_identificador_pg


def conectar_postgres(**kwargs):
    """Conecta ao PostgreSQL via psycopg2."""
    return psycopg2.connect(**kwargs)


def garantir_schema(cur, schema, escapar_fn=None):
    """Cria schema se nao existir. Pula se for 'public' ou vazio."""
    if not schema or schema.lower() == "public":
        return
    if escapar_fn is None:
        escapar_fn = escapar_identificador_pg
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {escapar_fn(schema)}")


def executar_sql(cur, sql, descricao, log_fn=None):
    """Executa SQL com logging opcional."""
    if log_fn:
        log_fn(descricao)
    cur.execute(sql)


def contar_tabela(cur, tabela):
    """Retorna count(*) de uma tabela."""
    cur.execute(f"SELECT count(*) FROM {tabela}")
    return int(cur.fetchone()[0])


class LeitorSanitizado:
    """Remove bytes NUL de um TextIOWrapper durante leitura (para COPY do PostgreSQL)."""

    def __init__(self, wrapped):
        self._wrapped = wrapped
        self.null_bytes_removed = 0

    def read(self, size=-1):
        chunk = self._wrapped.read(size)
        if not chunk:
            return chunk
        nulls = chunk.count("\x00")
        if nulls:
            self.null_bytes_removed += nulls
            chunk = chunk.replace("\x00", "")
        return chunk

    def readline(self, size=-1):
        line = self._wrapped.readline(size)
        if not line:
            return line
        nulls = line.count("\x00")
        if nulls:
            self.null_bytes_removed += nulls
            line = line.replace("\x00", "")
        return line
