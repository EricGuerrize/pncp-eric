"""
aplic_extractor.py — Extração APLIC do Oracle para múltiplos municípios.

Passos:
  1. Descoberta: encontra UG codes no Oracle para os municípios informados
  2. Extração: roda a SQL APLIC parametrizada pelos UG codes
  3. Enriquecimento: busca CNPJ no Excel PNCP mais recente (output/)
  4. Atualiza orgaos.json com as novas entidades
  5. Exporta CSVs por cidade em input/licitacao_{slug}_{ano}.csv

Uso:
    python aplic_extractor.py --cidades rondolandia acorizal jangada "lucas do rio verde" --ano 2026
    python aplic_extractor.py --dry-run --cidades "lucas do rio verde"  # só mostra UGs descobertos
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import unicodedata
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent))
from oracle_connector import extrair_dados_oracle

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

INPUT_DIR  = Path(__file__).parent / "input"
OUTPUT_DIR = Path(__file__).parent / "output"
ORGAOS_JSON = INPUT_DIR / "orgaos.json"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalizar(texto: str) -> str:
    """Remove acentos e coloca em maiúsculo para comparação."""
    nfkd = unicodedata.normalize("NFKD", str(texto))
    return nfkd.encode("ASCII", "ignore").decode("ASCII").upper().strip()


def _slug(nome: str) -> str:
    """'Lucas do Rio Verde' → 'lucas_do_rio_verde'"""
    return _normalizar(nome).lower().replace(" ", "_")


# ---------------------------------------------------------------------------
# Passo 1: Descoberta de UG codes no Oracle
# ---------------------------------------------------------------------------

SQL_DISCOVERY = """
    SELECT DISTINCT
        TRIM(TO_CHAR(E.CNPJ_CPF_COD_TCE_ENTIDADE)) AS ug_code,
        E.NOME_entidade AS nome,
        E.CNPJ_DIREITO_PUBLICO AS cnpj_publico,
        MN.MUN_NOME AS municipio
    FROM aplic2008.ENTIDADE@conectprod E
    JOIN publico.MUNICIPIO@conectprod MN ON MN.MUN_CODIGO = E.MUN_CODIGO
    WHERE TRANSLATE(UPPER(MN.MUN_NOME),
          'ÁÀÃÂÄÉÈÊËÍÌÎÏÓÒÕÔÖÚÙÛÜÇ',
          'AAAAAEEEEIIIIOOOOOOUUUUC') LIKE :busca
    ORDER BY MN.MUN_NOME, E.NOME_entidade
"""


def descobrir_ugs(cidades: list[str]) -> pd.DataFrame:
    """
    Consulta Oracle e retorna DataFrame com colunas:
      ug_code | nome | cnpj_publico | municipio
    """
    logger.info(f"Descobrindo UG codes para: {cidades}")
    blocos: list[pd.DataFrame] = []

    for cidade in cidades:
        busca = f"%{_normalizar(cidade)}%"
        df = extrair_dados_oracle(SQL_DISCOVERY, params={"busca": busca})
        if df.empty:
            logger.warning(f"  Nenhuma entidade encontrada para: '{cidade}'")
        else:
            logger.info(f"  {cidade}: {len(df)} entidade(s) encontrada(s)")
            blocos.append(df)

    if not blocos:
        return pd.DataFrame()

    resultado = pd.concat(blocos, ignore_index=True)
    resultado.columns = resultado.columns.str.lower()
    resultado = resultado.drop_duplicates(subset=["ug_code"])
    return resultado


# ---------------------------------------------------------------------------
# Passo 2: Extração APLIC parametrizada
# ---------------------------------------------------------------------------

def _build_aplic_sql(ugs: list[str], ano: int) -> str:
    """
    Monta a SQL de extração APLIC com UG codes e ano parametrizados.
    UG codes são validados como numéricos antes de serem interpolados.
    """
    for ug in ugs:
        if not re.fullmatch(r"\d+", str(ug).strip()):
            raise ValueError(f"UG code inválido (não-numérico): '{ug}'")

    ugs_str  = ", ".join(f"'{u}'" for u in ugs)
    ano_str  = str(int(ano))  # valida que é inteiro

    return f"""
    SELECT *
    FROM (
        -- Parte 1: Licitações regulares (sem adesão a ARP)
        SELECT DISTINCT
               P.ENT_CODIGO        AS "Cód. UG",
               VW.NOME_entidade    AS "UG",
               VW.MUN_CODIGO       AS "Cód. município",
               MN.MUN_NOME         AS "Município",
               P.PLIC_NUMERO       AS "Nº Licitação",
               PA.PLIC_DATA        AS "Data Abertura",
               P.MLIC_CODIGO       AS "Cod. Modalidade",
               M.MLIC_DESCRICAO    AS "Modalidade",
               P.CG_IDENTIFICACAO  AS "Adesão à Licitação do Orgão",
               P.EXERCICIO         AS "Exercício",
               C.CG_NOME           AS "Nome Adesão",
               PA.PLIC_TIPO        AS "Cod.Tipo",
               DECODE(PA.PLIC_TIPO,
                      '1','Preço','2','Técnica','3','Técnica e Preço',
                      '4','Tarifa','5','Tarifa e técnica',
                      '6','Contraprestação','7','Contraprestação e técnica') AS "Tipo",
               PA.PLIC_DATALIMENTRPROPOSTA          AS "Data Limite",
               PA.PLIC_REGISTROPRECO                AS "Registro de Preço",
               PA.PLIC_NOMERESPJURIDICO             AS "Responsável Jurídico",
               PA.PLIC_NUMOAB                       AS "Nº OAB",
               PA.PLIC_VALORESTIMADO                AS "Valor Estimado",
               PA.PLIC_VALORCUSTOCOPIA              AS "Custo Cópia Edital",
               PA.PLIC_OBJETO                       AS "Objetivo",
               PA.PLIC_MOTIVO                       AS "Motivo",
               PA.CMPLIC_NUMPORTARIA                AS "Nº Portaria",
               P.AUTORIZADO_REENVIO                 AS "Reenvio",
               CASE WHEN PA.PLIC_LOTEITEM = '1' THEN 'Lote' ELSE 'Item' END AS "Lote/Item",
               (SELECT COUNT(1)
                FROM aplic2008.EMPENHO@conectprod EMP
                WHERE P.ENT_CODIGO = EMP.ENT_CODIGO
                  AND P.PLIC_NUMERO = EMP.PLIC_NUMERO
                  AND P.MLIC_CODIGO = EMP.MLIC_CODIGO) AS "Empenho(s)",
               NULL AS "Valor Vencedor",
               NULL AS "Cod. Situação",
               NULL AS "Situação",
               NULL AS "Data Situação",
               NULL AS "Data Adjudicação",
               NULL AS "Data Julgamento Proposta",
               (SELECT COUNT(1)
                FROM aplic2008.PROCESSO_LICITATORIO_DOTACAO@conectprod PD
                WHERE PD.ENT_CODIGO = P.ENT_CODIGO AND PD.EXERCICIO = P.EXERCICIO
                  AND PD.PLIC_NUMERO = P.PLIC_NUMERO AND PD.MLIC_CODIGO = P.MLIC_CODIGO) AS "Qtde.Dotação",
               P.PLIC_NOMEARQPDF                        AS "Arq.Processo Carona",
               PA.PLIC_DATAABERTURASESSAOPUBLICA        AS "Data abert. sessão públ.",
               P.PLIC_NUMLICITACAO                      AS "Nº licitação(Reg. de preço)",
               P.PLIC_MODALIDADE                        AS "Cód. Modalidade(Reg. de preço)",
               NULL                                     AS "Modalidade(Reg. de preço)",
               P.PLIC_NUMATA                            AS "Nº Ata reg. de preço",
               P.PLIC_NUMPMIMPI                         AS "Nº PMI/MPI",
               DECODE((SELECT COUNT(1) FROM aplic2008.PROC_LICIT_ATA_REGISTRO_PRECO@conectprod ARP
                       WHERE ARP.ENT_CODIGO = P.ENT_CODIGO AND ARP.PLIC_NUMERO = P.PLIC_NUMERO
                         AND ARP.MLIC_CODIGO = P.MLIC_CODIGO), 0, 'NÃO', 'SIM') AS "Possui ARP?",
               P.RGENV_DATAENVIO    AS "Recebido em...",
               PLM.PLM_DESCRICAO    AS "Para micro empresa?"
        FROM aplic2008.PROCESSO_LICITATORIO@conectprod P
        INNER JOIN aplic2008.MODALIDADE_LICITACAO@conectprod M ON M.MLIC_CODIGO = P.MLIC_CODIGO
        LEFT  JOIN aplic2008.CADASTRO_GERAL@conectprod C
               ON P.ENT_CODIGO = C.ENT_CODIGO AND C.EXERCICIO >= 2015
              AND P.CG_IDENTIFICACAO = C.CG_IDENTIFICACAO
        INNER JOIN aplic2008.PROC_LICIT_ABERTURA_RETIFIC@conectprod PA
               ON P.ENT_CODIGO = PA.ENT_CODIGO AND P.EXERCICIO = PA.EXERCICIO
              AND P.PLIC_NUMERO = PA.PLIC_NUMERO AND P.MLIC_CODIGO = PA.MLIC_CODIGO
              AND PA.PLIC_SITUACAO = 1
        LEFT  JOIN aplic2008.PROCESSO_LICITATORIO_DOTACAO@conectprod D
               ON P.MLIC_CODIGO = D.MLIC_CODIGO AND P.PLIC_NUMERO = D.PLIC_NUMERO
              AND P.EXERCICIO = D.EXERCICIO AND P.ENT_CODIGO = D.ENT_CODIGO
        INNER JOIN aplic2008.ENTIDADE@conectprod VW ON P.ENT_CODIGO = VW.CNPJ_CPF_COD_TCE_ENTIDADE
        INNER JOIN publico.MUNICIPIO@conectprod MN ON MN.MUN_CODIGO = VW.MUN_CODIGO
        INNER JOIN aplic2008.PROC_LICIT_MICROEMPRESA@conectprod PLM ON PLM.PLM_CODIGO = PA.PLIC_PARAMICROEMPRESA
        WHERE P.ENT_CODIGO IN ({ugs_str})
          AND SUBSTR(P.PLIC_NUMERO, 13, 4) IN ('{ano_str}')
          AND P.MLIC_CODIGO NOT IN ('17', '22', '23', '25')

        UNION

        -- Parte 2: Adesões a Ata de Registro de Preços
        SELECT DISTINCT
               P.ENT_CODIGO        AS "Cód. UG",
               VW.NOME_entidade    AS "UG",
               VW.MUN_CODIGO       AS "Cód. município",
               MN.MUN_NOME         AS "Município",
               P.PLIC_NUMERO       AS "Nº Licitação",
               NULL                AS "Data Abertura",
               P.MLIC_CODIGO       AS "Cod. Modalidade",
               M.MLIC_DESCRICAO    AS "Modalidade",
               P.CG_IDENTIFICACAO  AS "Adesão à Licitação do Orgão",
               P.EXERCICIO         AS "Exercício",
               C.CG_NOME           AS "Nome Adesão",
               NULL AS "Cod.Tipo",
               NULL AS "Tipo",
               NULL AS "Data Limite",
               NULL AS "Registro de Preço",
               NULL AS "Responsável Jurídico",
               NULL AS "Nº OAB",
               NULL AS "Valor Estimado",
               NULL AS "Custo Cópia Edital",
               NULL AS "Objetivo",
               NULL AS "Motivo",
               NULL AS "Nº Portaria",
               P.AUTORIZADO_REENVIO AS "Reenvio",
               NULL AS "Lote/Item",
               NULL AS "Empenho(s)",
               NULL AS "Valor Vencedor",
               NULL AS "Cod. Situação",
               NULL AS "Situação",
               NULL AS "Data Situação",
               NULL AS "Data Adjudicação",
               NULL AS "Data Julgamento Proposta",
               NULL AS "Qtde.Dotação",
               P.PLIC_NOMEARQPDF AS "Arq.Processo Carona",
               NULL              AS "Data abert. sessão públ.",
               P.PLIC_NUMLICITACAO AS "Nº licitação(Reg. de preço)",
               P.PLIC_MODALIDADE   AS "Cód. Modalidade(Reg. de preço)",
               ML.MLIC_DESCRICAO   AS "Modalidade(Reg. de preço)",
               P.PLIC_NUMATA       AS "Nº Ata reg. de preço",
               P.PLIC_NUMPMIMPI    AS "Nº PMI/MPI",
               DECODE((SELECT COUNT(1) FROM aplic2008.PROC_LICIT_ATA_REGISTRO_PRECO@conectprod ARP
                       WHERE ARP.ENT_CODIGO = P.ENT_CODIGO AND ARP.PLIC_NUMERO = P.PLIC_NUMERO
                         AND ARP.MLIC_CODIGO = P.MLIC_CODIGO), 0, 'NÃO', 'SIM') AS "Possui ARP?",
               P.RGENV_DATAENVIO   AS "Recebido em...",
               NULL                AS "Para micro empresa?"
        FROM aplic2008.PROCESSO_LICITATORIO@conectprod P
        INNER JOIN aplic2008.MODALIDADE_LICITACAO@conectprod M ON M.MLIC_CODIGO = P.MLIC_CODIGO
        LEFT  JOIN aplic2008.MODALIDADE_LICITACAO@conectprod ML ON ML.MLIC_CODIGO = P.PLIC_MODALIDADE
        LEFT  JOIN aplic2008.CADASTRO_GERAL@conectprod C
               ON P.ENT_CODIGO = C.ENT_CODIGO AND C.EXERCICIO >= 2015
              AND P.CG_IDENTIFICACAO = C.CG_IDENTIFICACAO
        INNER JOIN aplic2008.ENTIDADE@conectprod VW ON P.ENT_CODIGO = VW.CNPJ_CPF_COD_TCE_ENTIDADE
        INNER JOIN publico.MUNICIPIO@conectprod MN ON MN.MUN_CODIGO = VW.MUN_CODIGO
        WHERE P.ENT_CODIGO IN ({ugs_str})
          AND SUBSTR(P.PLIC_NUMERO, 13, 4) IN ('{ano_str}')
          AND P.MLIC_CODIGO IN ('17', '22', '23', '25')
    )
    ORDER BY "Município", "UG", "Nº Licitação"
    """


def extrair_aplic(ugs: list[str], ano: int) -> pd.DataFrame:
    """Executa extração APLIC no Oracle para os UG codes e ano informados."""
    if not ugs:
        logger.warning("Nenhum UG code informado para extração.")
        return pd.DataFrame()

    sql = _build_aplic_sql(ugs, ano)
    logger.info(f"Extraindo APLIC: {len(ugs)} UG(s), ano {ano}")
    return extrair_dados_oracle(sql)


# ---------------------------------------------------------------------------
# Passo 3: Enriquecimento de CNPJ via Excel PNCP
# ---------------------------------------------------------------------------

def enriquecer_cnpj_do_pncp(df_ugs: pd.DataFrame) -> pd.DataFrame:
    """
    Se o Oracle não retornou CNPJ em cnpj_publico, tenta encontrar
    nos arquivos PNCP Excel de output/ buscando por município.
    """
    df = df_ugs.copy()

    # Já temos CNPJ do Oracle?
    cnpj_col = "cnpj_publico" if "cnpj_publico" in df.columns else "cnpj"
    if cnpj_col in df.columns:
        df["cnpj"] = df[cnpj_col].fillna("").astype(str).apply(lambda x: re.sub(r"\D", "", x))
        tem_cnpj = df["cnpj"].str.len() >= 14
        if tem_cnpj.all():
            logger.info("CNPJs obtidos direto do Oracle.")
            return df

    # Busca no PNCP Excel mais recente
    pncp_files = sorted(OUTPUT_DIR.glob("pncp_contratacoes_MT_*.xlsx"), reverse=True)
    if not pncp_files:
        logger.warning("Nenhum Excel PNCP encontrado em output/. CNPJ será vazio.")
        df["cnpj"] = df.get("cnpj", "")
        return df

    pncp_path = pncp_files[0]
    logger.info(f"Buscando CNPJs complementares no PNCP: {pncp_path.name}")

    try:
        df_pncp = pd.read_excel(pncp_path, dtype=str)

        mun_col  = next((c for c in df_pncp.columns if "municipioNome" in c), None)
        cnpj_src = next((c for c in df_pncp.columns if "cnpj" in c.lower() and "orgao" in c.lower()), None)

        if mun_col and cnpj_src:
            cnpj_map: dict[str, str] = {}
            for _, row in df_pncp[[mun_col, cnpj_src]].dropna().iterrows():
                mun_norm = _normalizar(str(row[mun_col]))
                cnpj_val = re.sub(r"\D", "", str(row[cnpj_src]))
                if len(cnpj_val) >= 14:
                    cnpj_map[mun_norm] = cnpj_val

            def _lookup_cnpj(row):
                if "cnpj" in row and len(str(row.get("cnpj", "")).strip()) >= 14:
                    return row["cnpj"]
                mun_norm = _normalizar(str(row.get("municipio", "")))
                return cnpj_map.get(mun_norm, "")

            df["cnpj"] = df.apply(_lookup_cnpj, axis=1)
    except Exception as e:
        logger.warning(f"Erro ao buscar CNPJs no Excel PNCP: {e}")
        if "cnpj" not in df.columns:
            df["cnpj"] = ""

    return df


# ---------------------------------------------------------------------------
# Passo 4: Atualizar orgaos.json
# ---------------------------------------------------------------------------

def atualizar_orgaos_json(df_ugs: pd.DataFrame) -> None:
    """Faz merge das novas entidades em orgaos.json sem duplicar."""
    if ORGAOS_JSON.exists():
        with open(ORGAOS_JSON, encoding="utf-8") as f:
            orgaos: list[dict] = json.load(f)
    else:
        orgaos = []

    ugs_existentes = {o["ug"] for o in orgaos}
    adicionados = 0

    for _, row in df_ugs.iterrows():
        ug = str(row.get("ug_code", "")).strip()
        if not ug or ug in ugs_existentes:
            continue

        municipio = str(row.get("municipio", "")).strip().lower()
        cnpj      = re.sub(r"\D", "", str(row.get("cnpj", "") or ""))
        nome      = str(row.get("nome", "")).strip()

        orgaos.append({"ug": ug, "municipio": municipio, "cnpj": cnpj, "nome": nome})
        ugs_existentes.add(ug)
        adicionados += 1
        logger.info(f"  + orgaos.json: {nome} [{ug}] — {municipio} (cnpj: {cnpj or 'não encontrado'})")

    with open(ORGAOS_JSON, "w", encoding="utf-8") as f:
        json.dump(orgaos, f, ensure_ascii=False, indent=2)

    logger.info(f"orgaos.json: {adicionados} nova(s) entidade(s) adicionada(s) ({len(orgaos)} total)")


# ---------------------------------------------------------------------------
# Passo 5: Exportar CSVs por cidade
# ---------------------------------------------------------------------------

def exportar_csvs(df_aplic: pd.DataFrame, ano: int) -> list[Path]:
    """Salva input/licitacao_{slug}_{ano}.csv para cada município no DataFrame."""
    if df_aplic.empty:
        logger.warning("Extração APLIC vazia — nenhum CSV exportado.")
        return []

    INPUT_DIR.mkdir(exist_ok=True)
    saidas: list[Path] = []

    mun_col = next(
        (c for c in df_aplic.columns if c in ("Município", "municipio", "MUNICIPIO")),
        None
    )

    if mun_col is None:
        # Sem coluna de município: exporta tudo junto
        out = INPUT_DIR / f"licitacao_aplic_{ano}.csv"
        df_aplic.to_csv(out, index=False, encoding="utf-8-sig")
        logger.info(f"  Exportado: {out.name} ({len(df_aplic)} registros)")
        saidas.append(out)
        return saidas

    for municipio, grupo in df_aplic.groupby(mun_col):
        slug = _slug(str(municipio))
        out  = INPUT_DIR / f"licitacao_{slug}_{ano}.csv"
        grupo.to_csv(out, index=False, encoding="utf-8-sig")
        logger.info(f"  Exportado: {out.name} ({len(grupo)} registros)")
        saidas.append(out)

    return saidas


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def run(cidades: list[str], ano: int, dry_run: bool = False) -> list[Path]:
    """
    Executa o pipeline completo de extração APLIC.
    Retorna lista de CSVs gerados.
    """
    # 1. Descoberta
    df_ugs = descobrir_ugs(cidades)
    if df_ugs.empty:
        logger.error("Nenhum UG code encontrado. Verifique os nomes dos municípios.")
        return []

    print("\n=== UGs descobertos ===")
    print(df_ugs[["ug_code", "nome", "municipio"]].to_string(index=False))
    print()

    if dry_run:
        logger.info("--dry-run: etapas de extração e exportação puladas.")
        return []

    # 2. Enriquecimento de CNPJ
    df_ugs = enriquecer_cnpj_do_pncp(df_ugs)

    # 3. Atualiza orgaos.json
    atualizar_orgaos_json(df_ugs)

    # 4. Extração APLIC
    ugs = df_ugs["ug_code"].tolist()
    df_aplic = extrair_aplic(ugs, ano)

    if df_aplic.empty:
        logger.warning("Extração APLIC retornou vazia. Verifique conexão Oracle e UG codes.")
        return []

    print(f"\n=== Extração APLIC: {len(df_aplic)} registros ===")

    # 5. Exporta CSVs
    csvs = exportar_csvs(df_aplic, ano)

    print(f"\n=== Concluído: {len(csvs)} CSV(s) gerado(s) em {INPUT_DIR} ===")
    return csvs


def main():
    parser = argparse.ArgumentParser(
        description="Extrai dados APLIC do Oracle para municípios informados."
    )
    parser.add_argument(
        "--cidades",
        nargs="+",
        required=True,
        help="Nomes dos municípios (ex: rondolandia acorizal jangada 'lucas do rio verde')",
    )
    parser.add_argument(
        "--ano",
        type=int,
        default=2026,
        help="Exercício/ano a extrair (padrão: 2026)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Apenas descobre UG codes, sem extrair dados nem exportar CSVs",
    )

    args = parser.parse_args()
    csvs = run(args.cidades, args.ano, dry_run=args.dry_run)
    sys.exit(0 if csvs or args.dry_run else 1)


if __name__ == "__main__":
    main()
