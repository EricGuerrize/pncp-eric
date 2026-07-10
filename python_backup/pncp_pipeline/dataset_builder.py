import pandas as pd
import logging

logger = logging.getLogger(__name__)

def build_dataset(normalized_data: list) -> pd.DataFrame:
    """
    Consolidates the list of flat dictionaries into a pandas DataFrame.
    """
    logger.info("Construindo o dataset (DataFrame)...")
    if not normalized_data:
        logger.warning("Nenhum dado para construir o dataset. Retornando DataFrame vazio.")
        return pd.DataFrame()
        
    df = pd.DataFrame(normalized_data)
    logger.info(f"Dataset construído com formato: {df.shape}")
    return df

def clean_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the DataFrame according to specified rules.
    """
    logger.info("Limpando e filtrando os dados...")
    
    if df.empty:
        return df
        
    # Filter specific spheres if column exists
    if 'orgaoEntidade_esferaId' in df.columns:
        df = df[df['orgaoEntidade_esferaId'].isin(['M', 'E'])]
        
    # Columns to drop if they exist
    cols_to_drop = [
        'orgaoEntidade_poderId', 'orgaoEntidade_esferaId',
        'sequencialCompra', 'orgaoSubRogado', 'unidadeOrgao_ufNome',
        'unidadeOrgao_ufSigla', 'unidadeSubRogada',
        'srp', 'amparoLegal_codigo', 'amparoLegal_nome', 'amparoLegal_descricao',
        'dataEncerramentoProposta', 'informacaoComplementar', 'linkSistemaOrigem',
        'justificativaPresencial', 'dataAtualizacaoGlobal', 'linkProcessoEletronico',
        'modoDisputaId', 'fontesOrcamentarias',
        'tipoInstrumentoConvocatorioNome', 'tipoInstrumentoConvocatorioCodigo',
        'data_execucao', 'modalidade_codigo_consultada', 'modalidade_nome_consultada',
        'pagina_origem'
    ]
    
    existing_cols_to_drop = [col for col in cols_to_drop if col in df.columns]
    df = df.drop(columns=existing_cols_to_drop)
    
    # Columns to keep, reordering if possible
    cols_to_keep = [
        'dataInclusao', 'dataPublicacaoPncp', 'orgaoEntidade_cnpj', 'orgaoEntidade_razaoSocial',
        'unidadeOrgao_municipioNome', 'unidadeOrgao_nomeUnidade', 'unidadeOrgao_codigoUnidade',
        'unidadeOrgao_codigoIbge', 'usuarioNome',
        'numeroCompra', 'anoCompra', 'processo',
        'modalidadeId', 'modalidadeNome', 'modoDisputaNome',
        'situacaoCompraId', 'situacaoCompraNome',
        'objetoCompra', 'valorTotalEstimado', 'valorTotalHomologado',
        'numeroControlePNCP', 'dataAtualizacao'
    ]
    
    existing_cols_to_keep = [col for col in cols_to_keep if col in df.columns]
    
    # Add any columns that might be present in the df but weren't in the keep/drop lists 
    # to avoid data loss on unexpected API response changes, but ordered at the end.
    remaining_cols = [col for col in df.columns if col not in existing_cols_to_keep and col not in existing_cols_to_drop]
    
    df = df[existing_cols_to_keep + remaining_cols]
    
    logger.info(f"Dataset após limpeza apresenta formato: {df.shape}")
    
    return df
