SELECT *                                                                                                                                
                                                                                                                                         
  FROM (          
                                                                                                                                         
      SELECT DISTINCT

             P.ENT_CODIGO AS "Cód. UG",                                                                                                  
   
             VW.NOME_entidade AS "UG",                                                                                                    
                 
             VW.MUN_CODIGO AS "Cód. município",                                                                                          
                 
             MN.MUN_NOME AS "Município",                                                                                                  
                 
             P.PLIC_NUMERO AS "Nº Licitação",                                                                                            
                 
             PA.PLIC_DATA AS "Data Abertura",                                                                                            
                 
             P.MLIC_CODIGO AS "Cod. Modalidade",                                                                                          
                 
             M.MLIC_DESCRICAO AS "Modalidade",

             P.CG_IDENTIFICACAO AS "Adesão à Licitação do Orgão",                                                                        
   
             P.EXERCICIO AS "Exercício",                                                                                                  
                 
             C.CG_NOME AS "Nome Adesão",                                                                                                  
                 
             PA.PLIC_TIPO AS "Cod.Tipo",                                                                                                  
   
             DECODE(PA.PLIC_TIPO,                                                                                                        
                    '1', 'Preço',
                    '2', 'Técnica',                                                                                                      
                    '3', 'Técnica e Preço',
                    '4', 'Tarifa',                                                                                                        
                    '5', 'Tarifa e técnica',                                                                                              
                    '6', 'Contraprestação',                                                                                              
                    '7', 'Contraprestação e técnica') AS "Tipo",                                                                          
                                                                                                                                         
             PA.PLIC_DATALIMENTRPROPOSTA AS "Data Limite",                                                                                
                                                                                                                                         
             PA.PLIC_REGISTROPRECO AS "Registro de Preço",                                                                                
                 
             PA.PLIC_NOMERESPJURIDICO AS "Responsável Jurídico",                                                                          
                 
             PA.PLIC_NUMOAB AS "Nº OAB",                                                                                                  
                 
             PA.PLIC_VALORESTIMADO AS "Valor Estimado",

             PA.PLIC_VALORCUSTOCOPIA AS "Custo Cópia Edital",                                                                            
   
             PA.PLIC_OBJETO AS "Objetivo",                                                                                                
                 
             PA.PLIC_MOTIVO AS "Motivo",                                                                                                  
   
             PA.CMPLIC_NUMPORTARIA AS "Nº Portaria",                                                                                      
                 
             P.AUTORIZADO_REENVIO AS "Reenvio",                                                                                          
                 
             CASE                                                                                                                        
                 WHEN PA.PLIC_LOTEITEM = '1' THEN 'Lote'
                 ELSE 'Item'                                                                                                              
             END AS "Lote/Item",
                                                                                                                                         
             (                                                                                                                            
                 SELECT COUNT(1)
                 FROM aplic2008.EMPENHO@conectprod EMP                                                                                    
                 WHERE P.ENT_CODIGO = EMP.ENT_CODIGO                                                                                      
                   AND P.PLIC_NUMERO = EMP.PLIC_NUMERO                                                                                    
                   AND P.MLIC_CODIGO = EMP.MLIC_CODIGO                                                                                    
             ) AS "Empenho(s)",                                                                                                          
                 
             CASE                                                                                                                        
                 WHEN PA.PLIC_LOTEITEM = '2' THEN
                     (                                                                                                                    
                         SELECT SUM(PLIT.PCPLIC_VALORCOTADO)
                         FROM aplic2008.PARTICIPACAO_PROC_LICIT_ITEM@conectprod PLIT                                                      
                         WHERE PLIT.ENT_CODIGO = P.ENT_CODIGO                                                                            
                           AND PLIT.EXERCICIO = P.EXERCICIO                                                                              
                           AND PLIT.PLIC_NUMERO = P.PLIC_NUMERO                                                                          
                           AND PLIT.MLIC_CODIGO = P.MLIC_CODIGO                                                                          
                           AND PLIT.PCPLIC_VENCEDOR = 'S'                                                                                
                           AND PLIT.PLIC_SITUACAOPLS = (                                                                                  
                               SELECT MAX(PLITA.PLIC_SITUACAOPLS)                                                                        
                               FROM aplic2008.PARTICIPACAO_PROC_LICIT_ITEM@conectprod PLITA                                              
                               WHERE PLITA.ENT_CODIGO = P.ENT_CODIGO                                                                      
                                 AND PLITA.EXERCICIO = P.EXERCICIO                                                                        
                                 AND PLITA.PLIC_NUMERO = P.PLIC_NUMERO                                                                    
                                 AND PLITA.MLIC_CODIGO = P.MLIC_CODIGO                                                                    
                                 AND PLITA.PLIC_SITUACAOPLS IN (5, 6)
                                 AND PLITA.PCPLIC_VENCEDOR = 'S'                                                                          
                           )                                                                                                              
                           AND PLIT.PLIC_DATASITUACAOPLS = (                                                                              
                               SELECT MAX(PLITA.PLIC_DATASITUACAOPLS)                                                                    
                               FROM aplic2008.PARTICIPACAO_PROC_LICIT_ITEM@conectprod PLITA                                              
                               WHERE PLITA.ENT_CODIGO = P.ENT_CODIGO                                                                      
                                 AND PLITA.EXERCICIO = P.EXERCICIO                                                                        
                                 AND PLITA.PLIC_NUMERO = P.PLIC_NUMERO                                                                    
                                 AND PLITA.MLIC_CODIGO = P.MLIC_CODIGO                                                                    
                                 AND PLITA.PLIC_SITUACAOPLS IN (5, 6)                                                                    
                                 AND PLITA.PCPLIC_VENCEDOR = 'S'                                                                          
                           )                                                                                                              
                     )                                                                                                                    
                 ELSE                                                                                                                    
                     (
                         SELECT SUM(PLOT.PCPLICLOTE_VALORCOTADO)                                                                          
                         FROM aplic2008.PARTICIPACAO_PROC_LICIT_LOTE@conectprod PLOT                                                      
                         WHERE PLOT.ENT_CODIGO = P.ENT_CODIGO                                                                            
                           AND PLOT.EXERCICIO = P.EXERCICIO                                                                              
                           AND PLOT.PLIC_NUMERO = P.PLIC_NUMERO                                                                          
                           AND PLOT.MLIC_CODIGO = P.MLIC_CODIGO                                                                          
                           AND PLOT.PCPLICLOTE_VENCEDOR = 'S'                                                                            
                           AND PLOT.PLIC_SITUACAOPLS = (                                                                                  
                               SELECT MAX(PLOTA.PLIC_SITUACAOPLS)                                                                        
                               FROM aplic2008.PARTICIPACAO_PROC_LICIT_LOTE@conectprod PLOTA                                              
                               WHERE PLOTA.ENT_CODIGO = P.ENT_CODIGO                                                                      
                                 AND PLOTA.EXERCICIO = P.EXERCICIO                                                                        
                                 AND PLOTA.PLIC_NUMERO = P.PLIC_NUMERO                                                                    
                                 AND PLOTA.MLIC_CODIGO = P.MLIC_CODIGO                                                                    
                                 AND PLOTA.PLIC_SITUACAOPLS IN (5, 6)                                                                    
                                 AND PLOTA.PCPLICLOTE_VENCEDOR = 'S'                                                                      
                           )                                                                                                              
                           AND PLOT.PLIC_DATASITUACAOPLS = (                                                                              
                               SELECT MAX(PLOTA.PLIC_DATASITUACAOPLS)                                                                    
                               FROM aplic2008.PARTICIPACAO_PROC_LICIT_LOTE@conectprod PLOTA                                              
                               WHERE PLOTA.ENT_CODIGO = P.ENT_CODIGO                                                                      
                                 AND PLOTA.EXERCICIO = P.EXERCICIO                                                                        
                                 AND PLOTA.PLIC_NUMERO = P.PLIC_NUMERO
                                 AND PLOTA.MLIC_CODIGO = P.MLIC_CODIGO                                                                    
                                 AND PLOTA.PLIC_SITUACAOPLS IN (5, 6)
                                 AND PLOTA.PCPLICLOTE_VENCEDOR = 'S'                                                                      
                           )                                                                                                              
                     )                                                                                                                    
             END AS "Valor Vencedor",                                                                                                    
                 
             CAST('' AS INTEGER) AS "Cod. Situação",                                                                                      
             CAST('' AS VARCHAR2(50)) AS "Situação",
             CAST('' AS DATE) AS "Data Situação",                                                                                        
             CAST('' AS DATE) AS "Data Adjudicação",                                                                                      
             CAST('' AS DATE) AS "Data Julgamento Proposta",                                                                              
                                                                                                                                         
             (                                                                                                                            
                 SELECT COUNT(1)                                                                                                          
                 FROM aplic2008.PROCESSO_LICITATORIO_DOTACAO@conectprod PD                                                                
                 WHERE PD.ENT_CODIGO = P.ENT_CODIGO                                                                                      
                   AND PD.EXERCICIO = P.EXERCICIO                                                                                        
                   AND PD.PLIC_NUMERO = P.PLIC_NUMERO                                                                                    
                   AND PD.MLIC_CODIGO = P.MLIC_CODIGO                                                                                    
             ) AS "Qtde.Dotação",                                                                                                        
                                                                                                                                         
             P.PLIC_NOMEARQPDF AS "Arq.Processo Carona",                                                                                  
                 
             PA.PLIC_DATAABERTURASESSAOPUBLICA AS "Data abert. sessão públ.",                                                            
                 
             P.PLIC_NUMLICITACAO AS "Nº licitação(Reg. de preço)",                                                                        
                 
             P.PLIC_MODALIDADE AS "Cód. Modalidade(Reg. de preço)",                                                                      
                 
             CAST('' AS VARCHAR2(100)) AS "Modalidade(Reg. de preço)",                                                                    
                 
             P.PLIC_NUMATA AS "Nº Ata reg. de preço",                                                                                    
                 
             P.PLIC_NUMPMIMPI AS "Nº PMI/MPI",                                                                                            
                 
             DECODE((                                                                                                                    
                 SELECT COUNT(1)
                 FROM aplic2008.PROC_LICIT_ATA_REGISTRO_PRECO@conectprod ARP                                                              
                 WHERE ARP.ENT_CODIGO = P.ENT_CODIGO                                                                                      
                   AND ARP.PLIC_NUMERO = P.PLIC_NUMERO                                                                                    
                   AND ARP.MLIC_CODIGO = P.MLIC_CODIGO                                                                                    
             ), 0, 'NÃO', 'SIM') AS "Possui ARP?",                                                                                        
                                                                                                                                         
             P.RGENV_DATAENVIO AS "Recebido em...",                                                                                      
                                                                                                                                         
             PLM.PLM_DESCRICAO AS "Para micro empresa?"                                                                                  
                 
      FROM aplic2008.PROCESSO_LICITATORIO@conectprod P                                                                                    
                 
      INNER JOIN aplic2008.MODALIDADE_LICITACAO@conectprod M                                                                              
          ON M.MLIC_CODIGO = P.MLIC_CODIGO
                                                                                                                                         
      LEFT JOIN aplic2008.CADASTRO_GERAL@conectprod C                                                                                    
          ON P.ENT_CODIGO = C.ENT_CODIGO                                                                                                  
         AND C.EXERCICIO >= 2015                                                                                                          
         AND P.CG_IDENTIFICACAO = C.CG_IDENTIFICACAO                                                                                      
                                                                                                                                         
      INNER JOIN aplic2008.PROC_LICIT_ABERTURA_RETIFIC@conectprod PA                                                                      
          ON P.ENT_CODIGO = PA.ENT_CODIGO                                                                                                
         AND P.EXERCICIO = PA.EXERCICIO                                                                                                  
         AND P.PLIC_NUMERO = PA.PLIC_NUMERO                                                                                              
         AND P.MLIC_CODIGO = PA.MLIC_CODIGO                                                                                              
         AND PA.PLIC_SITUACAO = 1                                                                                                        
                                                                                                                                         
      LEFT JOIN aplic2008.PROCESSO_LICITATORIO_DOTACAO@conectprod D                                                                      
          ON P.MLIC_CODIGO = D.MLIC_CODIGO                                                                                                
         AND P.PLIC_NUMERO = D.PLIC_NUMERO                                                                                                
         AND P.EXERCICIO = D.EXERCICIO                                                                                                    
         AND P.ENT_CODIGO = D.ENT_CODIGO                                                                                                  
                                                                                                                                         
      INNER JOIN aplic2008.ENTIDADE@conectprod VW                                                                                        
          ON P.ENT_CODIGO = VW.CNPJ_CPF_COD_TCE_ENTIDADE                                                                                  
                                                                                                                                         
      INNER JOIN publico.MUNICIPIO@conectprod MN                                                                                          
          ON MN.MUN_CODIGO = VW.MUN_CODIGO                                                                                                
                                                                                                                                         
      INNER JOIN aplic2008.PROC_LICIT_MICROEMPRESA@conectprod PLM                                                                        
          ON PLM.PLM_CODIGO = PA.PLIC_PARAMICROEMPRESA                                                                                    
                                                                                                                                         
      WHERE P.ENT_CODIGO IN ('1159326', '1113257', '1118736', '1112309')  -- << ALTERADO                                                  
        AND SUBSTR(P.PLIC_NUMERO, 13, 4) IN ('2026')                                                                                      
        AND P.MLIC_CODIGO NOT IN ('17', '22', '23', '25')                                                                                
                                                                                                                                         
      UNION                                                                                                                              
                                                                                                                                         
      SELECT DISTINCT                                                                                                                    
                 
             P.ENT_CODIGO AS "Cód. UG",                                                                                                  
                 
             VW.NOME_entidade AS "UG",                                                                                                    
                 
             VW.MUN_CODIGO AS "Cód. município",                                                                                          
                 
             MN.MUN_NOME AS "Município",                                                                                                  
                 
             P.PLIC_NUMERO AS "Nº Licitação",                                                                                            
                 
             CAST('' AS DATE) AS "Data Abertura",                                                                                        
                 
             P.MLIC_CODIGO AS "Cod. Modalidade",                                                                                          
                 
             M.MLIC_DESCRICAO AS "Modalidade",                                                                                            
   
             P.CG_IDENTIFICACAO AS "Adesão à Licitação do Orgão",                                                                        
                 
             P.EXERCICIO AS "Exercício",                                                                                                  
                 
             C.CG_NOME AS "Nome Adesão",                                                                                                  
                 
             '' AS "Cod.Tipo",                                                                                                            
             '' AS "Tipo",
                                                                                                                                         
             CAST('' AS DATE) AS "Data Limite",                                                                                          
                                                                                                                                         
             '' AS "Registro de Preço",                                                                                                  
             '' AS "Responsável Jurídico",
             '' AS "Nº OAB",                                                                                                              
                                                                                                                                         
             CAST('' AS INTEGER) AS "Valor Estimado",                                                                                    
             CAST('' AS INTEGER) AS "Custo Cópia Edital",                                                                                
                                                                                                                                         
             '' AS "Objetivo",                                                                                                            
             '' AS "Motivo",                                                                                                              
             '' AS "Nº Portaria",                                                                                                        
                 
             P.AUTORIZADO_REENVIO AS "Reenvio",                                                                                          
                 
             '' AS "Lote/Item",                                                                                                          
                 
             CAST('' AS INTEGER) AS "Empenho(s)",                                                                                        
                 
             (                                                                                                                            
                 SELECT DECODE(
                            (                                                                                                            
                                SELECT COUNT(1)                                                                                          
                                FROM aplic2008.PARTICIP_ATA_REG_PRECO_ITEM@conectprod A                                                  
                                WHERE A.PLIC_NUMERO = P.PLIC_NUMLICITACAO                                                                
                                  AND A.MLIC_CODIGO = P.PLIC_MODALIDADE                                                                  
                                  AND A.PLICATA_NUMATA = P.PLIC_NUMATA                                                                    
                                  AND A.PLICATAI_TIPOPARTICIPACAO IN (2, 3)                                                              
                                  AND REPLACE(REPLACE(REPLACE(A.PLICATAI_CNPJPARTICIPANTE, '.', ''), '-', ''), '/', '') =                
                                      (                                                                                                  
                                          SELECT E.CNPJ_DIREITO_PUBLICO                                                                  
                                          FROM aplic2008.ENTIDADE@conectprod E                                                            
                                          WHERE E.CNPJ_CPF_COD_TCE_ENTIDADE = P.ENT_CODIGO  -- << ALTERADO                                
                                      )                                                                                                  
                            ),                                                                                                            
                            0,                                                                                                            
                            (                                                                                                            
                                SELECT SUM(I.IPLA_VALORTOTAL)                                                                            
                                FROM aplic2008.ITEM_PROC_LICIT_ADESAO@conectprod I                                                        
                                INNER JOIN aplic2008.TIPO_ITEM_PROC_LICIT@conectprod TIPL                                                
                                    ON TIPL.TIPL_CODIGO = I.TIPL_CODIGO                                                                  
                                INNER JOIN aplic2008.UND_FORNECIMENTO@conectprod U                                                        
                                    ON U.UNDFORNEC_CODIGO = I.UNDFRN_CODIGO                                                              
                                WHERE I.ENT_CODIGO = P.ENT_CODIGO  -- << ALTERADO                                                        
                                  AND I.PLIC_NUMERO = P.PLIC_NUMERO                                                                      
                                  AND I.MLIC_CODIGO = P.MLIC_CODIGO                                                                      
                            ),                                                                                                            
                            (                                                                                                            
                                SELECT SUM((A.PLICATAI_QUANTIDADE * PPLI.PCPLIC_VALORUNITARIOCOTADO))                                    
                                FROM aplic2008.PARTICIP_ATA_REG_PRECO_ITEM@conectprod A                                                  
                                INNER JOIN aplic2008.PARTICIPACAO_PROC_LICIT_ITEM@conectprod PPLI                                        
                                    ON PPLI.ENT_CODIGO = A.ENT_CODIGO                                                                    
                                   AND PPLI.PLIC_NUMERO = A.PLIC_NUMERO                                                                  
                                   AND PPLI.MLIC_CODIGO = A.MLIC_CODIGO                                                                  
                                   AND PPLI.PLIC_DATA = A.PLIC_DATASITUACAOITEM                                                          
                                   AND PPLI.PLIC_SITUACAO = A.PLIC_SITUACAOITEM                                                          
                                   AND PPLI.PLICLOTE_NUMEROLOTE = A.PLICLOTE_NUMEROLOTE                                                  
                                   AND PPLI.IPLIC_NUMEROITEM = A.IPLIC_NUMEROITEM                                                        
                                WHERE A.PLIC_NUMERO = P.PLIC_NUMLICITACAO                                                                
                                  AND A.MLIC_CODIGO = P.PLIC_MODALIDADE                                                                  
                                  AND A.PLICATA_NUMATA = P.PLIC_NUMATA                                                                    
                                  AND A.PLICATAI_TIPOPARTICIPACAO IN (2, 3)                                                              
                                  AND PPLI.PCPLIC_VENCEDOR = 'S'                                                                          
                                  AND PPLI.PCPLIC_TIPOVALOR = 1                                                                          
                                  AND REPLACE(REPLACE(REPLACE(A.PLICATAI_CNPJPARTICIPANTE, '.', ''), '-', ''), '/', '') =                
                                      (                                                                                                  
                                          SELECT E.CNPJ_DIREITO_PUBLICO                                                                  
                                          FROM aplic2008.ENTIDADE@conectprod E                                                            
                                          WHERE E.CNPJ_CPF_COD_TCE_ENTIDADE = P.ENT_CODIGO  -- << ALTERADO                                
                                      )                                                                                                  
                            )                                                                                                            
                        )                                                                                                                
                 FROM DUAL                                                                                                                
             ) AS "Valor Vencedor",                                                                                                      
                                                                                                                                         
             CAST('' AS INTEGER) AS "Cod. Situação",                                                                                      
             CAST('' AS VARCHAR2(50)) AS "Situação",                                                                                      
             CAST('' AS DATE) AS "Data Situação",                                                                                        
             CAST('' AS DATE) AS "Data Adjudicação",                                                                                      
             CAST('' AS DATE) AS "Data Julgamento Proposta",                                                                              
             CAST('' AS INTEGER) AS "Qtde.Dotação",                                                                                      
                                                                                                                                         
             P.PLIC_NOMEARQPDF AS "Arq.Processo Carona",                                                                                  
                                                                                                                                         
             CAST('' AS DATE) AS "Data abert. sessão públ.",                                                                              
                 
             P.PLIC_NUMLICITACAO AS "Nº licitação(Reg. de preço)",                                                                        
                 
             P.PLIC_MODALIDADE AS "Cód. Modalidade(Reg. de preço)",                                                                      
                 
             ML.MLIC_DESCRICAO AS "Modalidade(Reg. de preço)",                                                                            
                 
             P.PLIC_NUMATA AS "Nº Ata reg. de preço",                                                                                    
                 
             P.PLIC_NUMPMIMPI AS "Nº PMI/MPI",                                                                                            
                 
             DECODE((                                                                                                                    
                 SELECT COUNT(1)
                 FROM aplic2008.PROC_LICIT_ATA_REGISTRO_PRECO@conectprod ARP
                 WHERE ARP.ENT_CODIGO = P.ENT_CODIGO                                                                                      
                   AND ARP.PLIC_NUMERO = P.PLIC_NUMERO                                                                                    
                   AND ARP.MLIC_CODIGO = P.MLIC_CODIGO                                                                                    
             ), 0, 'NÃO', 'SIM') AS "Possui ARP?",                                                                                        
                                                                                                                                         
             P.RGENV_DATAENVIO,                                                                                                          
                                                                                                                                         
             CAST('' AS VARCHAR2(50))                                                                                                    
                 
      FROM aplic2008.PROCESSO_LICITATORIO@conectprod P                                                                                    
                 
      INNER JOIN aplic2008.MODALIDADE_LICITACAO@conectprod M                                                                              
          ON M.MLIC_CODIGO = P.MLIC_CODIGO
                                                                                                                                         
      LEFT JOIN aplic2008.MODALIDADE_LICITACAO@conectprod ML                                                                              
          ON ML.MLIC_CODIGO = P.PLIC_MODALIDADE                                                                                          
                                                                                                                                         
      LEFT JOIN aplic2008.CADASTRO_GERAL@conectprod C                                                                                    
          ON P.ENT_CODIGO = C.ENT_CODIGO                                                                                                  
         AND C.EXERCICIO >= 2015                                                                                                          
         AND P.CG_IDENTIFICACAO = C.CG_IDENTIFICACAO                                                                                      
                                                                                                                                         
      INNER JOIN aplic2008.ENTIDADE@conectprod VW                                                                                        
          ON P.ENT_CODIGO = VW.CNPJ_CPF_COD_TCE_ENTIDADE                                                                                  
                                                                                                                                         
      INNER JOIN publico.MUNICIPIO@conectprod MN                                                                                          
          ON MN.MUN_CODIGO = VW.MUN_CODIGO                                                                                                
                                                                                                                                         
      WHERE P.ENT_CODIGO IN ('1159326', '1113257', '1118736', '1112309')  -- << ALTERADO                                                  
        AND SUBSTR(P.PLIC_NUMERO, 13, 4) IN ('2026')                                                                                      
        AND P.MLIC_CODIGO IN ('17', '22', '23', '25')                                                                                    
                                                                                                                                         
  )                                                                                                                                      
                                                                                                                                         
  ORDER BY "Nº Licitação", "Cod. Modalidade"; //