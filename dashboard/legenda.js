/**
 * legenda.js — Renderiza o bloco de legenda explicativa do Monitor PNCP × APLIC.
 * Incluir em qualquer página do projeto:
 *   <div id="legenda-monitor"></div>
 *   <script src="legenda.js"></script>
 */
(function () {
  const el = document.getElementById("legenda-monitor");
  if (!el) return;

  el.innerHTML = `
  <div style="background:#fff;border-radius:10px;border:1px solid #dde1ea;padding:28px 32px;font-size:.84rem;color:#444;line-height:1.7">

    <h2 style="font-size:1rem;color:#1a3a6b;margin-bottom:18px;font-weight:600">Como funciona este monitor</h2>

    <div style="display:grid;grid-template-columns:1fr 1fr;gap:28px 40px">

      <!-- Abas -->
      <div>
        <h3 style="font-size:.88rem;color:#1a3a6b;margin-bottom:8px;font-weight:600">📋 O que significa cada aba</h3>
        <table style="width:100%;border-collapse:collapse;font-size:.82rem">
          <tr>
            <td style="padding:5px 8px;border-left:4px solid #28a745;padding-left:10px;font-weight:600">Em Ambos</td>
            <td style="padding:5px 8px">A licitação foi publicada no PNCP <strong>e</strong> enviada ao APLIC. O cruzamento foi bem-sucedido.</td>
          </tr>
          <tr>
            <td style="padding:5px 8px;border-left:4px solid #ffc107;padding-left:10px;font-weight:600">Apenas PNCP</td>
            <td style="padding:5px 8px">Publicada no PNCP mas ainda <strong>não encontrada no APLIC</strong>. O município tem 3 dias úteis para enviar.</td>
          </tr>
          <tr>
            <td style="padding:5px 8px;border-left:4px solid #fd7e14;padding-left:10px;font-weight:600">Apenas APLIC</td>
            <td style="padding:5px 8px">Existe no APLIC mas <strong>não foi ao PNCP</strong>. Pode indicar publicação irregular ou fora do prazo.</td>
          </tr>
          <tr>
            <td style="padding:5px 8px;border-left:4px solid #dc3545;padding-left:10px;font-weight:600">Alertas Ativos</td>
            <td style="padding:5px 8px">Licitações em "Apenas PNCP" cujo prazo de 3 dias úteis <strong>já venceu</strong> sem APLIC registrado.</td>
          </tr>
        </table>
      </div>

      <!-- Como o cruzamento é feito -->
      <div>
        <h3 style="font-size:.88rem;color:#1a3a6b;margin-bottom:8px;font-weight:600">🔗 Como o cruzamento é feito</h3>
        <p style="margin-bottom:8px">O sistema tenta cruzar PNCP × APLIC em <strong>3 etapas (tiers)</strong>, do mais preciso ao mais amplo:</p>
        <ol style="padding-left:18px;margin:0">
          <li style="margin-bottom:6px"><strong>Semântico + Valor</strong> — compara o texto do objeto/objetivo (similaridade RapidFuzz) e verifica se o valor estimado diverge menos de 10%. Critério mais confiável.</li>
          <li style="margin-bottom:6px"><strong>CNPJ + Data</strong> — mesmo órgão (CNPJ) e datas com até 30 dias de diferença. Usado quando o texto varia muito.</li>
          <li style="margin-bottom:6px"><strong>Estrutural</strong> — mesmo município, número, ano e modalidade. Fallback de último recurso.</li>
        </ol>
      </div>

      <!-- Score -->
      <div>
        <h3 style="font-size:.88rem;color:#1a3a6b;margin-bottom:8px;font-weight:600">📊 O que é o Score</h3>
        <p style="margin-bottom:8px">O score (0–100) mede a confiança no cruzamento:</p>
        <ul style="padding-left:18px;margin:0 0 10px">
          <li><strong>50%</strong> — similaridade de texto (objeto PNCP vs objetivo/motivo APLIC)</li>
          <li><strong>30%</strong> — proximidade de valor estimado</li>
          <li><strong>20%</strong> — proximidade de data</li>
        </ul>
        <div style="display:flex;gap:8px;flex-wrap:wrap">
          <span style="background:#d4edda;color:#155724;border-radius:10px;padding:2px 10px;font-size:.78rem;font-weight:600">≥ 85 → Confirmado</span>
          <span style="background:#fff3cd;color:#856404;border-radius:10px;padding:2px 10px;font-size:.78rem;font-weight:600">70–84 → Parcial</span>
          <span style="background:#f8d7da;color:#721c24;border-radius:10px;padding:2px 10px;font-size:.78rem;font-weight:600">&lt; 70 → Sem match</span>
        </div>
      </div>

      <!-- Sistemas -->
      <div>
        <h3 style="font-size:.88rem;color:#1a3a6b;margin-bottom:8px;font-weight:600">🏛️ Os dois sistemas monitorados</h3>
        <ul style="padding-left:18px;margin:0">
          <li style="margin-bottom:6px"><strong>PNCP</strong> — portal federal onde órgãos públicos publicam licitações. Dados coletados automaticamente via API.</li>
          <li style="margin-bottom:6px"><strong>APLIC</strong> (TCE-MT) — sistema Oracle interno. Municípios devem enviar em até <strong>3 dias úteis</strong> após publicar no PNCP. Dados inseridos via exportação manual do Oracle.</li>
        </ul>
        <p style="margin-top:10px;color:#888;font-size:.78rem">Órgãos monitorados: Prefeitura Municipal de Sinop · Câmara Municipal de Sinop · IPREV Sinop</p>
      </div>

    </div>

    <!-- Diagrama de contagem -->
    <div style="margin-top:28px;padding-top:22px;border-top:1px solid #eef0f4">
      <h3 style="font-size:.88rem;color:#1a3a6b;margin-bottom:12px;font-weight:600">🔢 Como interpretar os números dos cards</h3>
      <p style="margin-bottom:14px;color:#555">
        Os cards no topo mostram a distribuição das licitações entre os dois sistemas.
        O total do PNCP é a soma de <strong>Em Ambos</strong> + <strong>Apenas PNCP</strong>.
        O total do APLIC é a soma de <strong>Em Ambos</strong> + <strong>Apenas APLIC</strong>.
      </p>
      <div style="background:#f5f7fb;border-radius:8px;padding:16px 20px;font-family:monospace;font-size:.82rem;line-height:2;color:#333">
        PNCP (total coletado)  ──┬── <span style="color:#28a745;font-weight:700">Em Ambos</span>      → foram encontrados nos dois sistemas<br>
        &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;└── <span style="color:#e6a800;font-weight:700">Apenas PNCP</span>   → publicados no PNCP, sem par no APLIC ainda<br>
        <br>
        APLIC (exportação)     ──┬── <span style="color:#28a745;font-weight:700">Em Ambos</span>      → foram cruzados com um registro PNCP<br>
        &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;└── <span style="color:#fd7e14;font-weight:700">Apenas APLIC</span>  → existem no APLIC sem publicação no PNCP
      </div>
      <p style="margin-top:10px;color:#888;font-size:.78rem">
        Exemplo: 48 Em Ambos + 50 Apenas PNCP = 98 licitações coletadas do PNCP no período.
        0 Apenas APLIC significa que todos os registros da exportação Oracle foram cruzados com sucesso.
      </p>
    </div>

  </div>
  `;
})();
