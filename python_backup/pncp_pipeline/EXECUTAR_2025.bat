@echo off
echo ============================================================
echo  PASSO 1 — Coleta PNCP Janeiro a Abril 2025
echo ============================================================

cd /d "%~dp0"

python main.py --from 20250101 --to 20250430

echo.
echo ============================================================
echo  PASSO 2 — Crossmatch PNCP x APLIC + Sync Firebase
echo  Municipios: Sinop, Lucas do Rio Verde, Rondolandia,
echo              Acorizal, Jangada
echo  Ano: 2025
echo ============================================================

python pipeline_multicidades.py ^
  --cidades sinop "lucas do rio verde" rondolandia acorizal jangada ^
  --ano 2025 ^
  --pncp-excel output\pncp_contratacoes_MT_20250101_20250430.xlsx

echo.
echo ============================================================
echo  Concluido! Resultados em output\crossmatch_*_2025.xlsx
echo ============================================================
pause
