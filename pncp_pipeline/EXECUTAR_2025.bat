@echo off
echo ============================================================
echo  COLETA PNCP - Janeiro a Abril 2025
echo ============================================================

cd /d "%~dp0"

python main.py --from 20250101 --to 20250430

echo.
echo ============================================================
echo  Coleta concluida! Excel salvo em output\
echo  Proximo passo: rodar pipeline_multicidades.py
echo ============================================================
pause
