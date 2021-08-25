echo "INICIANDO SCRAPPER %PROPPI%"

cmd /k "cd /d %PROPPI%\venv\proppienv\Scripts & activate & cd /d   %PROPPI% & python src/scrapper.py"

echo "FINALIZANDO SCRAPPER"

PAUSE