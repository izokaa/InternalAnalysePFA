@echo on
setlocal

REM Aller dans le dossier projet
cd /d "C:\Users\HP\Desktop\Desktop\Stage PFA_Data" || (echo [ERREUR] cd a echoue & pause & exit /b 1)

REM Vérifier Python Anaconda
"C:\Users\HP\anaconda3\python.exe" -V || (echo [ERREUR] Python introuvable & pause & exit /b 1)

REM Créer le dossier logs s'il manque
if not exist logs mkdir logs

REM Lancer le script (sans redirection pour VOIR la sortie)
"C:\Users\HP\anaconda3\python.exe" "C:\Users\HP\Desktop\Desktop\Stage PFA_Data\run_pipeline.py"

echo [INFO] Code retour = %ERRORLEVEL%
pause
endlocal
