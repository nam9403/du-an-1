@echo off
cd /d "%~dp0"

set "APP_MODE=%~1"
if /I "%APP_MODE%"=="" set "APP_MODE=dev"
if /I "%APP_MODE%"=="--prod" set "APP_MODE=prod"
if /I "%APP_MODE%"=="prod" goto :PROFILE_PROD
if /I "%APP_MODE%"=="--dev" set "APP_MODE=dev"
if /I not "%APP_MODE%"=="dev" (
  echo [WARN] Mode khong hop le: %APP_MODE%
  echo [INFO] Su dung: run_app.bat ^| run_app.bat dev ^| run_app.bat prod
  set "APP_MODE=dev"
)
goto :PROFILE_DEV

:PROFILE_DEV
echo [INFO] App mode: DEV-FAST (uu tien toc do, cho phep stale cache)
set "II_ENV=dev"
set "II_SNAPSHOT_ATTACH_LIVE=0"
set "II_READ_STALE_DISK=1"
set "VALUE_INVESTOR_PORTAL_TIMEOUT=4"
set "II_PORTAL_TIMEOUT_LADDER_SEC=2,4"
set "II_ALIGN_PRICE_WITH_OHLCV=1"
set "II_OHLCV_DISK_FIRST=1"
set "II_OHLCV_DISK_MAX_AGE_SEC=7200"
set "II_FINANCIAL_DISK_FIRST=1"
set "II_FINANCIAL_DISK_MAX_AGE_SEC=21600"
set "II_FINANCIAL_MAX_PROBES=1"
set "II_PORTAL_LIVE_BUDGET_SEC=6"
set "II_REQUIRE_APP_SECRET_KEY=0"
goto :PROFILE_COMMON

:PROFILE_PROD
echo [INFO] App mode: PROD-STRICT (uu tien do tin cay du lieu va bao mat)
set "II_ENV=prod"
set "II_SNAPSHOT_ATTACH_LIVE=1"
set "II_READ_STALE_DISK=0"
set "VALUE_INVESTOR_PORTAL_TIMEOUT=10"
set "II_PORTAL_TIMEOUT_LADDER_SEC=5,10,15"
set "II_ALIGN_PRICE_WITH_OHLCV=1"
set "II_OHLCV_DISK_FIRST=0"
set "II_OHLCV_DISK_MAX_AGE_SEC=900"
set "II_FINANCIAL_DISK_FIRST=0"
set "II_FINANCIAL_DISK_MAX_AGE_SEC=1800"
set "II_FINANCIAL_MAX_PROBES=2"
set "II_PORTAL_LIVE_BUDGET_SEC=15"
set "II_REQUIRE_APP_SECRET_KEY=1"
if "%II_APP_SECRET_KEY%"=="" (
  echo [ERROR] II_APP_SECRET_KEY dang rong. Dat key truoc khi chay production.
  echo [HINT] setx II_APP_SECRET_KEY "your_fernet_key_here"
  pause
  exit /b 1
)
goto :PROFILE_COMMON

:PROFILE_COMMON
REM Hybrid Legend calibrated defaults (batch calibration consensus)
set "II_LEGEND_PROFILE=defensive"
set "II_LEGEND_STRONG_BUY_MOS_MIN=8"
set "II_LEGEND_MAX_PEG_FOR_BUY=1.1"
set "II_LEGEND_WATCH_BUY_MOS_MIN=8"
REM Tuyet doi KHONG hardcode API key trong file batch.
REM Cau hinh key tu Environment Variables cua may:
REM setx GROQ_API_KEYS "gsk_key_1;gsk_key_2"
REM setx OPENAI_API_KEYS "sk_key_1;sk_key_2"
REM setx GEMINI_API_KEYS "AIza_key_1;AIza_key_2"
if "%GROQ_API_KEYS%"=="" (
  echo [WARN] GROQ_API_KEYS dang rong. App van chay, nhung co the fallback/no-LLM.
)
REM AI auto routing: speed | balanced | quality
set "AI_AUTO_TASK_MODE=balanced"
REM Neu confidence du lieu thap hon nguong nay thi tu dong nang cap sang tang quality
set "AI_ESCALATE_CONFIDENCE_BELOW=70"
REM Cache ket qua LLM de giam do tre khi phan tich lap lai
set "AI_LLM_CACHE_TTL_SEC=240"
REM Thu tu uu tien provider khi preferred_llm=auto (global/default)
set "AI_AUTO_PROVIDER_ORDER=groq,openai,gemini"
REM Co the override theo mode:
REM set "AI_AUTO_PROVIDER_ORDER_SPEED=groq,gemini,openai"
REM set "AI_AUTO_PROVIDER_ORDER_BALANCED=groq,openai,gemini"
REM set "AI_AUTO_PROVIDER_ORDER_QUALITY=openai,gemini,groq"

set "PY=%LOCALAPPDATA%\Programs\Python\Python312\python.exe"
if exist "%PY%" (
  "%PY%" -m streamlit run app.py
  if errorlevel 1 pause
  exit /b %errorlevel%
)

where py >nul 2>&1
if %errorlevel%==0 (
  py -m streamlit run app.py
  if errorlevel 1 pause
  exit /b %errorlevel%
)

where python >nul 2>&1
if %errorlevel%==0 (
  python -m streamlit run app.py
  if errorlevel 1 pause
  exit /b %errorlevel%
)

echo Khong tim thay Python. Cai Python 3 hoac them python.exe vao PATH.
pause
exit /b 1
