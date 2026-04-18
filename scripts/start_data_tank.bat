@echo off
REM "Bồn dữ liệu": làm đầy snapshot_disk_cache.json định kỳ (mặc định 30 phút / vòng).
REM Chạy file này song song với Streamlit — cửa sổ riêng, không chặn giao diện app.
cd /d "%~dp0.."
set PYTHONUNBUFFERED=1
set II_REFRESH_INTERVAL_SEC=1800
set II_SNAPSHOT_CACHE_TTL_SEC=1800
echo [1] Mot lan day du ngay: python scripts\fill_data_tank.py
echo [2] Loop nen: snapshot_disk_refresh_loop
echo Starting loop. Close this window to stop.
python scripts\snapshot_disk_refresh_loop.py --all --max 500
pause
