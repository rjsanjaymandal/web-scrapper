@echo off
git add .
git commit -m "Switch to Dockerfile: Robust Playwright support with browser binaries"
git push origin master
echo Done! Build triggered. Please check Railway dashboard: https://railway.app/dashboard
pause
