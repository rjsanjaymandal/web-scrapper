@echo off
git add .
git commit -m "Switch: Deleted Dockerfile to use Procfile instead"
git push origin master
echo Done! Please check your Railway dashboard in 30 seconds.
pause
