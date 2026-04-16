# 🚀 Railway Deployment Guide: Enterprise Scraper V2

This guide ensures your scraper is correctly configured to utilize your **8 vCPU / 8 GB RAM** resources for maximum lead generation.

## 1. Resource Allocation (MANDATORY)
In your Railway project:
1.  Go to **Settings** -> **Resource Limits**.
2.  Set **CPU** to **8 vCPU**.
3.  Set **Memory** to **8 GB**.

## 2. Environment Variables
Add these keys in the **Variables** tab of your Railway service:

| Variable | Recommended Value | Description |
| :--- | :--- | :--- |
| `PROCESS_TYPE` | `automator` | Switches the app to 100-city automation mode. |
| `MAX_CONCURRENT` | `20` | Leverages your 8 vCPUs for parallel scraping. |
| `HEADLESS` | `True` | Runs browsers in the background (Required). |
| `PROXY_HOST` | `gw.dataimpulse.com:823` | Your Data Impulse host. |
| `PROXY_USER` | `1accade8fd4acb75b8ae` | Your Data Impulse login. |
| `PROXY_PASS` | `c51e0887f7425452` | Your Data Impulse password. |
| `TIMEOUT` | `120000` | 2 mins timeout for deep discovery. |

## 3. Deployment Steps
1.  Push your changes to your GitHub repository connected to Railway.
2.  Railway will automatically build the image using the provided `Dockerfile`.
3.  Monitor the **Logs** tab. You should see `🚀 [BOOTSTRAP] Handoff to Enterprise Automator...`.

## 4. Progress Monitoring
- **Logs**: The `EnterpriseAutomator` will log every city it finishes and the total leads found.
- **Database**: Check your Postgres instance in Railway to see the `contacts` table filling up in real-time.

---
> [!TIP]
> **Data Usage**: Monitor your Data Impulse dashboard periodically. While the scraper blocks images to save data, high-volume scraping can still consume GBs over time.
