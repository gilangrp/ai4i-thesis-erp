// pm2 start ecosystem.config.js   |   pm2 startOrReload ecosystem.config.js --update-env
const path = require("path");

module.exports = {
  apps: [
    {
      name: "ml-failure-api",
      // uvicorn dari virtualenv project; interpreter "none" = jalankan sebagai binary langsung
      script: path.join(__dirname, ".venv", "bin", "uvicorn"),
      interpreter: "none",
      args: `app:app --host 127.0.0.1 --port 8000 --workers ${process.env.UVICORN_WORKERS || 1}`,
      cwd: path.join(__dirname, "serving"),
      autorestart: true,
      max_restarts: 10,
      restart_delay: 3000,
      env: { PYTHONUNBUFFERED: "1" },
    },
  ],
};
