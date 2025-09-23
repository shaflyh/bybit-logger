module.exports = {
  apps: [
    {
      name: "bybit-logger",
      script: "realtime_logger.py",
      interpreter: "./venv/bin/python",
      restart_on_exit: true,
      watch: false,
    },
  ],
};
