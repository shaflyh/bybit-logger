module.exports = {
  apps: [
    {
      name: "bybit-logger",
      script: "realtime_logger.py",
      interpreter: "./venv/bin/python",

      // Restart configuration
      autorestart: true,
      restart_delay: 5000, // Wait 5 seconds before restart
      max_restarts: 10, // Max restarts per minute
      min_uptime: "30s", // Minimum uptime before considering stable

      // Memory management
      max_memory_restart: "200M", // Restart if memory exceeds 200MB

      // Process management
      instances: 1,
      exec_mode: "fork",

      // Monitoring and logs
      watch: false,
      ignore_watch: ["node_modules", "logs", "*.log"],

      // Error handling
      exp_backoff_restart_delay: 100, // Exponential backoff for restarts

      // Health check (optional - requires custom endpoint)
      // health_check_grace_period: 3000,
      // health_check_fatal_exceptions: true,

      // Logging configuration
      log_date_format: "YYYY-MM-DD HH:mm:ss Z",
      error_file: "./logs/bybit-logger-error.log",
      out_file: "./logs/bybit-logger-out.log",
      log_file: "./logs/bybit-logger-combined.log",

      // Time settings
      time: true,

      // Kill timeout
      kill_timeout: 5000,
    },
  ],
};
