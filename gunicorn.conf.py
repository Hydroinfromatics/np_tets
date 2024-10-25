
import os

# Get port from environment variable
port = os.environ.get('PORT', 8050)

# Server socket
bind = f"0.0.0.0:{port}"
workers = 4  # Adjust based on your needs
threads = 2

# Timeout
timeout = 120

# Logging
accesslog = '-'
errorlog = '-'
loglevel = 'info'

# Worker process naming
proc_name = 'water-monitoring-dashboard'

# Maximum requests a worker will process before restarting
max_requests = 1000
max_requests_jitter = 50

# Restart workers gracefully
graceful_timeout = 30