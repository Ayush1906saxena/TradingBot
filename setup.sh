#!/bin/bash
# One-command setup for macOS (Apple Silicon M4 Pro)
set -e

echo "=== Algo Trading System Setup (macOS) ==="

# Check if Homebrew is installed
if ! command -v brew &> /dev/null; then
    echo "Installing Homebrew..."
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
fi

# Install Python 3.11+ via Homebrew (macOS ships with older Python or none)
if ! command -v python3.11 &> /dev/null && ! python3 --version 2>&1 | grep -q "3.1[1-9]"; then
    echo "Installing Python 3.11..."
    brew install python@3.11
fi

# Use whatever python3 is available (3.11+ should be present)
PYTHON=$(command -v python3.11 || command -v python3)
echo "Using Python: $($PYTHON --version)"

# SQLite comes pre-installed on macOS — no action needed

# Create virtual environment
$PYTHON -m venv venv
source venv/bin/activate

# Install Python packages
pip install --upgrade pip
pip install -r requirements.txt

# Create directories
mkdir -p db logs

# Copy config template if not exists
if [ ! -f configs/config.yaml ]; then
    echo "configs/config.yaml already present"
fi

# Copy env template if not exists
if [ ! -f .env ]; then
    cp .env.example .env
    echo "Created .env — optionally add secrets here instead of config.yaml"
fi

# Prevent Mac from sleeping during market hours (optional helper)
echo ""
echo "=== IMPORTANT: Prevent Mac Sleep During Market Hours ==="
echo "Your Mac must stay awake from 9:00 AM to 4:00 PM IST for live/paper trading."
echo "Options:"
echo "  1. System Settings → Energy → Prevent automatic sleeping when display is off"
echo "  2. Or run: caffeinate -d -t 28800 &  (keeps Mac awake for 8 hours)"
echo "  3. Or use the included launchd service (see README)"
echo ""

echo "=== Setup complete ==="
echo "Next steps:"
echo "  1. Edit configs/config.yaml with your broker credentials"
echo "  2. Run: source venv/bin/activate"
echo "  3. Run: python main.py --mode backtest"
