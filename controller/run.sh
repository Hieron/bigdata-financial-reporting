#!/bin/bash

LOG_FILE="/tmp/data/logs/$(date '+%Y%m%d%H%M%S').log"

log() {
    local TYPE=$1
    local MESSAGE=$2
    echo "$(date '+%y/%m/%d %H:%M:%S') $TYPE $MESSAGE" >> "$LOG_FILE"
}

echo "Creating logs folder..."
mkdir -p /tmp/data/logs

echo "Installing system dependencies..."
log "INFO" "Iniciando a instalação das dependências de sistema."

# Atualizar o gerenciador de pacotes
apt-get update >> "$LOG_FILE" 2>&1
if [ $? -ne 0 ]; then
    echo "Error: Failed to update package lists."
    log "ERROR" "Falha ao atualizar as listas de pacotes."
    exit 1
else
    log "INFO" "Listas de pacotes atualizadas com sucesso."
fi

# Instalar as dependências necessárias para WeasyPrint
apt-get install -y \
    libcairo2 \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libgdk-pixbuf2.0-0 \
    libffi-dev \
    shared-mime-info \
    libssl-dev \
    libjpeg-dev \
    libxml2-dev \
    libxslt1-dev \
    zlib1g-dev \
    libgirepository1.0-dev \
    pkg-config \
    >> "$LOG_FILE" 2>&1

if [ $? -ne 0 ]; then
    echo "Error: Failed to install system dependencies."
    log "ERROR" "Falha ao instalar dependências de sistema."
    exit 1
else
    log "INFO" "Dependências de sistema instaladas com sucesso."
fi

echo "Installing Python dependencies..."
log "INFO" "Iniciando a instalação das dependências Python."

# Atualizar pip
pip install --upgrade pip >> "$LOG_FILE" 2>&1
if [ $? -ne 0 ]; then
    echo "Error: Failed to upgrade pip."
    log "ERROR" "Falha ao atualizar o pip."
    exit 1
else
    log "INFO" "pip atualizado com sucesso."
fi

# Instalar dependências Python
pip install -r /tmp/data/requirements.txt >> "$LOG_FILE" 2>&1
if [ $? -ne 0 ]; then
    echo "Error: Failed to install Python dependencies."
    log "ERROR" "Falha ao instalar dependências Python."
    exit 1
else
    log "INFO" "Dependências Python instaladas com sucesso."
fi

echo "Running the Python script app.py..."
log "INFO" "Iniciando a execução do script Python app.py."

# Executar o script Python e redirecionar logs
python3 /tmp/data/app.py #>> "$LOG_FILE" 2>&1
if [ $? -ne 0 ]; then
    echo "Error: Python script failed."
    log "ERROR" "Script Python falhou."
    exit 1
else
    log "INFO" "Script Python executado com sucesso."
fi
