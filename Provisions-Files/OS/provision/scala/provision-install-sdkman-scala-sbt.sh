#!/bin/bash
set -e

echo "================================================"
echo "=== Installation SDKMAN + Scala + SBT        ==="
echo "================================================"

# ─────────────────────────────────────────
# Dépendances requises
# ─────────────────────────────────────────
apt-get update -qq
apt-get install -y \
    curl \
    wget \
    unzip \
    zip \
    gnupg \
    software-properties-common

# ─────────────────────────────────────────
# Installation SDKMAN pour root
# ─────────────────────────────────────────
echo "=== Installation SDKMAN ==="

export SDKMAN_DIR="/usr/local/sdkman"

curl -s "https://get.sdkman.io" | bash

# Charger SDKMAN
source "${SDKMAN_DIR}/bin/sdkman-init.sh"

echo "SDKMAN version : $(sdk version)"

# ─────────────────────────────────────────
# Installation Scala via SDKMAN
# ─────────────────────────────────────────
echo "=== Installation Scala ==="

# Scala 2.13 (stable, compatible Spark/Trino)
sdk install scala 2.13.12

# Scala 3 (optionnel)
# sdk install scala 3.3.1

echo "Scala version : $(scala -version 2>&1)"

# ─────────────────────────────────────────
# Installation SBT via SDKMAN
# ─────────────────────────────────────────
echo "=== Installation SBT ==="

sdk install sbt 1.9.7

echo "SBT version : $(sbt -version 2>&1 | head -1)"

# ─────────────────────────────────────────
# Rendre SDKMAN disponible pour vagrant
# ─────────────────────────────────────────
echo "=== Configuration SDKMAN pour vagrant ==="

# Installer SDKMAN aussi pour l'utilisateur vagrant
su - vagrant -c '
    export SDKMAN_DIR="$HOME/.sdkman"
    curl -s "https://get.sdkman.io" | bash
    source "$HOME/.sdkman/bin/sdkman-init.sh"

    # Scala
    sdk install scala 2.13.12

    # SBT
    sdk install sbt 1.9.7

    echo "Scala : $(scala -version 2>&1)"
    echo "SBT   : $(sbt -version 2>&1 | head -1)"
'

# ─────────────────────────────────────────
# Ajouter SDKMAN au .bashrc de vagrant
# ─────────────────────────────────────────
cat >> /home/vagrant/.bashrc << 'EOF'

# SDKMAN
export SDKMAN_DIR="$HOME/.sdkman"
[[ -s "$HOME/.sdkman/bin/sdkman-init.sh" ]] && source "$HOME/.sdkman/bin/sdkman-init.sh"
EOF

chown vagrant:vagrant /home/vagrant/.bashrc

# ─────────────────────────────────────────
# Variables d'environnement globales
# ─────────────────────────────────────────
cat > /etc/profile.d/scala.sh << 'EOF'
export SDKMAN_DIR="$HOME/.sdkman"
[[ -s "$HOME/.sdkman/bin/sdkman-init.sh" ]] && source "$HOME/.sdkman/bin/sdkman-init.sh"
EOF

chmod +x /etc/profile.d/scala.sh

# ─────────────────────────────────────────
# Vérification finale
# ─────────────────────────────────────────
echo ""
echo "================================================"
su - vagrant -c '
    source "$HOME/.sdkman/bin/sdkman-init.sh"
    echo "SDKMAN  : $(sdk version)"
    echo "Scala   : $(scala -version 2>&1)"
    echo "SBT     : $(sbt -version 2>&1 | head -1)"
'
echo "=== Installation SDKMAN + Scala + SBT terminée ✓ ==="
echo "================================================"

# ─────────────────────────────────────────
# Ajouter XDG_RUNTIME_DIR au .bashrc de vagrant. SBT utilise XDG_RUNTIME_DIR pour trouver /run/user/1000. le user vagrant
# ─────────────────────────────────────────

echo 'export XDG_RUNTIME_DIR=/tmp/runtime-$(id -u)' >> ~/.bashrc
source ~/.bashrc