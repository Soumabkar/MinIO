#!/bin/bash
set -e

echo "================================================"
echo "=== Installation Java 17 pour Ubuntu        ==="
echo "================================================"

# Mise à jour des paquets
apt-get update -qq

# Installer les dépendances
apt-get install -y \
    curl \
    wget \
    gnupg \
    software-properties-common

# ─────────────────────────────────────────
# Installation Java 17 (LTS)
# ─────────────────────────────────────────

# Option 1 — OpenJDK (dépôt Ubuntu officiel)
apt-get install -y openjdk-17-jdk

# Option 2 — Eclipse Temurin (Adoptium) — plus récent et mieux maintenu
# install -m 0755 -d /etc/apt/keyrings
# curl -fsSL https://packages.adoptium.net/artifactory/api/gpg/key/public \
#   | gpg --dearmor \
#   | tee /etc/apt/keyrings/adoptium.gpg > /dev/null
# echo "deb [signed-by=/etc/apt/keyrings/adoptium.gpg] https://packages.adoptium.net/artifactory/deb \
#   $(. /etc/os-release && echo "$VERSION_CODENAME") main" \
#   | tee /etc/apt/sources.list.d/adoptium.list > /dev/null
# apt-get update -qq
# apt-get install -y temurin-17-jdk

# ─────────────────────────────────────────
# Définir JAVA_HOME (global + permanent)
# ─────────────────────────────────────────
JAVA_HOME_PATH="/usr/lib/jvm/java-17-openjdk-amd64"

# Pour tous les utilisateurs
cat >> /etc/environment << EOF
JAVA_HOME="${JAVA_HOME_PATH}"
PATH="${JAVA_HOME_PATH}/bin:$PATH"
EOF

# Pour les shells bash
cat > /etc/profile.d/java.sh << EOF
export JAVA_HOME="${JAVA_HOME_PATH}"
export PATH="\$JAVA_HOME/bin:\$PATH"
EOF

chmod +x /etc/profile.d/java.sh
source /etc/profile.d/java.sh

# ─────────────────────────────────────────
# Pour l'utilisateur vagrant
# ─────────────────────────────────────────
cat >> /home/vagrant/.bashrc << EOF

# Java
export JAVA_HOME="${JAVA_HOME_PATH}"
export PATH="\$JAVA_HOME/bin:\$PATH"
EOF

chown vagrant:vagrant /home/vagrant/.bashrc

# ─────────────────────────────────────────
# Vérification
# ─────────────────────────────────────────
echo "JAVA_HOME  : ${JAVA_HOME_PATH}"
echo "Java       : $(java -version 2>&1 | head -1)"
echo "Javac      : $(javac -version)"
echo "=== Installation Java 17 terminée ✓ ==="