#!/bin/bash

set -e  # Arrêter en cas d'erreur

echo "=== Installation Java pour Scala 2.12 ==="

# Configuration non-interactive pour éviter les prompts
export DEBIAN_FRONTEND=noninteractive
export DEBCONF_NONINTERACTIVE_SEEN=true

# Mise à jour des paquets
echo "Mise à jour des paquets..."
sudo apt-get update -qq

# Installer Java 8
echo ""
echo "Installation d'OpenJDK 8, unzip et zip..."
sudo apt-get install -y -qq openjdk-8-jdk unzip zip curl

# Vérifier l'installation
echo ""
java -version
javac -version

# Configurer JAVA_HOME
echo ""
echo "Configuration de JAVA_HOME..."
if ! grep -q "JAVA_HOME.*java-8" /home/vagrant/.bashrc; then
    cat >> /home/vagrant/.bashrc << 'EOF'

# Java 8 pour Scala 2.12
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=$PATH:$JAVA_HOME/bin
EOF
    echo "✓ JAVA_HOME configuré dans .bashrc"
fi

# Définir Java 8 comme version par défaut
echo ""
echo "Définition de Java 8 comme version par défaut..."
sudo update-alternatives --set java /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java

# SDKMAN Installation
echo ""
echo "=== Installation SDKMAN ==="

if [ -d "/home/vagrant/.sdkman" ]; then
    echo "✓ SDKMAN est déjà installé"
else
    echo "Installation de SDKMAN..."
    # Installer SDKMAN en tant qu'utilisateur vagrant
    sudo -u vagrant bash << 'SDKMAN_INSTALL'
export SDKMAN_DIR="/home/vagrant/.sdkman"
curl -s "https://get.sdkman.io" | bash
SDKMAN_INSTALL
fi

# Installer Scala et sbt via SDKMAN
echo ""
echo "=== Installation Scala et sbt ==="

sudo -u vagrant bash << 'SCALA_INSTALL'
# Sourcer SDKMAN
export SDKMAN_DIR="/home/vagrant/.sdkman"
source "$SDKMAN_DIR/bin/sdkman-init.sh"

# Installer Scala 2.12.19
if sdk list scala 2>&1 | grep -q "2.12.19"; then
    if ! sdk current scala 2>&1 | grep -q "2.12.19"; then
        echo "Installation de Scala 2.12.19..."
        sdk install scala 2.12.19 < /dev/null
    else
        echo "✓ Scala 2.12.19 est déjà installé"
    fi
fi

# Installer sbt
if ! command -v sbt &> /dev/null; then
    echo "Installation de sbt..."
    sdk install sbt < /dev/null
else
    echo "✓ sbt est déjà installé"
fi

# Vérifier les installations
echo ""
echo "=== Vérification des installations ==="
echo "Scala version:"
scala -version 2>&1 || echo "Erreur lors de la vérification de Scala"

echo ""
echo "sbt version:"
sbt --version 2>&1 | head -n 1 || echo "Erreur lors de la vérification de sbt"
SCALA_INSTALL

echo ""
echo "=== Installation terminée ==="
echo ""
echo "Pour utiliser Scala et sbt, reconnectez-vous:"
echo "  vagrant ssh"
echo ""
echo "Puis vérifiez:"
echo "  java -version"
echo "  scala -version"
echo "  sbt --version"