#!/bin/bash

# Mettre à jour les paquets et installer les dépendances nécessaires
sudo apt-get update

#!/bin/bash

echo "=== Installation Java pour Scala 2.12 ==="

# Installer Java 8
echo ""
echo "Installation d'OpenJDK 8 unzip zip..."
sudo apt-get install -y openjdk-8-jdk unzip zip

# Vérifier l'installation
java -version
echo ""
javac -version

# Configurer JAVA_HOME si pas déjà fait
if ! grep -q "JAVA_HOME.*java-8" ~/.bashrc; then
    echo ""
    echo "Configuration de JAVA_HOME..."
    echo "" >> ~/.bashrc
    echo "# Java 8 pour Scala 2.12" >> ~/.bashrc
    echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" >> ~/.bashrc
    echo "export PATH=\$PATH:\$JAVA_HOME/bin" >> ~/.bashrc
    
    source ~/.bashrc
    echo "JAVA_HOME configuré"
fi

# Définir Java 8 comme version par défaut
echo ""
echo "Définition de Java 8 comme version par défaut..."
sudo update-alternatives --set java /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java
echo "JAVA_HOME: $JAVA_HOME"
echo ""
echo "Redémarrez votre terminal pour appliquer les changements"
echo ""
echo "Pour installer Scala 2.12 ensuite:"
echo "  sdk install scala 2.12.19"


if [ -d "/home/vagrant/.sdkman" ]; then             # vagrant -> $HOME (root)
    echo "✓ SDKMAN est installé à: $HOME/.sdkman"
    source "vagrant/.sdkman/bin/sdkman-init.sh" # vagrant -> $HOME
    sdk version
else
    echo "✗ SDKMAN n'est pas installé"
    echo "Installation de SDKMAN..."
    # Installer SDKMAN! pour gérer les versions de Scala
    curl -s "https://get.sdkman.io" | bash
    #source "/home/vagrant/.sdkman/bin/sdkman-init.sh" #source "$HOME/.sdkman/bin/sdkman-init.sh" # vagrant -> $HOME
fi

if [ -d "/home/vagrant/.sdkman" ]; then
  source "/home/vagrant/.sdkman/bin/sdkman-init.sh" 
  
fi #source "$HOME/.sdkman/bin/sdkman-init.sh" # vagrant -> $HOME

if command scala -version &> /dev/null; then
    echo "✓ Scala est installé"
    scala -version
    which scala
else
    echo "✗ Scala n'est pas installé"
    echo "Installation de Scala via SDKMAN..."
    # Installer Scala
    #sdk install scala # Installer la dernière version de Scala
    sdk install scala 2.12.19 # Installer une version spécifique de Scala (par exemple, 2.12.19)
fi

if command sbt --version &> /dev/null; then
    echo "✓ sbt est installé"
    sbt --version
    echo "Chemin: $(which sbt)"
else
    echo "✗ sbt n'est pas installé"
    echo "Installation de sbt via SDKMAN..."
    sdk install sbt
fi