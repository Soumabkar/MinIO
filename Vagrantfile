Vagrant.configure("2") do |config|

  # Box ubuntu/jammy64 — Box officielle Ubuntu 22.04 LTS
  config.vm.box = "ubuntu/jammy64" #"debian/bookworm64" Box Debian 12
  config.vm.boot_timeout = 600
  config.vm.hostname = "MinIO"

  # Plugin vbguest — désactiver la mise à jour auto
  config.vbguest.auto_update = false

  # VirtualBox
  config.vm.provider "virtualbox" do |vb|
    vb.name   = "MinIO"
    vb.memory = "8192"
    vb.cpus   = 2

  end

  # Réseau privé
  config.vm.network "private_network", ip: "192.168.56.10"

  # Provisionnement OS

  config.vm.provision "shell", path: "Provisions-Files/OS/provision_prepa_env.sh"
  config.vm.provision "shell", path: "Provisions-Files/OS/provision/python/provision-install-python.sh"
  config.vm.provision "shell", path: "Provisions-Files/OS/provision/java/provision-install-java.sh"
  config.vm.provision "shell", path: "Provisions-Files/OS/provision/scala/provision-install-sdkman-scala-sbt.sh"
  config.vm.provision "shell", path: "Provisions-Files/docker/provision/provision_install_docker.sh"

  config.vm.provision "shell", path: "Provisions-Files/kubernetes/provision_install_arkade.sh"
  config.vm.provision "shell", path: "Provisions-Files/kubernetes/provision_install_minikube_arkade.sh"
  config.vm.provision "shell", path: "Provisions-Files/kubernetes/provision_install_helm_arkade.sh"

  config.vm.provision "shell", path: "Provisions-Files/docker/provision/provision_download_image.sh"

  #Fichiers Docker
  config.vm.provision "file",
    source:      "Provisions-Files/docker/dockerfile/docker-compose-datalake.yml",
    destination: "/home/vagrant/docker/dockerfile/docker-compose-datalake.yml"
  config.vm.provision "file",
    source:      "Provisions-Files/docker/provision/provision_download_image.sh",
    destination: "/home/vagrant/docker/dockerfile/provision_download_image.sh"
  config.vm.provision "file",
    source:      "Provisions-Files/docker/dockerfile/Dockerfile-hive",
    destination: "/home/vagrant/docker/dockerfile/Dockerfile-hive"

  # Fichiers config Trino
  config.vm.provision "file",
    source:      "Provisions-Files/docker/dockerfile/trino-config/config.properties",
    destination: "/home/vagrant/docker/dockerfile/trino-config/config.properties"
  config.vm.provision "file",
    source:      "Provisions-Files/docker/dockerfile/trino-config/jvm.config",
    destination: "/home/vagrant/docker/dockerfile/trino-config/jvm.config"
  config.vm.provision "file",
    source:      "Provisions-Files/docker/dockerfile/trino-config/node.properties",
    destination: "/home/vagrant/docker/dockerfile/trino-config/node.properties"
  config.vm.provision "file",
    source:      "Provisions-Files/docker/dockerfile/trino-config/catalog/hive.properties",
    destination: "/home/vagrant/docker/dockerfile/trino-config/catalog/hive.properties"

  # Fichiers config Hive
  config.vm.provision "file",
    source:      "Provisions-Files/docker/dockerfile/hive-site.xml",
    destination: "/home/vagrant/docker/dockerfile/hive-site.xml"

  # Projet MinIO Python
  config.vm.provision "file",
    source:      "Provisions-Files/project/python/MinIO/config_minio.py",
    destination: "/home/vagrant/project/python/MinIO/config_minio.py"
  config.vm.provision "file",
    source:      "Provisions-Files/project/python/MinIO/requirements.txt",
    destination: "/home/vagrant/project/python/MinIO/requirements.txt"
  config.vm.provision "file",
    source:      "Provisions-Files/project/python/MinIO/config.ini",
    destination: "/home/vagrant/project/python/MinIO/config.ini"
  config.vm.provision "file",
    source:      "Provisions-Files/project/python/MinIO/Datawarehouse/Minio.py",
    destination: "/home/vagrant/project/python/MinIO/Datawarehouse/Minio.py"
  config.vm.provision "file",
    source:      "Provisions-Files/project/python/MinIO/Datawarehouse/__init__.py",
    destination: "/home/vagrant/project/python/MinIO/Datawarehouse/__init__.py"
  config.vm.provision "file",
    source:      "Provisions-Files/project/python/MinIO/Entity/Data.py",
    destination: "/home/vagrant/project/python/MinIO/Entity/Data.py" 
  config.vm.provision "file",
    source:      "Provisions-Files/project/python/MinIO/Entity/__init__.py",
    destination: "/home/vagrant/project/python/MinIO/Entity/__init__.py"
  config.vm.provision "file",
    source:      "Provisions-Files/project/python/MinIO/Spark/SparkMinIo.py",
    destination: "/home/vagrant/project/python/MinIO/Spark/SparkMinIo.py"
  config.vm.provision "file",
    source:      "Provisions-Files/project/python/MinIO/Spark/__init__.py",
    destination: "/home/vagrant/project/python/MinIO/Spark/__init__.py"
  config.vm.provision "file",
    source:      "Provisions-Files/project/python/MinIO/SqlEngine/Trino.py",
    destination: "/home/vagrant/project/python/MinIO/SqlEngine/Trino.py"
  config.vm.provision "file",
    source:      "Provisions-Files/project/python/MinIO/SqlEngine/__init__.py",
    destination: "/home/vagrant/project/python/MinIO/SqlEngine/__init__.py"
  config.vm.provision "file",
    source:      "Provisions-Files/project/python/MinIO/main/main.py",
    destination: "/home/vagrant/project/python/MinIO/main/main.py"
  config.vm.provision "file",
    source:      "Provisions-Files/project/python/MinIO/main/__init__.py",
    destination: "/home/vagrant/project/python/MinIO/main/__init__.py"

end