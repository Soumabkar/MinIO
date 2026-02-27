Vagrant.configure("2") do |config|
  config.vm.box = "ubuntu/jammy64"  
  config.vm.boot_timeout = 600  # Temps d'attente de 600 secondes (10 minutes)
  config.vbguest.auto_update = false
  config.vm.hostname = "MinIO-VM"
  config.vm.provider "virtualbox" do |vb|
    vb.memory = "8192"  # Allouer 8 Go de RAM 
  end
  config.vm.network "private_network", ip: "192.168.56.10"  # IP privée pour la VM

  # # Provisionnement de la VM
   config.vm.provision "shell", path: "Provisions-Files/OS/provision_prepa_env.sh"
  # config.vm.provision "shell", path: "Provisions-Files/OS/provision/python/provision-install-python.sh"
  # config.vm.provision "shell", path: "Provisions-Files/OS/provision/scala/provision-install-scala-sdkman-v1.sh"
  # config.vm.provision "shell", path: "Provisions-Files/docker/provision/provision_install_docker.sh"
  # config.vm.provision "shell", path: "Provisions-Files/kubernetes/provision_install_arkade.sh"
  # config.vm.provision "shell", path: "Provisions-Files/kubernetes/provision_install_minikube_arkade.sh"
  # config.vm.provision "shell", path: "Provisions-Files/kubernetes/provision_install_helm_arkade.sh"

  # # Provisionnement du data lake MinIO
  #   config.vm.provision "file", source: "Provisions-Files/docker/dockerfile/docker-compose-datalake.yml", destination: "/home/vagrant/docker/dockerfile/docker-compose-datalake.yml"

  # Provisionnement du projet MinIO Python 
  config.vm.provision "file", source: "Provisions-Files/project/python/MinIO/config_minio.py", destination: "/home/vagrant/project/python/MinIO/config_minio.py"
  config.vm.provision "file", source: "Provisions-Files/project/python/MinIO/requirements.txt", destination: "/home/vagrant/project/python/MinIO/requirements.txt"
  config.vm.provision "file", source: "Provisions-Files/project/python/MinIO/config.ini", destination: "/home/vagrant/project/python/MinIO/config.ini"
  config.vm.provision "file", source: "Provisions-Files/project/python/MinIO/data/table_dev1.csv", destination: "/home/vagrant/project/python/MinIO/data/table_dev1.csv"
  config.vm.provision "file", source: "Provisions-Files/project/python/MinIO/data/table.csv", destination: "/home/vagrant/project/python/MinIO/data/table.csv"





end