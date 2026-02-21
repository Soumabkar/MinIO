Vagrant.configure("2") do |config|
  config.vm.box = "ubuntu/jammy64"  
  config.vm.boot_timeout = 600  # Temps d'attente de 600 secondes (10 minutes)
  config.vbguest.auto_update = false
  config.vm.hostname = "MinIO-VM"
  config.vm.provider "virtualbox" do |vb|
    vb.memory = "4096"  # Allouer 4 Go de RAM
  end
  config.vm.network "private_network", ip: "192.168.56.10"  # IP privée pour la VM
  #config.vm.provision "shell", path: "Provisions-Files/provision_install_docker.sh"

  #config.vm.provision "file", source: "Provisions-Files/docker_minio_command.txt", destination: "/home/vagrant/docker_minio_command.txt"
  #config.vm.provision "shell", path: "Provisions-Files/provision_prepa_env.sh"
  #config.vm.provision "file", source: "config_minio.py", destination: "/home/vagrant/config_minio.py"
  #config.vm.provision "file", source: "config.ini", destination: "/home/vagrant/config.ini"
  config.vm.provision "file", source: "table.csv", destination: "/home/vagrant/table.csv"

end