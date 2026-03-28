#!/bin/bash

sudo wget https://go.dev/dl/go1.26.1.linux-amd64.tar.gz 
sudo tar -C /usr/local -xzf go1.26.1.linux-amd64.tar.gz 
sudo echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc  
source ~/.bashrc 
# go env GOROOT ( # export GOROOT=/usr/local/go # export PATH=$GOROOT/bin:$PATH ajouter dans ~/.bashrc manuellement si besoin )
# go env GOPATH ( # export GOPATH=$HOME/go # export PATH=$GOPATH/bin:$PATH ajouter dans ~/.bashrc manuellement si besoin )
# source ~/.bashrc  # ou source ~/.zshrc selon le shell utilisé

# # Définir GOPATH
# sudo mkdir -p $HOME/go/{bin,pkg,src} 
# sudo echo 'export GOPATH=$HOME/go' >> ~/.bashrc 
# source ~/.bashrc

# # Ajouter GOPATH/bin au PATH

# sudo echo $PATH | grep "$(go env GOPATH)/bin"
# sudo echo 'export PATH=$GOPATH/bin:$PATH' >> ~/.bashrc
# source ~/.bashrc
# go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest