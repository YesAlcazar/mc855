# MC885

Projetos de MC855 da Unicamp

## Configuração das Máquinas:
```
sudo apt-get install build-essential curl git python-setuptools ruby
ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Linuxbrew/install/master/install)"
PATH="$HOME/.linuxbrew/bin:$PATH"
echo 'export PATH="$HOME/.linuxbrew/bin:$PATH"' >>~/.bash_profile
brew install apache-spark
brew install jdk
brew install hadoop
```

##Máquinas:
* **Prod:** debian mc855Prod
* **Test:** debian mc855Test
