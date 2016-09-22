# MC885

Projetos de MC855 da Unicamp

## Configuração das Máquinas:
```
sudo apt-get install language-pack-UTF-8
sudo apt-get install build-essential curl git python-setuptools ruby
ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Linuxbrew/install/master/install)"
PATH="$HOME/.linuxbrew/bin:$PATH"
echo 'export PATH="$HOME/.linuxbrew/bin:$PATH"' >>~/.bash_profile
brew install jdk
brew install scala
brew install hadoop
brew install apache-spark
export JAVA_HOME=/home/debian/.linuxbrew/Cellar/jdk/1.8.0-102/
```

##Máquinas:
* **Prod:** debian mc855Prod
* **Test:** debian mc855Test

## Referências

[TutorialsPoint - Spark Quick Guide](https://www.tutorialspoint.com/apache_spark/apache_spark_quick_guide.htm)
