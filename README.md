# MC855

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
export LC_CTYPE=en_US.UTF-8
export LC_ALL=en_US.UTF-8
```

#Configuração de Ambiente:
```
mkdir downloads
cd downloads
wget https://archive.apache.org/dist/hadoop/common/hadoop-2.7.2/hadoop-2.7.2.tar.gz
wget http://d3kbcqa49mib13.cloudfront.net/spark-2.0.0-bin-hadoop2.7.tgz
cd ..
tar xfz downloads/hadoop-2.7.2.tar.gz
tar xfz downloads/spark-2.0.0-bin-hadoop2.7.tgz
```

##Máquinas:
* **Prod(143.106.73.44):** debian/mc855Prod
* **Test(143.106.73.43):** debian/mc855Test

## Referências:

[TutorialsPoint - Spark Quick Guide](https://www.tutorialspoint.com/apache_spark/apache_spark_quick_guide.htm)

##Problemas:

[StackOverflow - Locale Warning Perl](http://stackoverflow.com/questions/2499794/how-can-i-fix-a-locale-warning-from-perl)
