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
git clone https://github.com/YesAlcazar/mc855.git
cd mc855
eval "$(ssh-agent -s)"
chmod 400 cloud.key
ssh-add cloud.key
git remote set-url origin git@github.com:YesAlcazar/mc855.git
git pull
cd ..

```

##Máquinas:
* **Prod(143.106.73.44):** debian/mc855Prod
* **Test(143.106.73.43):** debian/mc855Test

## Referências:

[TutorialsPoint - Spark Quick Guide](https://www.tutorialspoint.com/apache_spark/apache_spark_quick_guide.htm)
[LinkedIn - Configure Spark on a YARN Cluster](https://www.linkedin.com/pulse/how-configure-spark-cluster-yarn-artem-pichugin)
[Apache Hadoop - Cluster Setup](http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/ClusterSetup.html)
[Spark standalone cluster Tutorial](http://mbonaci.github.io/mbo-spark/)

[Spinning up an Apache Spark Cluster: Step-by-Step](http://blog.insightdatalabs.com/spark-cluster-step-by-step/)

[Um comparativo entre MapReduce e Spark para analise de Big Data](https://www.infoq.com/br/articles/mapreduce-vs-spark)

[Five things you need to know about Hadoop v. Apache Spark](http://www.infoworld.com/article/3014440/big-data/five-things-you-need-to-know-about-hadoop-v-apache-spark.html)

##Problemas:

[StackOverflow - Locale Warning Perl](http://stackoverflow.com/questions/2499794/how-can-i-fix-a-locale-warning-from-perl)
