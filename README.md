# mc885
sudo apt-get install build-essential curl git python-setuptools ruby
ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Linuxbrew/install/master/install)"
PATH="$HOME/.linuxbrew/bin:$PATH"
echo 'export PATH="$HOME/.linuxbrew/bin:$PATH"' >>~/.bash_profile
brew install apache-spark
brew install jdk
brew install hadoop
