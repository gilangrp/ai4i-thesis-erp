apt update
apt install -y curl

curl -fsSL https://deb.nodesource.com/setup_24.x | bash -

apt install -y nodejs

node --version
npm --version
which node