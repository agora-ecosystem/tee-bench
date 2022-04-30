#!/bin/bash

TMP=/tmp
CUR_DIR=$(pwd)

echo "1. Install dependencies"
sudo apt update
sudo apt install make gcc g++ libssl-dev python3 python3-pip git git-lfs wget build-essential build-essential ocaml automake autoconf libtool python libssl-dev linux-headers-$(uname -r) cmake -y
pip3 install matplotlib numpy pyyaml pandas

echo "2. Install custom SGX driver"
git clone -b ewb-monitoring https://github.com/agora-ecosystem/linux-sgx-driver.git $TMP/linux-sgx-driver
cd $TMP/linux-sgx-driver
#unistall old version
sudo /sbin/modprobe -r isgx
sudo rm -rf "/lib/modules/"`uname -r`"/kernel/drivers/intel/sgx"
sudo /sbin/depmod
sudo /bin/sed -i '/^isgx$/d' /etc/modules

#install new version
make clean
make
sudo mkdir -p "/lib/modules/"`uname -r`"/kernel/drivers/intel/sgx"    
sudo cp isgx.ko "/lib/modules/"`uname -r`"/kernel/drivers/intel/sgx"    
sudo sh -c "cat /etc/modules | grep -Fxq isgx || echo isgx >> /etc/modules"    
sudo /sbin/depmod
sudo /sbin/modprobe isgx
cd $TMP
rm -rf $TMP/linux-sgx-driver

echo "3. Install SGX-SDK"
echo "deb [arch=amd64] https://download.01.org/intel-sgx/sgx_repo/ubuntu $(lsb_release -sc) main" | sudo tee /etc/apt/sources.list.d/intel-sgx.list
wget -qO - https://download.01.org/intel-sgx/sgx_repo/ubuntu/intel-sgx-deb.key | sudo apt-key add -
sudo apt-get update
sudo apt-get install libsgx-epid libsgx-quote-ex libsgx-dcap-ql -y
wget https://download.01.org/intel-sgx/latest/linux-latest/distro/ubuntu18.04-server/sgx_linux_x64_sdk_2.16.100.4.bin -P $TMP
chmod +x $TMP/sgx_linux_x64_sdk_2.16.100.4.bin
printf 'no\n\/opt\/intel\n' | sudo $TMP/sgx_linux_x64_sdk_2.16.100.4.bin
echo "source /opt/intel/sgxsdk/environment" >> $HOME/.bashrc

source /opt/intel/sgxsdk/environment
rm $TMP/sgx_linux_x64_sdk_2.16.100.4.bin

echo "4. Install Microsoft SEAL"
cd $TMP
git clone -b 3.6.6 https://github.com/microsoft/SEAL.git $TMP/SEAL
cd $TMP/seal
cmake -S . -B build
cmake --build build
sudo cmake --install build
rm -rf $TMP/SEAL
cd $CUR_DIR
