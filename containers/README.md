### Create an AMI for running containers

Launch the Ubuntu AMI from the console and ssh into it.
Put the instructions here for how to do it.

Then run:


``sudo apt update``
``sudo apt -y upgrade``

``sudo apt install -y autoconf automake make cryptsetup fuse fuse2fs git libfuse-dev libglib2.0-dev libseccomp-dev libtool pkg-config runc squashfs-tools squashfs-tools-ng uidmap wget zlib1g-dev libnvidia-container-tools``

``curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey | sudo gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg``
``curl -s -L https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list | sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' | sudo tee /etc/apt/sources.list.d/nvidia-container-toolkit.list``
``sudo apt-get update``
``sudo apt-get install -y nvidia-container-toolkit``

``export VERSION=1.21.6 OS=linux ARCH=amd64 && wget https://dl.google.com/go/go$VERSION.$OS-$ARCH.tar.gz && sudo tar -C /usr/local -xzvf go$VERSION.$OS-$ARCH.tar.gz && rm go$VERSION.$OS-$ARCH.tar.gz``


``echo 'export GOPATH=${HOME}/go' >> ~/.bashrc && echo 'export PATH=/usr/local/go/bin:${PATH}:${GOPATH}/bin' >> ~/.bashrc && source ~/.bashrc``

``export VERSION=4.1.0 && wget https://github.com/sylabs/singularity/releases/download/v${VERSION}/singularity-ce-${VERSION}.tar.gz && tar -xzf singularity-ce-${VERSION}.tar.gz && cd singularity-ce-${VERSION}``

``git clone --recurse-submodules https://github.com/sylabs/singularity.git``

``cd singularity``

``git checkout --recurse-submodules v4.1.0``

``./mconfig && make -C ./builddir && sudo make -C ./builddir install``
