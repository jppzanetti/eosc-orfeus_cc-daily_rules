# Dockerizing BASE 4 Clients
# Based on ubuntu:18.04
#
# Description:
# BASE 4 CLIENTS: MONGO, IRODS & OBSPY used for WFcatalog Rule execution procedure 
#
# imageName:'client-collector:1.1'
# Use:
# build command: sudo docker build -t "client-collector:1.1" .
# run command: sudo docker run -p 27017:27017 -p 1247:1247 --name total_client -v /opt/eudat/myrules:/usr/src/collector -v /mnt/seedstore_nfs/archive:/var/lib/datairods -it client-collector:1.1


FROM       ubuntu:18.04
MAINTAINER massimo.fares@ingv.it


#install dependencies
RUN apt-get update
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y apt-transport-https wget git vim python-dev python-pycurl python-simplejson \
    libcurl4-gnutls-dev libssl-dev pkg-config gcc-4.8 iputils-ping \
    python python-psutil python-requests python-jsonschema libfuse2 \
    python-setuptools python-dev build-essential python-pip \
    libxml2-dev libxslt1-dev libz-dev libfreetype6-dev \
    python-numpy python-scipy python-matplotlib 

RUN apt-get update

# mongo driver and irods client
RUN pip install pymongo python-irodsclient

# irods icommand
RUN mkdir -p /opt/icomm
WORKDIR /opt/icomm
RUN wget https://files.renci.org/pub/irods/releases/4.1.12/ubuntu14/irods-icommands-4.1.12-ubuntu14-x86_64.deb

RUN dpkg -i irods-icommands-4.1.12-ubuntu14-x86_64.deb
RUN apt-get install -y


#make app dir
RUN mkdir -p /usr/src/collector

# obspy
WORKDIR /
RUN git clone https://github.com/obspy/obspy.git
WORKDIR /obspy

RUN pip install -e .
RUN pip install pyproj

RUN mkdir -p /var/lib/datairods

WORKDIR /var/lib/irods/myrule

# Expose port #27017 from the container to the host (just in case - not necessary)
EXPOSE 27017 1247



