####################################################################################
# Tracker
# Build with: sudo docker build -t tracker .
####################################################################################

FROM debian:stable-slim
WORKDIR /root/tracker
ADD . /root/tracker
EXPOSE 443
EXPOSE 80

####################################################################################

# ulimit incrase (set in docker templats/aws ecs-task-definition too!!)
RUN bash -c 'echo "root hard nofile 16384" >> /etc/security/limits.conf' \
 && bash -c 'echo "root soft nofile 16384" >> /etc/security/limits.conf' \
 && bash -c 'echo "* hard nofile 16384" >> /etc/security/limits.conf' \
 && bash -c 'echo "* soft nofile 16384" >> /etc/security/limits.conf'

# ip/tcp tweaks, disable ipv6
RUN bash -c 'echo "net.core.somaxconn = 8192" >> /etc/sysctl.conf' \
 && bash -c 'echo "net.ipv4.tcp_max_tw_buckets = 1440000" >> /etc/sysctl.conf' \
 && bash -c 'echo "net.ipv6.conf.all.disable_ipv6 = 1" >> /etc/sysctl.conf' \ 
 && bash -c 'echo "net.ipv4.ip_local_port_range = 5000 65000" >> /etc/sysctl.conf' \
 && bash -c 'echo "net.ipv4.tcp_fin_timeout = 15" >> /etc/sysctl.conf' \
 && bash -c 'echo "net.ipv4.tcp_window_scaling = 1" >> /etc/sysctl.conf' \
 && bash -c 'echo "net.ipv4.tcp_syncookies = 1" >> /etc/sysctl.conf' \
 && bash -c 'echo "net.ipv4.tcp_max_syn_backlog = 8192" >> /etc/sysctl.conf' \
 && bash -c 'echo "fs.file-max=65536" >> /etc/sysctl.conf'

####################################################################################

# update packages and install required ones
RUN apt update && apt upgrade -y && apt install -y \
  golang-1.8-go \
  supervisor \
  git \
  libssl-dev \
  python-pip \
  jq \
  sudo

# apt cleanup
RUN apt autoclean -y && apt autoremove -y

# install latest AWS CLI
RUN pip install awscli --upgrade

# build app in production mode
RUN /usr/lib/go-1.8/bin/go get github.com/dioptre/tracker
RUN /usr/lib/go-1.8/bin/go install github.com/dioptre/tracker
RUN /usr/lib/go-1.8/bin/go build

####################################################################################

# copy files to other locations
COPY supervisord.conf /etc/supervisor/supervisord.conf
COPY tracker.supervisor.conf /etc/supervisor/conf.d/tracker.supervisor.conf

# make startup script executable
RUN chmod +x dockercmd.sh

####################################################################################

# startup command
CMD ./dockercmd.sh
