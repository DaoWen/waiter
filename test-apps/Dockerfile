FROM twosigma/mesos-agent:1.0.0-1.0.1604

# we need nginx for our http/2 tests
# gettext-base is needed for envsubst in the nginx server
# python is needed to launch kitchen
RUN apt-get update && apt-get install -y gettext-base nginx python3

COPY waiter-init/bin/waiter-mesos-init /usr/bin/waiter-mesos-init
COPY waiter-init/bin/waiter-k8s-init /usr/bin/waiter-k8s-init
COPY nginx/bin/run-nginx-server.sh /opt/nginx/bin/run-nginx-server.sh
COPY nginx/data/nginx-template.conf /opt/nginx/data/nginx-template.conf
COPY kitchen/bin/kitchen /opt/kitchen/kitchen
