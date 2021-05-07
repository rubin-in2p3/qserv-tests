# Run scaletests against Qserv in k8s

# Pre-requisites

- `Docker` > 20.16.6
- `kubectl` > 1.18
- Access to a Kubernetes cluster

# Launch

Run :

```
git clone https://github.com/in2p3-dp0/qserv-tests.git
cd qserv-tests
./build-image.sh
kubectl apply -f scaletests.yaml
POD=$(kubectl get pods -l app=scaletests -o jsonpath='{.items[0].metadata.name}')
kubectl exec -it $POD -- bash
/scaletests/bin/run.sh
# It is possible to fine tune the queries execution, for example:
/scaletests/python/test_cosmoDC2_parallel.py -H qserv-qserv -v -j 100 -p 4040 -f /scaletests/python/cosmodc2_DM_halos.pkl 
```

It is possible to edit in real time the code (in `/scaletests/python`) by installing https://www.telepresence.io/ and then run

```
./dev_local.sh
```

The code launched in this shell will be executed inside the k8s cluster, but you can edit it in real-time locally in `rootfs/scaletests/python`

However this will work only for one person at a time, unless you rename the `scaletests` deployment.