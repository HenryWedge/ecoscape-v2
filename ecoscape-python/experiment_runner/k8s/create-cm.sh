kubectl create cm sut-config --from-file=../../build/sut/base
kubectl create cm infra-config --from-file=../../build/infra/base
kubectl create cm chaos-config --from-file=../../build/infra/chaos
kubectl create cm monitor-config --from-file=../../build/monitor
kubectl create cm load-config --from-file=../../build/load/base
kubectl create cm experiment-config --from-file=../experiment_config