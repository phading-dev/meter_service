import { ENV_VARS } from "./env_vars";
import {
  K8S_SERVICE_NAME,
  K8S_SERVICE_PORT,
} from "@phading/meter_service_interface/service_const";
import { writeFileSync } from "fs";

export function generate(env: string) {
  let turnupTemplate = `#!/bin/bash
# GCP auth
gcloud auth application-default login
gcloud config set project ${ENV_VARS.projectId}

# Create service account
gcloud iam service-accounts create ${ENV_VARS.builderAccount}

# Grant permissions to the service account
gcloud projects add-iam-policy-binding ${ENV_VARS.projectId} --member="serviceAccount:${ENV_VARS.builderAccount}@${ENV_VARS.projectId}.iam.gserviceaccount.com" --role='roles/cloudbuild.builds.builder' --condition=None
gcloud projects add-iam-policy-binding ${ENV_VARS.projectId} --member="serviceAccount:${ENV_VARS.builderAccount}@${ENV_VARS.projectId}.iam.gserviceaccount.com" --role='roles/container.developer' --condition=None

# Set k8s cluster
gcloud container clusters get-credentials ${ENV_VARS.clusterName} --location=${ENV_VARS.clusterRegion}

# Create the service account
kubectl create serviceaccount ${ENV_VARS.serviceAccount} --namespace default

# Grant database permissions to the service account
gcloud projects add-iam-policy-binding ${ENV_VARS.projectId} --member=principal://iam.googleapis.com/projects/${ENV_VARS.projectNumber}/locations/global/workloadIdentityPools/${ENV_VARS.projectId}.svc.id.goog/subject/ns/default/sa/${ENV_VARS.serviceAccount} --role=roles/spanner.databaseUser --condition=None
gcloud projects add-iam-policy-binding ${ENV_VARS.projectId} --member=principal://iam.googleapis.com/projects/${ENV_VARS.projectNumber}/locations/global/workloadIdentityPools/${ENV_VARS.projectId}.svc.id.goog/subject/ns/default/sa/${ENV_VARS.serviceAccount} --role=roles/bigtable.user --condition=None

# Create Bigtable
cbt -project ${ENV_VARS.projectId} -instance ${ENV_VARS.bigtableInstanceId} createtable ${ENV_VARS.bigtableTableId}
cbt -project ${ENV_VARS.projectId} -instance ${ENV_VARS.bigtableInstanceId} createfamily ${ENV_VARS.bigtableTableId} w:maxversions=1
cbt -project ${ENV_VARS.projectId} -instance ${ENV_VARS.bigtableInstanceId} createfamily ${ENV_VARS.bigtableTableId} a:maxversions=1
cbt -project ${ENV_VARS.projectId} -instance ${ENV_VARS.bigtableInstanceId} createfamily ${ENV_VARS.bigtableTableId} s:maxversions=1
cbt -project ${ENV_VARS.projectId} -instance ${ENV_VARS.bigtableInstanceId} createfamily ${ENV_VARS.bigtableTableId} u:maxversions=1
cbt -project ${ENV_VARS.projectId} -instance ${ENV_VARS.bigtableInstanceId} createfamily ${ENV_VARS.bigtableTableId} t:maxversions=1
cbt -project ${ENV_VARS.projectId} -instance ${ENV_VARS.bigtableInstanceId} createfamily ${ENV_VARS.bigtableTableId} c:maxversions=1
`;
  writeFileSync(`${env}/turnup.sh`, turnupTemplate);

  let cloudbuildTemplate = `steps:
- name: 'node:20.12.1'
  entrypoint: 'npm'
  args: ['ci']
- name: node:20.12.1
  entrypoint: npx
  args: ['bundage', 'bfn', '${env}/main', 'main_bin', '-t', 'bin']
- name: 'gcr.io/cloud-builders/docker'
  args: ['build', '-t', 'gcr.io/${ENV_VARS.projectId}/${ENV_VARS.releaseServiceName}:latest', '-f', '${env}/Dockerfile', '.']
- name: "gcr.io/cloud-builders/docker"
  args: ['push', 'gcr.io/${ENV_VARS.projectId}/${ENV_VARS.releaseServiceName}:latest']
- name: 'gcr.io/cloud-builders/kubectl'
  args: ['apply', '-f', '${env}/service.yaml']
  env:
    - 'CLOUDSDK_CONTAINER_CLUSTER=${ENV_VARS.clusterName}'
    - 'CLOUDSDK_COMPUTE_REGION=${ENV_VARS.clusterRegion}'
- name: 'gcr.io/cloud-builders/kubectl'
  args: ['rollout', 'restart', 'deployment', '${ENV_VARS.releaseServiceName}-deployment']
  env:
    - 'CLOUDSDK_CONTAINER_CLUSTER=${ENV_VARS.clusterName}'
    - 'CLOUDSDK_COMPUTE_REGION=${ENV_VARS.clusterRegion}'
options:
  logging: CLOUD_LOGGING_ONLY
`;
  writeFileSync(`${env}/cloudbuild.yaml`, cloudbuildTemplate);

  let dockerTemplate = `FROM node:20.12.1

WORKDIR /app
COPY package.json .
COPY package-lock.json .
COPY bin/ .
RUN npm ci --omit=dev

EXPOSE ${ENV_VARS.port}
CMD ["node", "main_bin"]
`;
  writeFileSync(`${env}/Dockerfile`, dockerTemplate);

  let serviceTemplate = `apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${ENV_VARS.releaseServiceName}-deployment
spec:
  replicas: ${ENV_VARS.replicas}
  selector:
    matchLabels:
      app: ${ENV_VARS.releaseServiceName}-pod
  template:
    metadata:
      labels:
        app: ${ENV_VARS.releaseServiceName}-pod
    spec:
      serviceAccountName: ${ENV_VARS.serviceAccount}
      containers:
      - name: ${ENV_VARS.releaseServiceName}-container
        image: gcr.io/${ENV_VARS.projectId}/${ENV_VARS.releaseServiceName}:latest
        ports:
        - containerPort: ${ENV_VARS.port}
        livenessProbe:
          httpGet:
            path: /healthz
            port: ${ENV_VARS.port}
          initialDelaySeconds: 10
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /readiness
            port: ${ENV_VARS.port}
          initialDelaySeconds: 10
          periodSeconds: 10
        resources:
          requests:
            cpu: "${ENV_VARS.cpu}"
            memory: "${ENV_VARS.memory}"
          limits:
            cpu: "${ENV_VARS.cpu}"
            memory: "${ENV_VARS.memory}"
---
apiVersion: monitoring.googleapis.com/v1
kind: PodMonitoring
metadata:
  name: ${ENV_VARS.releaseServiceName}-monitoring
spec:
  selector:
    matchLabels:
      app: ${ENV_VARS.releaseServiceName}-pod
  endpoints:
  - port: ${ENV_VARS.port}
    path: /metricsz
    interval: 30s
---
apiVersion: cloud.google.com/v1
kind: BackendConfig
metadata:
  name: ${ENV_VARS.releaseServiceName}-neg-health-check
spec:
  healthCheck:
    port: ${ENV_VARS.port}
    type: HTTP
    requestPath: /healthz
---
apiVersion: v1
kind: Service
metadata:
  name: ${K8S_SERVICE_NAME}
  annotations:
    cloud.google.com/neg: '{"ingress": true}'
    beta.cloud.google.com/backend-config: '{"default": "${ENV_VARS.releaseServiceName}-neg-health-check"}'
spec:
  selector:
    app: ${ENV_VARS.releaseServiceName}-pod
  ports:
    - protocol: TCP
      port: ${K8S_SERVICE_PORT}
      targetPort: ${ENV_VARS.port}
  type: ClusterIP
`;
  writeFileSync(`${env}/service.yaml`, serviceTemplate);

  let mainTemplate = `import "./env";
import "../main";
`
  writeFileSync(`${env}/main.ts`, mainTemplate);
}

import "./dev/env";
generate("dev");

import "./prod/env";
generate("prod");
