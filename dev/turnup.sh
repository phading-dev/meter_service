#!/bin/bash
# GCP auth
gcloud auth application-default login
gcloud config set project phading-dev

# Create service account
gcloud iam service-accounts create product-meter-service-builder

# Grant permissions to the service account
gcloud projects add-iam-policy-binding phading-dev --member="serviceAccount:product-meter-service-builder@phading-dev.iam.gserviceaccount.com" --role='roles/cloudbuild.builds.builder' --condition=None
gcloud projects add-iam-policy-binding phading-dev --member="serviceAccount:product-meter-service-builder@phading-dev.iam.gserviceaccount.com" --role='roles/container.developer' --condition=None

# Set k8s cluster
gcloud container clusters get-credentials phading-cluster --location=us-central1

# Create the service account
kubectl create serviceaccount product-meter-service-account --namespace default

# Grant database permissions to the service account
gcloud projects add-iam-policy-binding phading-dev --member=principal://iam.googleapis.com/projects/178489203789/locations/global/workloadIdentityPools/phading-dev.svc.id.goog/subject/ns/default/sa/product-meter-service-account --role=roles/spanner.databaseUser --condition=None
gcloud projects add-iam-policy-binding phading-dev --member=principal://iam.googleapis.com/projects/178489203789/locations/global/workloadIdentityPools/phading-dev.svc.id.goog/subject/ns/default/sa/product-meter-service-account --role=roles/bigtable.user --condition=None

# Create Bigtable database
cbt -project phading-dev createinstance product-meter-instance "product-meter-instance" product-meter-db-cluster us-central1-a 1 SSD
cbt -project phading-dev -instance product-meter-instance createtable product-meter-db
cbt -project phading-dev -instance product-meter-instance createfamily product-meter-db w:maxversions=1
cbt -project phading-dev -instance product-meter-instance createfamily product-meter-db a:maxversions=1
cbt -project phading-dev -instance product-meter-instance createfamily product-meter-db s:maxversions=1
cbt -project phading-dev -instance product-meter-instance createfamily product-meter-db u:maxversions=1
cbt -project phading-dev -instance product-meter-instance createfamily product-meter-db t:maxversions=1
cbt -project phading-dev -instance product-meter-instance createfamily product-meter-db c:maxversions=1
