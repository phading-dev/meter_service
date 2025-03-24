#!/bin/bash
# GCP auth
gcloud auth application-default login
gcloud config set project phading-dev

# Create service account
gcloud iam service-accounts create meter-service-builder

# Grant permissions to the service account
gcloud projects add-iam-policy-binding phading-dev --member="serviceAccount:meter-service-builder@phading-dev.iam.gserviceaccount.com" --role='roles/cloudbuild.builds.builder' --condition=None
gcloud projects add-iam-policy-binding phading-dev --member="serviceAccount:meter-service-builder@phading-dev.iam.gserviceaccount.com" --role='roles/container.developer' --condition=None

# Set k8s cluster
gcloud container clusters get-credentials phading-cluster --location=us-central1

# Create the service account
kubectl create serviceaccount meter-service-account --namespace default

# Grant database permissions to the service account
gcloud projects add-iam-policy-binding phading-dev --member=principal://iam.googleapis.com/projects/178489203789/locations/global/workloadIdentityPools/phading-dev.svc.id.goog/subject/ns/default/sa/meter-service-account --role=roles/spanner.databaseUser --condition=None
gcloud projects add-iam-policy-binding phading-dev --member=principal://iam.googleapis.com/projects/178489203789/locations/global/workloadIdentityPools/phading-dev.svc.id.goog/subject/ns/default/sa/meter-service-account --role=roles/bigtable.user --condition=None

# Create Bigtable
cbt -project phading-dev -instance single-instance createtable meter-table
cbt -project phading-dev -instance single-instance createfamily meter-table w:maxversions=1
cbt -project phading-dev -instance single-instance createfamily meter-table a:maxversions=1
cbt -project phading-dev -instance single-instance createfamily meter-table s:maxversions=1
cbt -project phading-dev -instance single-instance createfamily meter-table u:maxversions=1
cbt -project phading-dev -instance single-instance createfamily meter-table t:maxversions=1
cbt -project phading-dev -instance single-instance createfamily meter-table c:maxversions=1
