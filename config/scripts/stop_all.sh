#!/usr/bin/env bash

gcloud compute instances stop $(gcloud compute instances list --uri)
