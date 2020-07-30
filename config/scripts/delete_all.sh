#!/usr/bin/env bash

gcloud compute instances delete $(gcloud compute instances list --uri) 
