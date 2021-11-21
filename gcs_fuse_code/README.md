Assuming you have curl downloaded, if not run:

sudo apt-get install curl

Run the following in order to setup the project:
1. Run "sh gcloud_cli_setup_script.sh" //Don't add the quotes. It downloads google cloud cli tools   
2. Now, login into your gcp account on browser pop up.
3. Run "sh gcsfuse_install_script.sh" //Don't add the quotes. It downloads gcsfuse
4. Add a google service account key JSON file in the credentials directory[It will be passed to gcsfuse while running gcsfuse commands].
5. Go to gcp on your browser and create a bucket say "legostore-test".
5. Create a directory called "gcs_mount", for example, and run the command "
gcsfuse --key-file <absolute path of the JSON file in ./credentials folder> legostore-test ./gcs_mount/"
