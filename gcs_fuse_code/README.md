## What does GCSFUSE do?

It mounts a GCS bucket in a directory as a user space file-system.

## How did we use it?

We used it to make rocksdb use the GCSFUSE directory to make data durable i.e. in case of VM failure data can be obtained from GCS. But, this idea failed because rocksdb relies on POSIX style interface and [GCSFUSE is not POSIX compatible](https://cloud.google.com/storage/docs/gcs-fuse#notes). Specifically, it failed when rocksdb used [access()](https://man7.org/linux/man-pages/man2/access.2.html) on a file path which is in GCSFUSE directory in our code base.

## How to get GCSFUSE to run code over here?

0. Assuming you have downloaded curl, if not run:
    ```
    sudo apt-get install curl
    ```
1. Run (It downloads google cloud cli tools):
    ```
    sh gcloud_cli_setup_script.sh
    ```
2. Now, login into your gcp account on browser pop up.
3. Run (It downloads gcsfuse):
    ```
    sh gcsfuse_install_script.sh
    ``` 
4. Add a google service account key JSON file in the credentials directory (It will be passed to gcsfuse while running gcsfuse commands).
5. Go to gcp on your browser and create a bucket say "**legostore-test**".
6. Create a directory called "gcs_mount", for example, and run the command:
    ```
    gcsfuse --key-file <absolute path of the JSON file in ./credentials folder> legostore-test ./gcs_mount/"
    ```