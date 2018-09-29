screen -X -S client kill
screen -dmS client
screen -S client -X stuff "python3 Client.py\n"
screen -ls
