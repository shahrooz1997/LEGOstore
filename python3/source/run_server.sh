screen -X -S server kill
screen -dmS server
screen -S server -X stuff "python3 data_server.py\n"
screen -ls
