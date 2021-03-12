ssh -n $1 "nohup java -jar $2 $3 100 FIFO $4" >> server_out/$4.out
