#!/bin/sh
mount -t cifs -o rw,vers=3.0,credentials=/root/.smbcredentials2 //192.168.1.2/Downloader /mnt/server/downloader