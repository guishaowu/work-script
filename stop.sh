#!/bin/sh
ps -ef|grep send_notification_ipm|awk '{print $2}'|xargs kill -9
