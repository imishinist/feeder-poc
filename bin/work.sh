#!/bin/bash

collection=$1
member_id=$2
enqueue_ts=$3

on_exit() {
  echo "on exit"
}

trap on_exit EXIT


echo "work.sh: $collection $member_id $enqueue_ts"
sleep 1