#!/bin/bash

folder_path="/home/ubuntu/Projects/rl_index_selection/experiment_results"

# 检查目录是否存在
if [ -d "$folder_path" ]; then
  # 清空目录中的内容
  rm -rf "$folder_path"/*
  echo "成功清空 $folder_path 文件夹中的内容。"
else
  echo "目录 $folder_path 不存在。"
fi