FROM faddat/archlinux

RUN pacman -Syyu --noconfirm go git rocksdb leveldb
