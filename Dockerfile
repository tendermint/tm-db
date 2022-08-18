# This file makes a development and test environment that includes the latest versions of relevant databases.

FROM archlinux

RUN pacman -Syyu --noconfirm go base-devel rocksdb leveldb