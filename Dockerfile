FROM ubuntu:18.04

WORKDIR /root

RUN apt-get update && apt-get install -y libzip-dev clang-6.0 cmake libboost1.65-dev

ADD *.h *.cpp run.sh CMakeLists.txt /root/
ADD db db

RUN mkdir /root/build && cd /root/build && cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=/usr/bin/clang-6.0 -DCMAKE_CXX_COMPILER=/usr/bin/clang++-6.0 ../

EXPOSE 80

CMD ./run.sh