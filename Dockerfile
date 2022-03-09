FROM openjdk:16-jdk
RUN mkdir /app
COPY ./build/install/crawler /app/
WORKDIR /app/bin
ENTRYPOINT ["./crawler"]