FROM openjdk:16-jdk
RUN mkdir /app
COPY ./crawler /app
RUN cd /app && chmod +x gradlew && ./gradlew build
WORKDIR /app
CMD ["chmod", "+x", "gradlew"]
CMD ["./gradlew", "run"]
