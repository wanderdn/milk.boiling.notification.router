FROM eclipse-temurin:24-jre
RUN mkdir -p /app/data && chown 10001:10001 /app/data
WORKDIR /app/data
COPY build/libs/app.jar /app/app.jar
ENV JAVA_TOOL_OPTIONS="-XX:ActiveProcessorCount=1 -Xms512m -Xmx3g -XX:+UseSerialGC"
USER 10001:10001
EXPOSE 8080 7000
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
