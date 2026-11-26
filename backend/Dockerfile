# ---- Stage 1: build the jar -------------------------------------------------------------
FROM eclipse-temurin:21-jdk-alpine AS builder
WORKDIR /build
COPY .mvn/ .mvn/
COPY mvnw pom.xml ./
RUN chmod +x mvnw && ./mvnw dependency:go-offline -B
COPY src/ src/
RUN ./mvnw clean package -DskipTests -B && cp target/backend-*.jar /build/app.jar

# ---- Stage 2: slim runtime (fits Render/Koyeb 512 MB free tier) -------------------------
FROM eclipse-temurin:21-jre-alpine
RUN addgroup -S aerosync && adduser -S aerosync -G aerosync
WORKDIR /app
COPY --from=builder --chown=aerosync:aerosync /build/app.jar /app/app.jar
USER aerosync
EXPOSE 8080
ENTRYPOINT ["sh", "-c", "java -XX:+UseContainerSupport -XX:MaxRAMPercentage=70.0 -XX:+UseSerialGC -Djava.security.egd=file:/dev/./urandom -jar /app/app.jar"]
