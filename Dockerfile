# ===== BUILD STAGE =====
FROM maven:3.9.9-eclipse-temurin-21 AS build

WORKDIR /app

# Copy dependency nội bộ
COPY docker-libs/plugin-api/1.2.1/plugin-api-1.2.1.jar /tmp/plugin-api-1.2.1.jar
COPY docker-libs/plugin-api/1.2.1/plugin-api-1.2.1.pom /tmp/plugin-api-1.2.1.pom

RUN mvn install:install-file \
    -Dfile=/tmp/plugin-api-1.2.1.jar \
    -DpomFile=/tmp/plugin-api-1.2.1.pom

# Copy pom trước để cache dependency
COPY pom.xml .

# Download dependency trước
RUN mvn dependency:go-offline

# Copy source
COPY src ./src

# Build project
RUN mvn clean package -DskipTests

# ===== RUN STAGE =====
FROM eclipse-temurin:21-jre

WORKDIR /app

# Copy file jar từ stage build
COPY --from=build /app/target/jobcrawlgiaydangkiemxecogioi-0.0.1-SNAPSHOT.jar app.jar

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "app.jar"]