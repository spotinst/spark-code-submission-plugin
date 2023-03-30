plugins {
    id("java-library")
    id("maven-publish")
}

group = "com.netapp.spark"
version = "1.0.0"

repositories {
    mavenCentral()
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(11))
    }
}

tasks.withType<JavaCompile>().configureEach { options.compilerArgs.add("--enable-preview") }

dependencies {
    implementation("org.apache.spark:spark-core_2.12:3.3.2")
    implementation("org.apache.spark:spark-sql_2.12:3.3.2")
    implementation("io.undertow:undertow-core:2.3.5.Final")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.14.2")

    testImplementation(platform("org.junit:junit-bom:5.9.2"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.8.2")
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            groupId = "com.netapp.spark"
            artifactId = "codesubmit"
            version = "1.0.0"

            from(components["java"])
        }
        /*create<IvyPublication>("ivy") {
            organisation = "com.netapp.spark"
            module = "codesubmit"
            revision = "1.0"

            from(components["java"])
        }*/
    }
    repositories {
        maven {
            url = uri(layout.projectDirectory.dir("repo"))
            //val homeDir = System.getProperty("user.home")
            //url = uri("$homeDir/.ivy2/cache")
        }
    }
}

/*tasks.compileJava {
    options.release.set(11)
}*/

tasks.test {
    jvmArgs = listOf(
        "--add-opens=java.base/java.util.regex=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.time=ALL-UNNAMED",
        "--add-opens=java.base/java.util.stream=ALL-UNNAMED",
        "--add-opens=java.base/java.nio.charset=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
    )
    useJUnitPlatform()
}